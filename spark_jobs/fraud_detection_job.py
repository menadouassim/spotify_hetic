"""
Spark Job #18 — fraud_detection_job (stateful)
==============================================
Détecte les comportements frauduleux par fenêtre de 1 min et par utilisateur :
  - bot_stream    : beaucoup d'écoutes très courtes (< 5 s)
  - burst_listen  : trop d'écoutes dans la fenêtre (activité non humaine)
Écrit les alertes dans la table `fraud_detections`.

L'état est borné par un watermark (agrégation fenêtrée stateful).

Lancement :
    /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
        /opt/spark-jobs/fraud_detection_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
TOPIC = "listening_events"
POSTGRES_URL = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")
PG = {"user": "spotify", "password": "spotify", "driver": "org.postgresql.Driver"}

SHORT_PLAY_MS = 5000
BOT_SHORT_THRESHOLD = 5     # >= 5 écoutes courtes / min → bot
BURST_THRESHOLD = 30        # >= 30 écoutes / min → burst

SCHEMA = StructType([
    StructField("event_id",     StringType()),
    StructField("user_id",      StringType()),
    StructField("track_id",     StringType()),
    StructField("timestamp",    StringType()),
    StructField("duration_ms",  IntegerType()),
    StructField("device_type",  StringType()),
    StructField("geo_country",  StringType()),
    StructField("completed",    BooleanType()),
    StructField("event_source", StringType()),
])

spark = SparkSession.builder.appName("fraud_detection_job").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    raw.selectExpr("CAST(value AS STRING) AS json")
    .select(F.from_json("json", SCHEMA).alias("e"))
    .select("e.*")
    .withColumn("event_time", F.to_timestamp("timestamp"))
)

windowed = (
    events
    .withWatermark("event_time", "2 minutes")
    .groupBy(F.window("event_time", "1 minute"), "user_id")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("duration_ms") < SHORT_PLAY_MS, 1).otherwise(0)).alias("short_plays"),
    )
)


def write_fraud(batch_df, batch_id):
    susp = (
        batch_df
        .filter((F.col("short_plays") >= BOT_SHORT_THRESHOLD) | (F.col("total") >= BURST_THRESHOLD))
        .select(
            F.col("user_id"),
            F.when(F.col("short_plays") >= BOT_SHORT_THRESHOLD, F.lit("bot_stream"))
             .otherwise(F.lit("burst_listen")).alias("fraud_type"),
            F.round(F.col("short_plays") / F.col("total"), 4).alias("suspicion_score"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
        )
    )
    if susp.rdd.isEmpty():
        return
    (susp.write.format("jdbc")
        .option("url", POSTGRES_URL).option("dbtable", "stg_fraud_detections")
        .option("user", PG["user"]).option("password", PG["password"]).option("driver", PG["driver"])
        .mode("overwrite").save())
    conn = batch_df.sparkSession._sc._jvm.java.sql.DriverManager.getConnection(
        POSTGRES_URL, PG["user"], PG["password"])
    try:
        st = conn.createStatement()
        st.execute("""
            INSERT INTO fraud_detections
                (user_id, fraud_type, suspicion_score, window_start, window_end)
            SELECT user_id::uuid, fraud_type, suspicion_score, window_start, window_end
            FROM stg_fraud_detections
        """)
        st.close()
    finally:
        conn.close()
    print(f"[batch {batch_id}] {susp.count()} alertes de fraude écrites")


writer = (
    windowed.writeStream
    .outputMode("update")
    .foreachBatch(write_fraud)
    .option("checkpointLocation", os.getenv("CHECKPOINT_DIR", "/tmp/chk/fraud"))
)
if os.getenv("SPARK_CONTINUOUS") != "1":
    writer = writer.trigger(availableNow=True)

writer.start().awaitTermination()
