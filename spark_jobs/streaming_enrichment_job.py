"""
Spark Job #17 — streaming_enrichment_job
=========================================
Enrichit les `listening_events` (Kafka) avec le catalogue PostgreSQL
(jointure stream-static) puis republie dans le topic `enriched_events`.

Lancement :
    /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
        /opt/spark-jobs/streaming_enrichment_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
SRC_TOPIC = "listening_events"
DST_TOPIC = "enriched_events"
POSTGRES_URL = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")
PG = {"user": "spotify", "password": "spotify", "driver": "org.postgresql.Driver"}

SCHEMA = StructType([
    StructField("event_id",     StringType()),
    StructField("user_id",      StringType()),
    StructField("track_id",     StringType()),
    StructField("source_peer",  StringType()),
    StructField("timestamp",    StringType()),
    StructField("duration_ms",  IntegerType()),
    StructField("device_type",  StringType()),
    StructField("geo_country",  StringType()),
    StructField("completed",    BooleanType()),
    StructField("event_source", StringType()),
])

spark = SparkSession.builder.appName("streaming_enrichment_job").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Catalogue statique (jointure stream-static) — rechargé au démarrage du job.
catalog = (
    spark.read.format("jdbc")
    .option("url", POSTGRES_URL).option("dbtable", "tracks")
    .option("user", PG["user"]).option("password", PG["password"]).option("driver", PG["driver"])
    .load()
    .select(
        F.col("id").cast("string").alias("track_id"),
        F.col("title").alias("track_title"),
        F.col("artist_id").cast("string").alias("artist_id"),
        F.col("genre"),
    )
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", SRC_TOPIC)
    .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
    .option("failOnDataLoss", "false")
    .load()
)

events = (
    raw.selectExpr("CAST(value AS STRING) AS json")
    .select(F.from_json("json", SCHEMA).alias("e"))
    .select("e.*")
)

# inner join : les events dont le track_id est inconnu du catalogue sont écartés
enriched = events.join(F.broadcast(catalog), "track_id", "inner")

out = enriched.select(
    F.col("event_id").alias("key"),
    F.to_json(F.struct(
        "event_id", "user_id", "track_id", "track_title", "artist_id", "genre",
        "timestamp", "duration_ms", "device_type", "geo_country", "completed", "event_source",
    )).alias("value"),
)

writer = (
    out.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", DST_TOPIC)
    .option("checkpointLocation", os.getenv("CHECKPOINT_DIR", "/tmp/chk/enrichment"))
)
if os.getenv("SPARK_CONTINUOUS") != "1":
    writer = writer.trigger(availableNow=True)

writer.start().awaitTermination()
