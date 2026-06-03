"""
Spark Job #13 — Premier job Spark
==================================
Lit le topic Kafka `listening_events` en streaming, désérialise le JSON,
et affiche les événements dans la console. Sert à valider la chaîne
Simulateur → Kafka → Spark.

Lancement :
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        /opt/spark-jobs/kafka_console_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
TOPIC = "listening_events"

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

spark = SparkSession.builder.appName("kafka_console_job").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw.selectExpr("CAST(value AS STRING) AS json")
    .select(F.from_json("json", SCHEMA).alias("e"))
    .select("e.*")
)

# availableNow=True : traite les messages déjà présents puis s'arrête (pratique pour tester).
# Pour un job qui tourne en continu, retire la ligne .trigger(...).
writer = (
    events.writeStream.format("console")
    .option("truncate", "false")
    .outputMode("append")
)
if os.getenv("SPARK_CONTINUOUS") != "1":
    writer = writer.trigger(availableNow=True)

query = writer.start()
query.awaitTermination()
