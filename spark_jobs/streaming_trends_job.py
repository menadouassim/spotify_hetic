"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    - Redis      → clé `top_tracks:live` (top genres par sliding window)

Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/streaming_trends_job.py

TODO :
    [ ] Implémenter la lecture du topic Kafka avec readStream
    [ ] Désérialiser les messages JSON avec le bon schéma
    [ ] Implémenter les fenêtres tumbling de 5 minutes
    [ ] Implémenter les sliding windows pour les genres (15 min / 5 min)
    [ ] Configurer le checkpoint sur MinIO
    [ ] Écrire les résultats dans PostgreSQL et Redis
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
# Checkpoint local pour le test (sinon s3a://spotify-checkpoints/streaming_trends)
CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH", "/tmp/spark_checkpoints/streaming_trends")
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":   "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),  # ISO 8601 → à caster en Timestamp
    StructField("duration_ms", IntegerType(),   True),
    StructField("device_type", StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("completed",   BooleanType(),   True),
    StructField("event_source",StringType(),    True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Crée et configure la SparkSession avec les dépendances nécessaires.

    TODO : vérifier que les packages kafka et postgresql sont disponibles
    """
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed_df = (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
    )
    return parsed_df

# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top 10 des tracks par tumbling window de 5 minutes avec sémantique Exactly-Once.
    Pour le test : affichage dans la console + checkpoint local
    """
    # 1. Groupement par tumbling window de 5 minutes
    aggregated_df = (
        events_df
        .groupBy(F.window("event_time", "5 minutes"), "track_id")
        .agg(
            F.count("*").alias("stream_count"),
            F.approx_count_distinct("user_id").alias("unique_listeners")
        )
    )

    # 2. Top 10 par window
    from pyspark.sql.window import Window as W
    window_spec = W.partitionBy("window").orderBy(F.col("stream_count").desc())
    
    top_10_df = (
        aggregated_df
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") <= 10)
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "track_id",
            "stream_count",
            "unique_listeners"
        )
    )

    # 3. Écriture avec console sink + checkpoint (pour démo Exactly-Once)
    query = (
        top_10_df.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/top_tracks")
        .start()
    )
    return query

def route_late_events(events_df):
    """#15 — route les events trop en retard (>10 min) vers le topic late_listening_events.

    Ces events seront retraités plus tard par le DAG Airflow late_events_reprocessing (#20).
    """
    late = (
        events_df
        .filter(F.col("event_time") < (F.current_timestamp() - F.expr("INTERVAL 10 MINUTES")))
        .select(
            F.col("event_id").alias("key"),
            F.to_json(F.struct(
                "event_id", "user_id", "track_id", "timestamp",
                "duration_ms", "device_type", "geo_country", "completed", "event_source",
            )).alias("value"),
        )
    )
    writer = (
        late.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "late_listening_events")
        .option("checkpointLocation", os.getenv("CHECKPOINT_DIR_LATE", "/tmp/chk/late_events"))
    )
    if os.getenv("SPARK_CONTINUOUS") != "1":
        writer = writer.trigger(availableNow=True)
    return writer.start()


def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Listeners uniques par genre en sliding window (15 min glissant toutes les 5 min).

    TODO :
        1. Joindre events_df avec catalog_df (stream-static join sur track_id)
           pour récupérer le genre du morceau
        2. groupBy(window("event_time", "15 minutes", "5 minutes"), "genre")
        3. agg(countDistinct("user_id").alias("unique_listeners"))
        4. Écrire dans Redis (clé "genre_listeners:live") via foreachBatch
           Utiliser redis-py dans le batch

    Hint : charger le catalogue PostgreSQL comme DataFrame statique avec spark.read.jdbc()
    """
    raise NotImplementedError("TODO : implémenter compute_genre_listeners_sliding()")


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    # Lecture Kafka
    events_df = read_kafka_stream(spark)

    # Chargement du catalogue (jointure statique — Phase 2, seq 2.3)
    # catalog_df = spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)

    # Agrégations + routage des late events
    query_top_tracks = compute_top_tracks_tumbling(events_df)
    query_late_events = route_late_events(events_df)
    
    # Attendre la terminaison de l'une ou l'autre query (elles s'exécutent en parallèle)
    print("Streaming actif — en attente d'événements Kafka...")
    try:
        # Attendre que le premier streaming s'arrête (Ctrl+C ou erreur)
        query_top_tracks.awaitTermination()
    except KeyboardInterrupt:
        print("\n⚠ Arrêt du streaming demandé...")
        query_top_tracks.stop()
        query_late_events.stop()
        print("✓ Streaming arrêté proprement")


if __name__ == "__main__":
    main()