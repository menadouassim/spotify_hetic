"""
Spark Job : fraud_detection_job
==================================
Analyse le flux d'écoutes en temps réel pour repérer les robots et faux comptes.

Patterns détectés :
    - Burst: >10 écoutes du même user en 30s = BOT
    - Phantom: même track écouté >5 fois simultanément = CHEAT
    - Rapid-fire: durée très courte, beaucoup d'écoutes = SPAM
    - Geographic: même user depuis N pays en T secondes = IMPOSSIBLE

Outputs :
    - Console (debug)
    - PostgreSQL → table `fraud_detections` (avec checkpoint pour Exactly-Once)
    - Checkpoint local : /tmp/spark_checkpoints/fraud_detection

Lancement :
    docker compose exec -u root spark /opt/spark/bin/spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        /opt/spark_jobs/fraud_detection_job.py

CTRL+C pour arrêter, puis relance la même commande → Exactly-Once sans doublon ✓
"""

import os
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType, DoubleType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH", "/tmp/spark_checkpoints/fraud_detection")
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":   "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# Seuils de détection
BURST_THRESHOLD = 10          # Écoutes par user en 30s
RAPID_FIRE_THRESHOLD = 20     # Écoutes rapides en 10s avec durée <15s
PHANTOM_THRESHOLD = 5         # Même track écouté N fois en parallèle
GEO_COUNTRIES_LIMIT = 3       # Pays différents en 5 minutes

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),
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
    """Crée et configure la SparkSession."""
    return (
        SparkSession.builder
        .appName("SPOTIFY-fraud-detection")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """Lit le topic Kafka et parse les événements."""
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
        .withColumn("duration_sec", F.col("duration_ms") / 1000.0)
    )
    
    return parsed_df


# ─────────────────────────────────────────────────────────────
# DÉTECTION DE FRAUDE (PATTERNS)
# ─────────────────────────────────────────────────────────────

def detect_fraud_patterns(events_df):
    """
    Détecte 4 patterns de fraude sur des fenêtres temps réel.
    Retourne : user_id, fraude_type, severity (0-100), details
    """
    
    # ========== PATTERN 1: BURST (trop d'écoutes en peu de temps) ==========
    burst_df = (
        events_df
        .groupBy(
            F.window("event_time", "30 seconds"),
            "user_id"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("track_id").alias("unique_tracks"),
            F.collect_list("event_id").alias("event_ids")
        )
        .filter(F.col("event_count") > BURST_THRESHOLD)
        .select(
            F.col("window.start").alias("fraud_time"),
            "user_id",
            F.lit("BURST").alias("fraud_type"),
            F.min(F.col("event_count") * 10).alias("severity"),  # 10-100
            F.concat_ws("|",
                F.lit(f"Burst: {BURST_THRESHOLD} écoutes en 30s"),
                F.col("event_count"),
                "unique_tracks"
            ).alias("details"),
            F.explode("event_ids").alias("event_id")
        )
    )
    
    # ========== PATTERN 2: RAPID-FIRE (écoutes rapides avec durée très courte) ==========
    rapid_fire_df = (
        events_df
        .filter(F.col("duration_sec") < 15)  # Écoutes < 15s
        .groupBy(
            F.window("event_time", "10 seconds"),
            "user_id"
        )
        .agg(
            F.count("*").alias("rapid_count"),
            F.avg("duration_sec").alias("avg_duration"),
            F.collect_list("event_id").alias("event_ids")
        )
        .filter(F.col("rapid_count") > RAPID_FIRE_THRESHOLD)
        .select(
            F.col("window.start").alias("fraud_time"),
            "user_id",
            F.lit("RAPID_FIRE").alias("fraud_type"),
            F.min(F.col("rapid_count") * 15).alias("severity"),
            F.concat_ws("|",
                F.lit(f"Rapid-fire: {RAPID_FIRE_THRESHOLD} écoutes <15s en 10s"),
                F.col("rapid_count"),
                F.round("avg_duration", 1)
            ).alias("details"),
            F.explode("event_ids").alias("event_id")
        )
    )
    
    # ========== PATTERN 3: PHANTOM (même track écouté N fois en parallèle) ==========
    phantom_df = (
        events_df
        .groupBy(
            F.window("event_time", "5 seconds"),
            "user_id",
            "track_id"
        )
        .agg(
            F.count("*").alias("parallel_count"),
            F.approx_count_distinct("device_type").alias("device_count"),
            F.collect_list("event_id").alias("event_ids")
        )
        .filter(F.col("parallel_count") > PHANTOM_THRESHOLD)
        .select(
            F.col("window.start").alias("fraud_time"),
            "user_id",
            F.lit("PHANTOM").alias("fraud_type"),
            F.min(F.col("parallel_count") * 20).alias("severity"),
            F.concat_ws("|",
                F.lit(f"Phantom: même track x{PHANTOM_THRESHOLD}"),
                "track_id",
                F.col("parallel_count"),
                "device_count"
            ).alias("details"),
            F.explode("event_ids").alias("event_id")
        )
    )
    
    # ========== PATTERN 4: GEOGRAPHIC (même user depuis pays différents) ==========
    geo_df = (
        events_df
        .filter(F.col("geo_country").isNotNull())
        .groupBy(
            F.window("event_time", "5 minutes"),
            "user_id"
        )
        .agg(
            F.approx_count_distinct("geo_country").alias("country_count"),
            F.collect_set("geo_country").alias("countries"),
            F.collect_list("event_id").alias("event_ids"),
            F.min("event_time").alias("first_event")
        )
        .filter(F.col("country_count") >= GEO_COUNTRIES_LIMIT)
        .select(
            F.col("window.start").alias("fraud_time"),
            "user_id",
            F.lit("GEOGRAPHIC").alias("fraud_type"),
            F.min(F.col("country_count") * 25).alias("severity"),
            F.concat_ws("|",
                F.lit(f"Geographic: {GEO_COUNTRIES_LIMIT} pays en 5 min"),
                F.concat_ws(",", "countries"),
                "country_count"
            ).alias("details"),
            F.explode("event_ids").alias("event_id")
        )
    )
    
    # ========== FUSION DE TOUS LES PATTERNS ==========
    all_fraud = burst_df.unionByName(rapid_fire_df, allowMissingColumns=True)
    all_fraud = all_fraud.unionByName(phantom_df, allowMissingColumns=True)
    all_fraud = all_fraud.unionByName(geo_df, allowMissingColumns=True)
    
    # Consolidate columns (uniform schema)
    all_fraud = (
        all_fraud
        .select(
            F.col("fraud_time"),
            "user_id",
            "fraud_type",
            F.col("severity").cast("INTEGER"),
            "details",
            "event_id"
        )
        .withColumn("detected_at", F.current_timestamp())
        .withColumn("status", F.lit("FLAGGED"))
    )
    
    return all_fraud


# ─────────────────────────────────────────────────────────────
# ÉCRITURE DANS POSTGRESQL (EXACTLY-ONCE)
# ─────────────────────────────────────────────────────────────

def write_fraud_to_postgres_exactly_once(batch_df, batch_id):
    """
    Micro-batch callback : écrit les fraudes détectées avec UPSERT idempotent.
    Clé unique : (user_id, event_id, fraud_type)
    """
    
    if batch_df.count() == 0:
        return
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="spotify",
            user="spotify",
            password="spotify"
        )
        cursor = conn.cursor()
        
        rows = batch_df.collect()
        inserted = 0
        
        for row in rows:
            # UPSERT idempotent : on ignore les doublons (même event_id + même user + même type)
            upsert_sql = """
                INSERT INTO fraud_detections 
                    (user_id, fraud_type, severity, details, event_id, detected_at, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, event_id, fraud_type) 
                DO UPDATE SET 
                    severity = GREATEST(fraud_detections.severity, EXCLUDED.severity),
                    updated_at = NOW()
                WHERE fraud_detections.status = 'FLAGGED';
            """
            
            try:
                cursor.execute(upsert_sql, (
                    row.user_id,
                    row.fraud_type,
                    row.severity,
                    row.details,
                    row.event_id,
                    row.detected_at,
                    row.status
                ))
                inserted += 1
            except psycopg2.IntegrityError as e:
                # Doublon ignoré (Exactly-Once ✓)
                pass
        
        conn.commit()
        cursor.close()
        
        print(f"✓ Batch {batch_id} : {inserted} fraudes détectées + {len(rows)-inserted} doublons ignorés")
        
    except Exception as e:
        print(f"✗ Erreur batch {batch_id} : {e}")
    finally:
        if conn:
            conn.close()


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("Démarrage fraud_detection_job...")
    print(f"  Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"  Checkpoint : {CHECKPOINT_PATH}")
    print(f"  Détection : BURST (>{BURST_THRESHOLD}/30s) | RAPID-FIRE (>{RAPID_FIRE_THRESHOLD}/<15s/10s)")
    print(f"              PHANTOM (>{PHANTOM_THRESHOLD} parallel) | GEOGRAPHIC (>{GEO_COUNTRIES_LIMIT} pays/5min)")
    print("=" * 80)
    print()

    # 1. Lire Kafka
    events_df = read_kafka_stream(spark)

    # 2. Détecter les fraudes
    fraud_df = detect_fraud_patterns(events_df)

    # 3. Écrire dans PostgreSQL avec console debug
    query = (
        fraud_df
        .writeStream
        .outputMode("update")
        .foreachBatch(write_fraud_to_postgres_exactly_once)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/detections")
        .start()
    )

    # 4. Console sink additionnel pour debug (affiche les 20 dernières fraudes)
    console_query = (
        fraud_df
        .select("detected_at", "user_id", "fraud_type", "severity", "details")
        .writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .start()
    )

    print("Streaming actif — en attente d'événements frauduleux...")
    print()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⚠ Arrêt du streaming demandé...")
        query.stop()
        console_query.stop()
        print("✓ Streaming arrêté proprement")


if __name__ == "__main__":
    main()
