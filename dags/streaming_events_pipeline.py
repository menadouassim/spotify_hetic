"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (pub/sub),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)

Architecture :
    Redis (pub/sub listening_events + p2p_network_events)
        → consume_from_redis()
        → validate_events()          ← invalides → DLQ
        → enrich_events()            ← jointure catalogue PostgreSQL
        → store_to_parquet()         ← MinIO partitionné par heure
        → upsert_to_postgres()       ← table listening_events

TODO :
    [ ] Implémenter consume_from_redis() — accumuler les events sur 5 min
    [ ] Implémenter validate_events() — champs obligatoires, envoyer invalides en DLQ
    [ ] Implémenter enrich_events() — joindre avec le catalogue (track_id → artiste, genre)
    [ ] Implémenter store_to_parquet() — Parquet sur MinIO partitionné par heure
    [ ] Implémenter upsert_to_postgres() — insérer dans listening_events
    [ ] Utiliser TaskFlow API (@task) pour toutes les tâches
    [ ] Ajouter des branches conditionnelles : séparer listening_events et p2p_network_events
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis channel `listening_events`
- Redis channel `p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID). L'upsert utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.

### TODO
Compléter les 5 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_CHANNELS   = ["listening_events", "p2p_network_events"]
BATCH_WINDOW_SEC = 300  # 5 minutes


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """Lit les events accumulés par le simulateur dans les listes Redis."""
        import os, json
        import redis
        r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/1"), decode_responses=True)
        out = {"listening": [], "p2p_network": []}
        queues = {"listening": "queue:listening_events", "p2p_network": "queue:p2p_network_events"}
        for key, qname in queues.items():
            for _ in range(50000):
                item = r.rpop(qname)
                if item is None:
                    break
                try:
                    out[key].append(json.loads(item))
                except Exception:
                    pass
        print(f"Consommé {len(out['listening'])} listening, {len(out['p2p_network'])} p2p")
        return out

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """Valide les listening_events ; les invalides partent en DLQ."""
        import json
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        valid, errors = [], 0
        for e in raw_events.get("listening", []):
            ok = all(e.get(f) not in (None, "") for f in ["event_id", "user_id", "track_id", "timestamp"]) \
                and isinstance(e.get("duration_ms"), int) and e["duration_ms"] > 0
            if ok:
                valid.append(e)
            else:
                errors += 1
                cur.execute(
                    "INSERT INTO dead_letter_events (original_topic, payload, error_type) VALUES (%s, %s, %s)",
                    ("listening_events", json.dumps(e), "validation"),
                )
        conn.commit()
        print(f"Valides : {len(valid)} | invalides → DLQ : {errors}")
        return {"valid_listening": valid, "errors": errors}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """Ajoute genre/artist_id/track_title ; track_id inconnu → DLQ."""
        import json
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        events = validated["valid_listening"]
        if not events:
            return []
        ids = list({e["track_id"] for e in events})
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        cur.execute("SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%s::uuid[])", (ids,))
        catalog = {str(r[0]): {"track_title": r[1], "artist_id": str(r[2]), "genre": r[3]}
                   for r in cur.fetchall()}
        enriched, unknown = [], 0
        for e in events:
            info = catalog.get(e["track_id"])
            if info is None:
                unknown += 1
                cur.execute(
                    "INSERT INTO dead_letter_events (original_topic, payload, error_type) VALUES (%s, %s, %s)",
                    ("listening_events", json.dumps(e), "unknown_track"),
                )
                continue
            enriched.append({**e, **info})
        conn.commit()
        print(f"Enrichis : {len(enriched)} | inconnus → DLQ : {unknown}")
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """Écrit les events en Parquet sur MinIO, partitionné par date/heure."""
        if not enriched_events:
            return ""
        import os, io
        import boto3, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
        df = pd.DataFrame(enriched_events)
        ts = pd.to_datetime(df["timestamp"], format="mixed", utc=True)
        date = ts.dt.strftime("%Y-%m-%d").iloc[0]
        hour = ts.dt.strftime("%H").iloc[0]
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pandas(df), buf)
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
        run_id = str(context.get("run_id", "manual")).replace(":", "-").replace("+", "-")
        key = f"listening_events/date={date}/hour={hour}/part-{run_id}.parquet"
        s3.put_object(Bucket=os.getenv("MINIO_BUCKET_PARQUET", "spotify-parquet"),
                      Key=key, Body=buf.getvalue())
        print(f"Parquet écrit : s3://spotify-parquet/{key}")
        return key

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """Insère les events dans listening_events (idempotent)."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        if not enriched_events:
            return {"inserted": 0}
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor(); n = 0
        for e in enriched_events:
            cur.execute(
                """INSERT INTO listening_events
                   (id, user_id, track_id, source_peer_id, timestamp, duration_ms,
                    device_type, geo_country, completed, event_source)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO NOTHING""",
                (e["event_id"], e["user_id"], e["track_id"], None, e["timestamp"],
                 e["duration_ms"], e.get("device_type"), e.get("geo_country"),
                 e.get("completed", False), e.get("event_source", "p2p")),
            )
            n += 1
        conn.commit()
        print(f"Insérés dans listening_events : {n}")
        return {"inserted": n}

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
