"""
DAG #20 — late_events_reprocessing
===================================
Consomme le topic Kafka `late_listening_events` (alimenté par streaming_trends_job
quand un event arrive trop en retard pour sa fenêtre) et réinjecte ces events
dans la table `listening_events` (idempotent).

Planification : toutes les 30 min.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

POSTGRES_CONN_ID = "spotify_postgres"
LATE_TOPIC = "late_listening_events"

DAG_DOC = """
## late_events_reprocessing (#20)

- Source : topic Kafka `late_listening_events` (events routés par le watermark, #15).
- Destination : table `listening_events` (INSERT ... ON CONFLICT (id) DO NOTHING).
- Seuls les events avec un `track_id` connu (FK `tracks`) sont réinjectés.
"""

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="late_events_reprocessing",
    default_args=DEFAULT_ARGS,
    description="Réinjecte les late events (Kafka) dans listening_events",
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "resilience", "late-events"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_late_events")
    def consume_late_events(**context) -> list:
        import os, json, time
        from confluent_kafka import Consumer
        consumer = Consumer({
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092"),
            "group.id": "late_events_reprocessing",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        consumer.subscribe([LATE_TOPIC])
        events, deadline = [], time.time() + 20
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                events.append(json.loads(msg.value()))
            except Exception:
                pass
        consumer.close()
        print(f"{len(events)} late events consommés depuis {LATE_TOPIC}")
        return events

    @task(task_id="reinject")
    def reinject(events: list, **context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        if not events:
            return {"inserted": 0}
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        ids = list({e.get("track_id") for e in events if e.get("track_id")})
        cur.execute("SELECT id FROM tracks WHERE id = ANY(%s::uuid[])", (ids,))
        known = {str(r[0]) for r in cur.fetchall()}
        n = 0
        for e in events:
            if not e.get("event_id") or not e.get("user_id") or e.get("track_id") not in known:
                continue
            cur.execute(
                """INSERT INTO listening_events
                   (id, user_id, track_id, timestamp, duration_ms, device_type,
                    geo_country, completed, event_source)
                   VALUES (%s, %s, %s, COALESCE(%s, NOW()), %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO NOTHING""",
                (e["event_id"], e["user_id"], e["track_id"], e.get("timestamp"),
                 e.get("duration_ms") or 0, e.get("device_type"), e.get("geo_country"),
                 e.get("completed", False), e.get("event_source", "late")),
            )
            n += 1
        conn.commit()
        print(f"{n} late events réinjectés dans listening_events")
        return {"inserted": n}

    reinject(consume_late_events())
