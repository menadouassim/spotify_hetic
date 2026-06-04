"""
DAG #19 — reconciliation_pipeline
==================================
Pont batch ↔ streaming : compare les totaux d'écoutes calculés par la couche
batch (`daily_streams`, via aggregation_pipeline) et par la couche streaming
(`realtime_top_tracks`, via streaming_trends_job), et signale les écarts.

Planification : quotidienne. Idempotent (lecture seule + log).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

POSTGRES_CONN_ID = "spotify_postgres"

DAG_DOC = """
## reconciliation_pipeline (#19)

Compare, par track, le total d'écoutes **batch** (`daily_streams.total_streams`)
et **streaming** (`realtime_top_tracks.stream_count`).

- Source batch     : table `daily_streams`
- Source streaming : table `realtime_top_tracks`
- Sortie : log des totaux + nombre de tracks divergents (XCom `report`).

Les deux couches couvrant des fenêtres temporelles différentes, un écart est
attendu ; le but est de **surveiller** la convergence, pas de la forcer.
"""

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="reconciliation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Réconciliation batch (daily_streams) ↔ streaming (realtime_top_tracks)",
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "reconciliation", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="reconcile")
    def reconcile(**context) -> dict:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records("""
            WITH b AS (SELECT track_id, SUM(total_streams) AS batch       FROM daily_streams       GROUP BY track_id),
                 s AS (SELECT track_id, SUM(stream_count)  AS streaming    FROM realtime_top_tracks GROUP BY track_id)
            SELECT COALESCE(b.track_id, s.track_id) AS track_id,
                   COALESCE(b.batch, 0)            AS batch,
                   COALESCE(s.streaming, 0)        AS streaming
            FROM b FULL OUTER JOIN s ON b.track_id = s.track_id
        """)
        batch_total = sum(int(r[1]) for r in rows)
        streaming_total = sum(int(r[2]) for r in rows)
        diverging = sum(1 for r in rows if int(r[1]) != int(r[2]))
        report = {
            "tracks_compared": len(rows),
            "batch_total": batch_total,
            "streaming_total": streaming_total,
            "diverging_tracks": diverging,
        }
        print(f"Réconciliation : {report}")
        return report

    reconcile()
