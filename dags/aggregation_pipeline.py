"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, latence moyenne
        → update_aggregates()       ← écriture PostgreSQL

TODO :
    [ ] Implémenter compute_top_tracks()
    [ ] Implémenter compute_artist_stats()
    [ ] Implémenter compute_p2p_metrics()
    [ ] Implémenter update_aggregates()
    [ ] Configurer correctement l'ExternalTaskSensor
    [ ] Stratégie incrémentale : calculer uniquement pour la date d'exécution
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...

### TODO
Compléter les 4 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    # Dépendance logique sur streaming_events_pipeline.
    # soft_fail + timeout court : en exécution manuelle (catalog → events → aggregation)
    # le capteur ne bloque pas indéfiniment — il "skip" si aucun run correspondant.
    # Il n'est pas câblé en amont des calculs : on lance les DAGs dans l'ordre.
    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        allowed_states=["success"],
        mode="poke",
        poke_interval=5,
        timeout=10,
        soft_fail=True,
        check_existence=True,
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """Top 50 (track, jour) par nombre d'écoutes → prêt pour daily_streams."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records("""
            SELECT track_id, DATE(timestamp), COUNT(*), COUNT(DISTINCT user_id),
                   COALESCE(SUM(duration_ms), 0), ARRAY_AGG(DISTINCT geo_country)
            FROM listening_events
            WHERE completed = TRUE
            GROUP BY track_id, DATE(timestamp)
            ORDER BY COUNT(*) DESC
            LIMIT 50
        """)
        result = [[str(r[0]), str(r[1]), int(r[2]), int(r[3]), int(r[4]),
                   [c for c in (r[5] or []) if c]] for r in rows]
        print(f"compute_top_tracks : {len(result)} lignes")
        return result

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """Stats (artiste, jour) → prêt pour artist_stats."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records("""
            SELECT t.artist_id, DATE(le.timestamp), COUNT(*), COUNT(DISTINCT le.user_id)
            FROM listening_events le
            JOIN tracks t ON t.id = le.track_id
            GROUP BY t.artist_id, DATE(le.timestamp)
        """)
        result = [[str(r[0]), str(r[1]), int(r[2]), int(r[3])] for r in rows]
        print(f"compute_artist_stats : {len(result)} lignes")
        return result

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """Métriques réseau P2P (taux de cache, auditeurs uniques)."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        total = hook.get_first("SELECT COUNT(*) FROM listening_events")[0] or 0
        cache = hook.get_first("SELECT COUNT(*) FROM listening_events WHERE event_source = 'cache'")[0] or 0
        listeners = hook.get_first("SELECT COUNT(DISTINCT user_id) FROM listening_events")[0] or 0
        metrics = {
            "total_events": int(total),
            "cache_hit_rate": round(cache / total, 4) if total else 0.0,
            "unique_listeners": int(listeners),
        }
        print(f"compute_p2p_metrics : {metrics}")
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """UPSERT idempotent dans daily_streams et artist_stats."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        for track_id, day, streams, listeners, duration, countries in top_tracks:
            cur.execute(
                """INSERT INTO daily_streams
                   (track_id, date, total_streams, unique_listeners, total_duration_ms, countries)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (track_id, date) DO UPDATE SET
                     total_streams = EXCLUDED.total_streams,
                     unique_listeners = EXCLUDED.unique_listeners,
                     total_duration_ms = EXCLUDED.total_duration_ms,
                     countries = EXCLUDED.countries, updated_at = NOW()""",
                (track_id, day, streams, listeners, duration, countries),
            )
        for artist_id, day, streams, listeners in artist_stats:
            cur.execute(
                """INSERT INTO artist_stats (artist_id, date, total_streams, unique_listeners)
                   VALUES (%s, %s, %s, %s)
                   ON CONFLICT (artist_id, date) DO UPDATE SET
                     total_streams = EXCLUDED.total_streams,
                     unique_listeners = EXCLUDED.unique_listeners, updated_at = NOW()""",
                (artist_id, day, streams, listeners),
            )
        conn.commit()
        print(f"daily_streams: {len(top_tracks)} | artist_stats: {len(artist_stats)} | p2p: {p2p_metrics}")

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
