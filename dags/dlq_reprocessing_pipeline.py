"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé

Architecture :
    PostgreSQL dead_letter_events (status='pending')
        → fetch_pending_dlq()       ← récupérer les events à retraiter
        → reprocess_events()        ← tenter de corriger et réinjecter
        → update_dlq_status()       ← marquer reprocessed ou abandoned

TODO :
    [ ] Implémenter fetch_pending_dlq()
    [ ] Implémenter reprocess_events()
    [ ] Implémenter update_dlq_status()
    [ ] Tester avec injection de données corrompues
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'\''injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100   # traiter par lots pour ne pas surcharger


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """Récupère les events DLQ en attente (status='pending', retry < max)."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records(
            """SELECT id, payload, error_type, retry_count, original_topic
               FROM dead_letter_events
               WHERE status = 'pending' AND retry_count < %s
               ORDER BY created_at ASC LIMIT %s""",
            parameters=(MAX_RETRIES, BATCH_SIZE),
        )
        result = [{"id": str(r[0]), "payload": r[1], "error_type": r[2],
                   "retry_count": int(r[3]), "original_topic": r[4]} for r in rows]
        print(f"{len(result)} événements pending trouvés")
        return result

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """Valide les payloads ; ceux corrigeables sont prêts à réinjecter."""
        import json, uuid as _uuid
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        if not pending_events:
            return {"reprocessed": [], "failed": []}
        candidate, failed = [], []
        for e in pending_events:
            p = e["payload"]
            if isinstance(p, str):
                try:
                    p = json.loads(p)
                except Exception:
                    p = {}
            uid, tid = p.get("user_id"), p.get("track_id")
            if not uid or not tid:
                failed.append(e["id"]); continue
            try:
                _uuid.UUID(str(tid))
            except Exception:
                failed.append(e["id"]); continue   # track_id non-UUID → irrécupérable
            candidate.append({"id": e["id"], "event": p})
        reprocessed = []
        if candidate:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            ids = list({str(c["event"]["track_id"]) for c in candidate})
            rows = hook.get_records("SELECT id FROM tracks WHERE id = ANY(%s::uuid[])", parameters=(ids,))
            existing = {str(x[0]) for x in rows}
            for c in candidate:
                if str(c["event"]["track_id"]) in existing:
                    reprocessed.append(c)
                else:
                    failed.append(c["id"])            # track inconnu → échec
        print(f"à réinjecter : {len(reprocessed)} | échecs : {len(failed)}")
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """Réinjecte les events réparés et met à jour les statuts DLQ."""
        import uuid as _uuid
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        reproc = 0
        for item in results["reprocessed"]:
            e = item["event"]
            eid = e.get("event_id") or str(_uuid.uuid4())
            cur.execute(
                """INSERT INTO listening_events (id, user_id, track_id, timestamp, duration_ms, completed)
                   VALUES (%s, %s, %s, COALESCE(%s, NOW()), %s, %s)
                   ON CONFLICT (id) DO NOTHING""",
                (eid, e["user_id"], e["track_id"], e.get("timestamp"),
                 e.get("duration_ms") or 0, e.get("completed", False)),
            )
            cur.execute("UPDATE dead_letter_events SET status='reprocessed', resolved_at=NOW() WHERE id=%s",
                        (item["id"],))
            reproc += 1
        abandoned = pending = 0
        for dlq_id in results["failed"]:
            cur.execute(
                """UPDATE dead_letter_events
                   SET retry_count = retry_count + 1, last_retry_at = NOW(),
                       status = CASE WHEN retry_count + 1 >= 3 THEN 'abandoned' ELSE 'pending' END
                   WHERE id = %s RETURNING status""",
                (dlq_id,),
            )
            row = cur.fetchone()
            if row and row[0] == "abandoned":
                abandoned += 1
            else:
                pending += 1
        conn.commit()
        print(f"{reproc} retraités, {abandoned} abandonnés, {pending} encore en pending")
        return {"reprocessed": reproc, "abandoned": abandoned, "pending": pending}

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
