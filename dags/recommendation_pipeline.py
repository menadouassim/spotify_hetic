"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.

TODO :
    [ ] Implémenter build_user_track_matrix()
    [ ] Implémenter compute_recommendations()
    [ ] Implémenter store_recommendations()
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins

### TODO
Compléter les 3 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 heures
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    # NB : on lance les DAGs dans l'ordre (… → aggregation → recommendation).
    # L'ExternalTaskSensor d'origine bloque sur les runs manuels, on s'appuie
    # donc sur l'ordre d'exécution.

    MIN_DISTINCT_TRACKS = 2  # un user est "actif" s'il a écouté ≥ 2 titres distincts

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """Matrice user × track (écoutes des 7 derniers jours)."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records("""
            SELECT user_id, track_id, COUNT(*)
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days' AND completed = TRUE
            GROUP BY user_id, track_id
        """)
        matrix = {}
        for u, t, c in rows:
            matrix.setdefault(str(u), {})[str(t)] = int(c)
        active = {u: plays for u, plays in matrix.items() if len(plays) >= MIN_DISTINCT_TRACKS}
        print(f"users actifs (>= {MIN_DISTINCT_TRACKS} titres) : {len(active)} / {len(matrix)}")
        return {"matrix": active}

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """Similarité cosinus entre users → titres aimés par les voisins."""
        matrix = matrix_data["matrix"]
        users = list(matrix.keys())
        if len(users) < 2:
            print("Pas assez d'utilisateurs actifs pour recommander")
            return {}
        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity
        tracks = sorted({t for plays in matrix.values() for t in plays})
        tindex = {t: i for i, t in enumerate(tracks)}
        mat = np.zeros((len(users), len(tracks)))
        for ui, u in enumerate(users):
            for t, c in matrix[u].items():
                mat[ui, tindex[t]] = c
        sim = cosine_similarity(mat)
        recos = {}
        for ui, u in enumerate(users):
            heard = set(matrix[u].keys())
            neighbors = [j for j in sorted(range(len(users)), key=lambda j: sim[ui, j], reverse=True)
                         if j != ui][:TOP_N_RECO]
            scores = {}
            for j in neighbors:
                if sim[ui, j] <= 0:
                    continue
                for t, c in matrix[users[j]].items():
                    if t not in heard:
                        scores[t] = scores.get(t, 0.0) + float(sim[ui, j]) * c
            top = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:TOP_N_RECO]
            if top:
                recos[u] = [{"track_id": t, "score": round(s, 4)} for t, s in top]
        print(f"recommandations générées pour {len(recos)} utilisateurs")
        return recos

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """Stocke les recommandations dans Redis (TTL 24h) et PostgreSQL."""
        import os, json
        import redis
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        r = redis.from_url(os.getenv("REDIS_URL", REDIS_URL), decode_responses=True)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        total = 0
        for user_id, recos in recommendations.items():
            r.setex(f"reco:{user_id}", RECO_TTL_SECONDS, json.dumps([x["track_id"] for x in recos]))
            for x in recos:
                cur.execute(
                    """INSERT INTO recommendations (user_id, track_id, score, generated_at)
                       VALUES (%s, %s, %s, NOW())
                       ON CONFLICT (user_id, track_id) DO UPDATE SET
                         score = EXCLUDED.score, generated_at = NOW()""",
                    (user_id, x["track_id"], x["score"]),
                )
                total += 1
        conn.commit()
        print(f"users_with_recos: {len(recommendations)} | total_recommendations: {total}")
        return {"users_with_recos": len(recommendations), "total_recommendations": total}

    # ── Orchestration ─────────────────────────────────────────
    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)
    store_recommendations(recommendations)
