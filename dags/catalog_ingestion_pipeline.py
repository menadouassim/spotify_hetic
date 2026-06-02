"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()
        → load_to_postgres()
        → notify_success()
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_BUCKET = "labels-raw"
LABEL_FILES = [
    "sunset_records.json",
    "nightwave_music.json",
    "urban_pulse.json"
]

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "ingestion", "catalog"],
) as dag:

    # ─────────────────────────────
    # EXTRACT (AJOUTÉ)
    # ─────────────────────────────
    @task(task_id="extract_from_minio")
    def extract_from_minio():

        import boto3
        import json
        from botocore.client import Config

        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )

        raw_catalogs = []

        for file_name in LABEL_FILES:
            try:
                obj = s3.get_object(
                    Bucket=MINIO_BUCKET,
                    Key=file_name
                )
                content = obj["Body"].read().decode("utf-8")
                raw_catalogs.append(json.loads(content))

            except Exception as e:
                print(f"[WARN] fichier non trouvé ou erreur: {file_name} -> {e}")

        return raw_catalogs

    # ─────────────────────────────
    # VALIDATE
    # ─────────────────────────────
    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:

        valid = {
            "artists": [],
            "albums": [],
            "tracks": []
        }

        errors_count = 0

        for catalog in raw_catalogs:

            for a in catalog.get("artists", []):
                if all(k in a for k in ["id", "name", "label"]):
                    valid["artists"].append(a)
                else:
                    errors_count += 1

            for al in catalog.get("albums", []):
                if all(k in al for k in ["id", "artist_id", "title"]):
                    valid["albums"].append(al)
                else:
                    errors_count += 1

            for t in catalog.get("tracks", []):
                if all(k in t for k in ["id", "artist_id", "album_id", "title", "duration_ms"]):
                    valid["tracks"].append(t)
                else:
                    errors_count += 1

        return {
            "valid": valid,
            "errors_count": errors_count
        }

    # ─────────────────────────────
    # TRANSFORM
    # ─────────────────────────────
    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:

        valid = validated.get("valid", {})

        artists = valid.get("artists", [])
        albums = valid.get("albums", [])
        tracks = valid.get("tracks", [])

        cleaned_artists = {}
        for a in artists:
            name = a["name"].strip().title()
            cleaned_artists[a["id"]] = {
                "id": a["id"],
                "name": name,
                "label": a["label"].strip()
            }

        cleaned_albums = {}
        for al in albums:
            cleaned_albums[al["id"]] = {
                "id": al["id"],
                "artist_id": al["artist_id"],
                "title": al["title"].strip().title()
            }

        cleaned_tracks = {}
        for t in tracks:

            duration = t.get("duration_ms", 0)

            if duration <= 0 or duration > 3_600_000:
                continue

            cleaned_tracks[t["id"]] = {
                "id": t["id"],
                "artist_id": t["artist_id"],
                "album_id": t["album_id"],
                "title": t["title"].strip().title(),
                "duration_ms": duration
            }

        return {
            "artists": list(cleaned_artists.values()),
            "albums": list(cleaned_albums.values()),
            "tracks": list(cleaned_tracks.values())
        }

    # ─────────────────────────────
    # LOAD
    # ─────────────────────────────
    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict):

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        stats = {
            "artists_inserted": 0,
            "albums_inserted": 0,
            "tracks_inserted": 0
        }

        for a in transformed.get("artists", []):
            cursor.execute("""
                INSERT INTO artists (id, name, label)
                VALUES (%s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET name = EXCLUDED.name,
                              label = EXCLUDED.label
            """, (a["id"], a["name"], a["label"]))
            stats["artists_inserted"] += 1

        for al in transformed.get("albums", []):
            cursor.execute("""
                INSERT INTO albums (id, artist_id, title)
                VALUES (%s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET artist_id = EXCLUDED.artist_id,
                              title = EXCLUDED.title
            """, (al["id"], al["artist_id"], al["title"]))
            stats["albums_inserted"] += 1

        for t in transformed.get("tracks", []):
            cursor.execute("""
                INSERT INTO tracks (id, artist_id, album_id, title, duration_ms)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id)
                DO UPDATE SET artist_id = EXCLUDED.artist_id,
                              album_id = EXCLUDED.album_id,
                              title = EXCLUDED.title,
                              duration_ms = EXCLUDED.duration_ms
            """, (t["id"], t["artist_id"], t["album_id"], t["title"], t["duration_ms"]))
            stats["tracks_inserted"] += 1

        conn.commit()
        cursor.close()
        conn.close()

        return stats

    # ─────────────────────────────
    # NOTIFY
    # ─────────────────────────────
    @task(task_id="notify_success")
    def notify_success(stats: dict):
        print(stats)

    # ─────────────────────────────
    # PIPELINE
    # ─────────────────────────────

    raw = extract_from_minio()
    validated = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats = load_to_postgres(transformed)
    notify_success(stats)