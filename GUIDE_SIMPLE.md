# 🎯 GUIDE SUPER SIMPLE — Faire les 25 issues

Pas de mots compliqués. Pour chaque issue je te dis :
**👉 c'est quoi** · **📂 quel fichier ouvrir** · **⌨️ quoi taper** · **✅ comment savoir que c'est bon**.

> 2 types d'issues :
> - 🟢 **À TAPER** = juste des commandes à copier-coller dans le terminal.
> - ✍️ **À CODER** = il faut écrire du code dans un fichier. Ouvre le fichier, et **dis-moi “fais l'issue #X”** → je l'écris pour toi. Tu testes ensuite avec la commande “✅”.

---

## 🟢 LA RECETTE POUR RENDRE CHAQUE ISSUE (toujours pareil)

Quand une issue est finie, tape ça dans le terminal (depuis le dossier du projet) :

```bash
git add .
git commit -m "feat: issue #NUMERO"
git push
```

Remplace `#NUMERO` par le numéro (ex: `issue #4`). C'est tout.
Pour la toute première fois seulement, tape avant : `git push -u origin main`.

### Ouvrir le terminal au bon endroit
```bash
cd /Users/administrateur/Desktop/spotify-project/spotify_hetic
```
Tape cette ligne en premier à chaque fois que tu ouvres un nouveau terminal.

### Les 3 sites web à connaître (ouvre-les dans Chrome)
- Airflow : http://localhost:8080  → user `admin`, mot de passe `admin`
- MinIO : http://localhost:9001  → user `minioadmin`, mot de passe `minioadmin`
- Kafka : http://localhost:8090  (seulement à partir de l'issue #11)

### Dans Airflow, "lancer un DAG" = toujours pareil
1. Va sur http://localhost:8080
2. Trouve le nom du DAG dans la liste
3. Clique sur le **bouton bleu (interrupteur)** à gauche pour l'allumer
4. Clique sur le **▶ (play)** à droite → "Trigger DAG"
5. ✅ Bon = tout devient **vert**. Rouge = ça a planté.

---

# 📅 PHASE 1 — Lundi & Mardi (issues #1 → #10)

D'abord, allume la stack (à faire une seule fois le matin) :
```bash
cd /Users/administrateur/Desktop/spotify-project/spotify_hetic
cp .env.example .env
docker compose up -d
docker compose ps
```
✅ Bon = tu vois plusieurs lignes "healthy"/"running".

---

### ✅ #1 — Setup Docker (DÉJÀ FAIT)
👉 Lancer la stack. **Tu l'as déjà fait** (tes captures dans `rendu/task-1`).
🟢 Rien à faire. Pour vérifier : ouvre http://localhost:8080.

### ✅ #2 — Schéma base de données (DÉJÀ FAIT)
👉 Documenter la base. **Déjà fait** : fichiers `docs/DATA_MODEL.md` et `docs/ARCHITECTURE.md`.
🟢 Rien à coder. Pour vérifier les tables :
```bash
docker compose exec postgres psql -U spotify -d spotify -c "\dt"
```
✅ Bon = tu vois 13 tables.

### ✍️ #3 — Générateur de catalogue (Faker)
👉 Créer de faux artistes / albums / morceaux.
📂 Fichier : `src/data_generator/generate_catalog.py`
⌨️ Pour le lancer :
```bash
docker compose exec airflow-worker python /opt/airflow/src/data_generator/generate_catalog.py
```
✅ Bon = le terminal affiche "X artistes, X morceaux créés".

### ✍️ #4 — DAG catalog_ingestion_pipeline
👉 Mettre le catalogue dans la base.
📂 Fichier : `dags/catalog_ingestion_pipeline.py`
⌨️ Va sur Airflow → lance le DAG **catalog_ingestion_pipeline** (voir recette plus haut).
✅ Bon = tout vert + la table `tracks` se remplit :
```bash
docker compose exec postgres psql -U spotify -d spotify -c "SELECT count(*) FROM tracks;"
```

### ✍️ #5 — Simulateur P2P
👉 Fabriquer de faux événements d'écoute en continu.
📂 Fichier : `src/p2p_simulator/simulator.py`
⌨️ Pour le lancer (laisse cette fenêtre ouverte) :
```bash
docker compose exec airflow-worker python /opt/airflow/src/p2p_simulator/simulator.py
```
✅ Bon = ça affiche des événements qui défilent.

### ✍️ #6 — DAG streaming_events_pipeline
👉 Ranger les événements d'écoute dans la base.
📂 Fichier : `dags/streaming_events_pipeline.py`
⌨️ Airflow → lance **streaming_events_pipeline**.
✅ Bon = vert + la table `listening_events` se remplit.

### ✍️ #7 — DAG aggregation_pipeline (+ MinIO)
👉 Calculer les totaux par jour et sauver des fichiers Parquet.
📂 Fichier : `dags/aggregation_pipeline.py`
⌨️ Airflow → lance **aggregation_pipeline**.
✅ Bon = vert + va sur MinIO http://localhost:9001, tu vois des fichiers dans le bucket.

### ✍️ #8 — DAG recommendation_pipeline
👉 Calculer des recommandations de morceaux.
📂 Fichier : `dags/recommendation_pipeline.py`
⌨️ Airflow → lance **recommendation_pipeline**.
✅ Bon = vert.

### ✍️ #9 (issue #9) — DAG dlq_reprocessing_pipeline
👉 Rattraper les événements cassés (poubelle à messages).
📂 Fichier : `dags/dlq_reprocessing_pipeline.py`
⌨️ Airflow → lance **dlq_reprocessing_pipeline**.
✅ Bon = vert.

### ✍️ #10 — Tests + README
👉 Vérifier que le code marche avec des tests automatiques.
📂 Dossier : `tests/`
⌨️ Pour lancer les tests :
```bash
docker compose exec airflow-worker pytest /opt/airflow/tests -v
```
✅ Bon = ligne verte "passed" en bas.

---

# 📅 PHASE 2 — Mercredi & Jeudi (issues #11 → #20)

### ✍️ #11 — Allumer Kafka
👉 Ajouter Kafka (le tuyau qui transporte les messages).
📂 Fichier : `docker-compose.yml` — la partie Kafka existe mais est **éteinte** (lignes qui commencent par `#`). Il faut enlever les `#`.
⌨️ Après l'avoir modifié :
```bash
docker compose up -d
```
✅ Bon = ouvre Kafka UI http://localhost:8090.

### ✍️ #12 — Brancher le simulateur sur Kafka
👉 Le simulateur envoie ses événements dans Kafka.
📂 Fichier : `src/p2p_simulator/simulator.py`
⌨️ Relance le simulateur (même commande qu'à l'issue #5).
✅ Bon = dans Kafka UI tu vois des messages arriver dans les topics.

### ✍️ #13 — Premier job Spark (affichage console)
👉 Spark lit Kafka et affiche les messages à l'écran.
📂 Fichier : `spark_jobs/` (nouveau fichier, je le crée).
⌨️ Pour lancer un job Spark :
```bash
docker compose exec spark spark-submit /opt/spark_jobs/LE_FICHIER.py
```
✅ Bon = des lignes de données s'affichent dans le terminal.

### ✍️ #14 — Job streaming_trends_job (fenêtres de 5 min)
👉 Calculer les morceaux les plus écoutés toutes les 5 minutes.
📂 Fichier : `spark_jobs/streaming_trends_job.py`
⌨️ `docker compose exec spark spark-submit /opt/spark_jobs/streaming_trends_job.py`
✅ Bon = la table `realtime_top_tracks` se remplit.

### ✍️ #15 — Watermarking (gérer les retards)
👉 Gérer les messages qui arrivent en retard.
📂 Fichier : `spark_jobs/streaming_trends_job.py` (on modifie le même).
⌨️ Relance le job Spark (même commande que #14).
✅ Bon = le job tourne sans planter même avec des données en retard.

### ✍️ #16 — Exactly-once (zéro doublon)
👉 Faire en sorte qu'un message ne soit jamais compté deux fois.
📂 Fichiers : `spark_jobs/streaming_trends_job.py` + réglages dans `docker-compose.yml`.
⌨️ Test : arrête le job (Ctrl+C) puis relance-le.
✅ Bon = pas de doublon dans les résultats après redémarrage.

### ✍️ #17 — Job streaming_enrichment_job
👉 Ajouter des infos (nom de l'artiste, etc.) aux événements.
📂 Fichier : `spark_jobs/streaming_enrichment_job.py` (nouveau).
⌨️ `docker compose exec spark spark-submit /opt/spark_jobs/streaming_enrichment_job.py`
✅ Bon = le job tourne et les événements ont plus d'infos.

### ✍️ #18 — Job fraud_detection_job (détection de triche)
👉 Repérer les faux comptes / robots qui trichent.
📂 Fichier : `spark_jobs/fraud_detection_job.py` (nouveau).
⌨️ `docker compose exec spark spark-submit /opt/spark_jobs/fraud_detection_job.py`
✅ Bon = la table `fraud_detections` se remplit.

### ✍️ #19 — DAG reconciliation_pipeline
👉 Vérifier que les chiffres "batch" et "temps réel" sont d'accord.
📂 Fichier : `dags/reconciliation_pipeline.py` (nouveau).
⌨️ Airflow → lance **reconciliation_pipeline**.
✅ Bon = vert.

### ✍️ #20 — DAG late_events_reprocessing
👉 Rattraper les événements arrivés trop tard.
📂 Fichier : `dags/late_events_reprocessing.py` (nouveau).
⌨️ Airflow → lance **late_events_reprocessing**.
✅ Bon = vert.

---

# 📅 PHASE 3 — Vendredi (issues #21 → #25)

> ⚠️ Cette phase se fait **avec les autres groupes** (vous échangez des données). Il faudra les infos de connexion des autres équipes.

### ✍️ #21 — Data contracts (formats communs)
👉 Écrire les "règles" du format des données partagées.
📂 Dossier : `contracts/` (fichiers `.json` à créer).
⌨️ Pas de commande, c'est de la doc / des fichiers JSON.
✅ Bon = les fichiers `.json` existent dans `contracts/`.

### ✍️ #22 — DAG catalog_federation_pipeline
👉 Récupérer les morceaux des autres groupes.
📂 Fichier : `dags/catalog_federation_pipeline.py` (nouveau).
⌨️ Airflow → lance **catalog_federation_pipeline**.
✅ Bon = la table `federated_catalog` se remplit.

### ✍️ #23 — P2P entre groupes
👉 Échanger des morceaux avec les autres groupes via Kafka.
📂 Fichier : `kafka/cross_group_config.yml` + simulateur.
⌨️ Relance le simulateur.
✅ Bon = au moins un échange de morceau avec un autre groupe marche.

### ✍️ #24 — Top 50 Global
👉 Le classement mondial qui mélange tous les groupes.
📂 Fichier : `dags/global_aggregation_pipeline.py` (nouveau).
⌨️ Airflow → lance **global_aggregation_pipeline**.
✅ Bon = vert + un Top 50 qui contient des morceaux de plusieurs groupes.

### ✍️ #25 — Chaos test + doc finale
👉 Éteindre des morceaux du système exprès pour voir s'il survit, puis finir la doc.
📂 Fichiers : `docs/RUNBOOK.md` + `docs/ARCHITECTURE.md`.
⌨️ Test "chaos" : éteins un service puis rallume-le :
```bash
docker compose stop spark
docker compose start spark
```
✅ Bon = le système se remet à marcher tout seul, et la doc est remplie.

---

# 🆘 SI ÇA PLANTE

- **Rien ne s'ouvre dans le navigateur ?**
  ```bash
  docker compose ps
  ```
  (regarde si c'est "running"). Pour tout relancer : `docker compose restart`.

- **Un DAG est rouge dans Airflow ?** Clique dessus → onglet "Logs" pour voir l'erreur. Copie-moi l'erreur, je corrige.

- **Tout repartir de zéro (efface les données) :**
  ```bash
  docker compose down -v
  docker compose up -d
  ```

- **Bloqué sur une issue “à coder” (✍️) ?** Dis-moi simplement : **“fais l'issue #X”**. J'écris le code, toi tu lances la commande “✅”.

---

# ✅ ORDRE À SUIVRE (ne pas sauter)

Fais-les **dans l'ordre des numéros** : 1 → 2 → 3 … → 25.
Chaque issue a besoin de la précédente. Après chaque issue, fais la **recette git** (tout en haut).
```
#1 ✅ → #2 ✅ → #3 → #4 → #5 → #6 → #7 → #8 → #9 → #10   (Phase 1 finie)
→ #11 → #12 → #13 → #14 → #15 → #16 → #17 → #18 → #19 → #20   (Phase 2 finie)
→ #21 → #22 → #23 → #24 → #25   (Fini ! 🎉)
```

---

# 🛠️ PHASE 1 — PROCÉDURE COMPLÈTE (reproductible seul, sans aide)

> Suis ces étapes **dans l'ordre**, en copiant chaque commande. Le code donné ici a été **testé**.
> Tape `cd /Users/administrateur/Desktop/spotify-project/spotify_hetic` avant chaque commande
> (adapte le chemin à TON dossier si tu es quelqu'un d'autre).

## ÉTAPE 0 — Installer les librairies Python dans Airflow (À FAIRE UNE FOIS)

L'image Airflow de base **n'a pas** faker / boto3 / pyarrow / sklearn. Sans ça, les DAGs plantent.

**a)** Ouvre `docker-compose.yml`. Tout en haut, trouve le bloc :
```yaml
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    ...
    REDIS_URL: redis://redis:6379/1
```
**b)** Juste après la ligne `REDIS_URL: redis://redis:6379/1`, ajoute cette ligne (4 espaces devant) :
```yaml
    _PIP_ADDITIONAL_REQUIREMENTS: "faker==24.0.0 boto3==1.34.0 pandas==2.2.0 pyarrow==15.0.0 redis==5.0.1 scikit-learn==1.4.0 numpy==1.26.4 pytest==8.0.0 pytest-mock==3.12.0"
```
**c)** Sauvegarde, puis :
```bash
docker compose down
docker compose up -d
```
⏳ Attends ~2-3 min (il installe les libs au démarrage).
**d)** Vérifie (doit afficher `LIBS OK`) :
```bash
docker compose exec airflow-worker python -c "import faker, boto3, pyarrow, sklearn, redis, pandas; print('LIBS OK')"
```

## ÉTAPE #3 — Générer le catalogue + l'envoyer dans MinIO

**a)** Générer les 3 fichiers JSON des labels (dans le conteneur) :
```bash
docker compose exec airflow-worker python -m data_generator.generate_catalog --output /opt/airflow/data/labels
```
✅ Affiche "3 catalogues générés".

**b)** Uploader ces 3 fichiers dans le bucket MinIO `labels-raw` :
```bash
docker compose exec airflow-worker python -c "
import boto3, os, glob
s3=boto3.client('s3',endpoint_url=os.getenv('MINIO_ENDPOINT'),aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'))
for f in sorted(glob.glob('/opt/airflow/data/labels/*.json')):
    s3.upload_file(f,'labels-raw',os.path.basename(f)); print('uploaded',os.path.basename(f))
"
```
✅ Affiche `uploaded sunset_records.json` etc. (Vérif possible sur http://localhost:9001 → bucket `labels-raw`.)

## ÉTAPE #4 (1/2) — Remplir le code du DAG catalog_ingestion_pipeline

Ouvre `dags/catalog_ingestion_pipeline.py`. Il contient **4 fonctions qui finissent par
`raise NotImplementedError(...)`**. Remplace **chaque fonction entière** (de sa ligne `@task(...)`
jusqu'à sa ligne `raise NotImplementedError`) par le bloc correspondant ci-dessous.
⚠️ Garde l'indentation : 4 espaces devant `@task`, 8 espaces devant le code.

**Fonction 1 — `extract_from_minio` :**
```python
    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list[dict]:
        """Télécharge les fichiers JSON des labels depuis MinIO."""
        import os, json, boto3
        s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
        catalogs = []
        for fname in LABEL_FILES:
            try:
                obj = s3.get_object(Bucket=MINIO_BUCKET, Key=fname)
                catalogs.append(json.loads(obj["Body"].read()))
                print(f"Lu depuis MinIO : {fname}")
            except Exception as e:
                print(f"WARN : {fname} introuvable ({e}) — on continue")
        return catalogs
```

**Fonction 2 — `validate_schema` :**
```python
    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """Valide le schéma ; les entrées invalides partent en DLQ."""
        import json
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        valid = {"artists": [], "albums": [], "tracks": []}
        errors = 0

        def has(d, fields):
            return all(d.get(f) not in (None, "") for f in fields)

        def to_dlq(payload):
            cur.execute(
                "INSERT INTO dead_letter_events (original_topic, payload, error_type) VALUES (%s, %s, %s)",
                ("catalog_ingestion", json.dumps(payload), "schema_validation"),
            )

        for cat in raw_catalogs:
            for a in cat.get("artists", []):
                if has(a, ["id", "name", "label"]):
                    valid["artists"].append(a)
                else:
                    to_dlq(a); errors += 1
            for al in cat.get("albums", []):
                if has(al, ["id", "artist_id", "title"]):
                    valid["albums"].append(al)
                else:
                    to_dlq(al); errors += 1
            for t in cat.get("tracks", []):
                if has(t, ["id", "artist_id", "title", "duration_ms"]):
                    valid["tracks"].append(t)
                else:
                    to_dlq(t); errors += 1
        conn.commit()
        print(f"Validés : {len(valid['tracks'])} tracks | erreurs DLQ : {errors}")
        return {"valid": valid, "errors_count": errors}
```

**Fonction 3 — `transform_catalog` :**
```python
    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """Normalise les noms d'artistes et filtre les durées aberrantes."""
        valid = validated["valid"]
        artists, seen = [], set()
        for a in valid["artists"]:
            a = {**a, "name": a["name"].strip().title()}
            key = (a["name"], a["label"])
            if key not in seen:
                seen.add(key)
                artists.append(a)
        tracks = [t for t in valid["tracks"] if 0 < t.get("duration_ms", 0) < 3_600_000]
        return {"artists": artists, "albums": valid["albums"], "tracks": tracks}
```

**Fonction 4 — `load_to_postgres` :**
```python
    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """Upsert idempotent dans artists / albums / tracks."""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn(); cur = conn.cursor()
        amap = {}
        for a in transformed["artists"]:
            cur.execute(
                """INSERT INTO artists (id, name, country, label, genres, monthly_listeners)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (name, label) DO UPDATE SET monthly_listeners = EXCLUDED.monthly_listeners
                   RETURNING id""",
                (a["id"], a["name"], a.get("country"), a["label"],
                 a.get("genres"), a.get("monthly_listeners", 0)),
            )
            amap[a["id"]] = cur.fetchone()[0]
        albums = 0
        for al in transformed["albums"]:
            aid = amap.get(al["artist_id"])
            if not aid:
                continue
            cur.execute(
                """INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
                   VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING""",
                (al["id"], aid, al["title"], al.get("release_year"), al.get("total_tracks")),
            )
            albums += 1
        tracks = 0
        for t in transformed["tracks"]:
            aid = amap.get(t["artist_id"])
            if not aid:
                continue
            cur.execute(
                """INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET updated_at = NOW()""",
                (t["id"], t.get("album_id"), aid, t["title"], t["duration_ms"],
                 t.get("genre"), t.get("bpm"), t.get("explicit", False)),
            )
            tracks += 1
        conn.commit()
        context["ti"].xcom_push(key="tracks_inserted", value=tracks)
        print(f"Chargé : {tracks} tracks, {len(amap)} artists, {albums} albums")
        return {"tracks_inserted": tracks, "artists_inserted": len(amap),
                "albums_inserted": albums, "errors_count": 0}
```

Ne touche pas à `notify_success` (déjà rempli) ni à l'ordre des tâches en bas du fichier.

## ÉTAPE #4 (2/2) — Lancer et vérifier

1. Va sur http://localhost:8080 (admin / admin).
2. Allume **catalog_ingestion_pipeline** (interrupteur bleu) puis clique ▶ (Trigger).
3. Attends que tout passe **vert**, puis vérifie :
```bash
docker compose exec postgres psql -U spotify -d spotify -c "SELECT count(*) FROM tracks;"
```
✅ Bon = un nombre autour de **748**.

---

## ÉTAPE #5 — Simulateur P2P

> Le code des fonctions est **déjà dans `src/p2p_simulator/simulator.py`** (générateurs d'events,
> `_publish_to_redis` qui pousse aussi dans une **liste Redis** persistante, et `_load_catalog()`
> qui charge les **vrais track_id** depuis PostgreSQL).

Lancer le simulateur en continu (laisse cette fenêtre ouverte, Ctrl+C pour arrêter) :
```bash
docker compose exec airflow-worker python -m p2p_simulator.simulator --peers 8 --rate 20
```
✅ Bon = logs "Catalogue chargé depuis PostgreSQL : N tracks" puis "Événements publiés : 100, 200…".
Les events s'accumulent dans Redis (`queue:listening_events`) en attendant le DAG #6.

## ÉTAPE #6 — DAG streaming_events_pipeline

> Code déjà dans `dags/streaming_events_pipeline.py` (les 5 fonctions :
> consume_from_redis → validate_events → enrich_events → store_to_parquet → upsert_to_postgres).

Dans Airflow, allume + lance **streaming_events_pipeline** (ou `docker compose exec airflow-scheduler
airflow dags test streaming_events_pipeline`). Vérifie :
```bash
docker compose exec postgres psql -U spotify -d spotify -c "SELECT count(*) FROM listening_events;"
```
✅ Bon = un nombre > 0 qui monte à chaque run. Un fichier Parquet apparaît aussi dans MinIO
(`spotify-parquet/listening_events/date=.../hour=.../`).

## ÉTAPE #7 — DAG aggregation_pipeline

> Code déjà dans `dags/aggregation_pipeline.py`. ⚠️ L'`ExternalTaskSensor` est en `soft_fail`
> + timeout court (10s) : en lancement manuel il "skip" sans bloquer — c'est normal.

Lance **aggregation_pipeline**. Vérifie :
```bash
docker compose exec postgres psql -U spotify -d spotify -c "SELECT (SELECT count(*) FROM daily_streams) AS daily, (SELECT count(*) FROM artist_stats) AS artists;"
```
✅ Bon = deux nombres > 0.

## ÉTAPE #8 — DAG recommendation_pipeline

> Code déjà dans `dags/recommendation_pipeline.py` (collaborative filtering, similarité cosinus
> avec scikit-learn).

⚠️ Pour avoir des recommandations, il faut **assez d'écoutes** : laisse le simulateur (#5) tourner
un peu et relance #6 quelques fois (vise quelques milliers de lignes dans `listening_events`).
Puis lance **recommendation_pipeline**. Vérifie :
```bash
docker compose exec postgres psql -U spotify -d spotify -c "SELECT count(*) FROM recommendations;"
```
✅ Bon = un nombre > 0 (aussi stocké dans Redis sous `reco:{user_id}`).

## ÉTAPE #9 — DAG dlq_reprocessing_pipeline

> Code déjà dans `dags/dlq_reprocessing_pipeline.py` (fetch → reprocess → update_status).

Pour tester, on injecte 1 message réparable + 1 cassé, puis on lance le DAG :
```bash
docker compose exec postgres psql -U spotify -d spotify -c "WITH t AS (SELECT id FROM tracks LIMIT 1) INSERT INTO dead_letter_events (original_topic,payload,error_type,status) SELECT 'listening_events', json_build_object('event_id',gen_random_uuid()::text,'user_id',gen_random_uuid()::text,'track_id',t.id::text,'duration_ms',60000,'completed',true)::jsonb,'manual_test','pending' FROM t;"
docker compose exec postgres psql -U spotify -d spotify -c "INSERT INTO dead_letter_events (original_topic,payload,error_type,status) VALUES ('listening_events','{\"user_id\":null,\"track_id\":\"invalid\"}'::jsonb,'manual_test','pending');"
```
Lance **dlq_reprocessing_pipeline**, puis :
```bash
docker compose exec postgres psql -U spotify -d spotify -c "SELECT status, count(*) FROM dead_letter_events GROUP BY status;"
```
✅ Bon = le message valide passe en `reprocessed`, le cassé reste `pending` (retry +1, puis `abandoned` après 3 essais).

## ÉTAPE #10 — Tests pytest

> `doc_md` est présent sur les DAGs, et `pytest` est dans la liste des libs (ÉTAPE 0).

```bash
docker compose exec -e PYTHONPATH=/opt/airflow:/opt/airflow/src airflow-worker python -m pytest tests -q
```
✅ Bon = `20 passed, 14 skipped` (les 14 "skipped" sont des tests TODO volontairement désactivés).

---

## ✅ RÉCAP — relancer toute la Phase 1 dans l'ordre

```
docker compose up -d                      # ÉTAPE 0
# (générer + uploader le catalogue : ÉTAPE #3)
# laisser tourner le simulateur (ÉTAPE #5) dans une fenêtre à part
```
Puis dans Airflow, allumer + lancer dans CET ordre :
**catalog_ingestion → streaming_events → aggregation → recommendation → dlq_reprocessing**

Compte final attendu (exemple) :
```bash
docker compose exec postgres psql -U spotify -d spotify -c "SELECT (SELECT count(*) FROM tracks) AS tracks, (SELECT count(*) FROM listening_events) AS ecoutes, (SELECT count(*) FROM daily_streams) AS agregats, (SELECT count(*) FROM recommendations) AS recos;"
```

🎉 **Phase 1 (#1 → #10) terminée et testée.**

---

# 🛠️ PHASE 2 — PROCÉDURE COMPLÈTE (Kafka + Spark, issues #11 → #20)

> ⚠️ **Mémoire** : Kafka (3 brokers) + Spark + Airflow, c'est lourd (~6–8 Go RAM Docker).
> Si ça rame ou plante, libère de la RAM en coupant temporairement Airflow :
> `docker compose stop airflow-webserver airflow-worker airflow-scheduler airflow-triggerer`
> (et rallume-les plus tard avec `docker compose start ...`).

## ÉTAPE #11 — Cluster Kafka KRaft ✅ (testé)

Les services Kafka (`kafka-1/2/3`, `kafka-ui`, `kafka-init`) sont dans `docker-compose.yml`.
**3 pièges classiques** (déjà corrigés dans ce repo — à vérifier si tu repars d'un skeleton) :

1. **Déclarer les volumes** : en bas de `docker-compose.yml`, la section `volumes:` doit contenir
   `kafka-1-data:`, `kafka-2-data:`, `kafka-3-data:` (sinon `docker compose` est invalide).
2. **Ports du quorum** : les 3 brokers doivent avoir **le même**
   `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9095,3@kafka-3:9097`
   (ports controller 9093 / 9095 / 9097).
3. **CLUSTER_ID valide** : la variable s'appelle `CLUSTER_ID` (pas `KAFKA_CLUSTER_ID`) et doit être
   un **vrai UUID base64** (22 caractères), identique sur les 3 brokers. Pour en générer un :
   ```bash
   docker compose run --rm --no-deps --entrypoint kafka-storage kafka-1 random-uuid
   ```
   Mets la valeur obtenue dans `CLUSTER_ID:` des 3 brokers.

**Lancer le cluster :**
```bash
docker compose up -d kafka-1 kafka-2 kafka-3 kafka-init kafka-ui
```
⏳ Attends ~30 s (kafka-init crée les topics). **Vérifier :**
```bash
docker compose exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:9092
```
✅ Bon = tu vois 6 topics : `listening_events`, `p2p_network_events`, `catalog_updates`,
`enriched_events`, `fraud_alerts`, `late_listening_events`.
Et l'UI **http://localhost:8090** → onglet Topics s'affiche (plus de page blanche).

> 🐛 **Page blanche dans Kafka UI ?** = un ou plusieurs brokers sont morts. Vérifie :
> `docker compose ps -a kafka-1 kafka-2 kafka-3` (cherche "Exited"), puis les logs :
> `docker compose logs kafka-1 | tail -20`. Si tu vois un message sur `CLUSTER_ID`, c'est le piège #3.

**Santé du cluster (réplication) :**
```bash
docker compose exec kafka-1 kafka-topics --describe --topic listening_events --bootstrap-server kafka-1:9092
```
✅ Bon = `ReplicationFactor: 3` et `Isr: 1,2,3` sur chaque partition.

> ⚙️ **Pré-requis libs** : `confluent-kafka` est dans `_PIP_ADDITIONAL_REQUIREMENTS` (cf. ÉTAPE 0)
> et la variable `KAFKA_BOOTSTRAP: kafka-1:9092,kafka-2:9094,kafka-3:9096` est dans le compose.
> Le code de #12→#15 est **déjà écrit et testé** dans le repo.

## ÉTAPE #12 — Simulateur → Kafka ✅ (testé)

> Code dans `src/p2p_simulator/simulator.py` : `_publish_to_kafka()` (clé = `user_id`/`peer_id`),
> appelé dans `_publish_event` en plus de Redis.

```bash
# produit ~5 s d'événements dans Kafka, puis vérifie le topic
docker compose exec airflow-worker python -m p2p_simulator.simulator --peers 6 --rate 80 &
sleep 6; docker compose exec airflow-worker pkill -f p2p_simulator || true
docker compose exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:9092 --topic listening_events
```
✅ Bon = le total de messages du topic `listening_events` augmente.
(Ou regarde le topic dans Kafka UI http://localhost:8090.)

## ÉTAPE #13 — Premier job Spark (console) ✅ (testé)

> Fichier `spark_jobs/kafka_console_job.py` — lit `listening_events` et l'affiche.

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-jobs/kafka_console_job.py
```
✅ Bon = un tableau d'événements s'affiche (`Batch: 0`, colonnes user_id/track_id/…).
> 💡 **Pièges Spark** (déjà gérés) : utiliser le chemin **absolu** `/opt/spark/bin/spark-submit`,
> et ajouter **`--conf spark.jars.ivy=/tmp/ivy`** (sinon erreur Ivy `.ivy2 No such file`).

## ÉTAPE #14 — streaming_trends_job (fenêtres 5 min) ✅ (testé)

> Fichier `spark_jobs/streaming_trends_job.py` — fenêtres tumbling 5 min → table `realtime_top_tracks`
> (top 10 par fenêtre, upsert idempotent), checkpoint local `/tmp/chk`.

```bash
# 1) produire des événements (cf. ÉTAPE #12)
# 2) lancer le job (le trigger availableNow traite l'existant puis s'arrête)
docker compose exec spark-master bash -c '
rm -rf /tmp/chk/streaming_trends
/opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  /opt/spark-jobs/streaming_trends_job.py'
docker compose exec postgres psql -U spotify -d spotify -c "SELECT count(*), count(DISTINCT window_start) FROM realtime_top_tracks;"
```
✅ Bon = `realtime_top_tracks` se remplit (≈ 10 lignes par fenêtre de 5 min).

## ÉTAPE #15 — Watermarking + late events ✅ (testé)

> Même fichier : `withWatermark("event_time", "10 minutes")` borne l'état ; les events trop en
> retard (>10 min) sont routés vers le topic Kafka `late_listening_events` (pour le DAG #20).

```bash
# produire des events EN RETARD puis relancer le job, et vérifier le topic late
docker compose exec airflow-worker python -c "
import os; os.environ.setdefault('KAFKA_BOOTSTRAP','kafka-1:9092,kafka-2:9094,kafka-3:9096')
from datetime import datetime, timezone, timedelta
from p2p_simulator.simulator import P2PSimulator
sim=P2PSimulator(n_peers=6, events_per_second=100, mode='normal')
for _ in range(50):
    e=sim._generate_listening_event()
    e['timestamp']=(datetime.now(timezone.utc)-timedelta(minutes=18)).isoformat()
    sim._publish_event('listening', e)
sim.kafka.flush(15); print('50 late events produits')
"
docker compose exec spark-master bash -c 'rm -rf /tmp/chk/streaming_trends /tmp/chk/late_events; /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 /opt/spark-jobs/streaming_trends_job.py'
docker compose exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-1:9092 --topic late_listening_events
```
✅ Bon = le topic `late_listening_events` reçoit les events en retard (≈ 50).

## ÉTAPE #16+ (#16 → #20) — À CODER (pas encore fait)

- **#16** — Exactly-once bout-en-bout (checkpoints + idempotence Kafka/Spark).
- **#17** — `streaming_enrichment_job` (jointure stream-static catalogue).
- **#18** — `fraud_detection_job` (stateful, détection bots/free-riders).
- **#19** — DAG `reconciliation_pipeline` (pont batch ↔ streaming).
- **#20** — DAG `late_events_reprocessing` (consomme `late_listening_events`).

➡️ Dis-moi **« fais l'issue #16 »** (etc.) et je l'écris + la teste avant de l'ajouter ici.
