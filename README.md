# POC Data Platform

POC d'une plateforme d'ingestion de données : **Apache Airflow** orchestre des pipelines
**dlt** (data load tool) qui chargent des données depuis des sources (API REST, bases SQL)
vers des destinations (MinIO/S3, Azure ADLS, PostgreSQL).

Le parti pris de l'architecture : **chaque pipeline s'exécute dans son propre pod Kubernetes**
(`KubernetesPodOperator`), et non dans le worker Airflow. Le DAG ne fait que valider une
configuration et lancer un pod ; toute la logique dlt vit dans `scripts/`.

---

## Sommaire

- [Architecture](#architecture)
- [Structure du dépôt](#structure-du-dépôt)
- [Prérequis](#prérequis)
- [Exécution sur Kubernetes](#exécution-sur-kubernetes)
- [Configuration d'un pipeline](#configuration-dun-pipeline)
- [Secrets et variables d'environnement](#secrets-et-variables-denvironnement)
- [État actuel du POC](#état-actuel-du-poc)

---

## Architecture

```
Airflow (scheduler / DAG)          image localhost:5000/airflow-custom:<tag>
      |                            construite par airflow.dockerfile
      |                            deployee par le chart Helm (values.yaml)
      |
      |  DltPipelineOperator (hérite de KubernetesPodOperator)
      |   1. valide le JSON de config avec Pydantic
      |   2. déduit les secrets K8s nécessaires
      |   3. injecte PIPELINE_CONFIG dans l'env du pod
      v
Pod Kubernetes dédié               image localhost:5000/dlt-pipeline:<tag>
      |  python /app/<script>.py   construite par dockerfile
      v
Pipeline dlt  -->  Source (REST API, SQL)  -->  Destination (MinIO/S3, ADLS, Postgres)
```

### Deux images, deux responsabilités

Le dépôt produit deux images qui ne partagent aucune dépendance :

| | **`dockerfile`** | **`airflow.dockerfile`** |
|---|---|---|
| Image | `localhost:5000/dlt-pipeline` | `localhost:5000/airflow-custom` |
| Base | `python:3.12-slim` | `apache/airflow:3.2.2` |
| Contenu | `scripts/` + dlt et ses dépendances | `dags/` + `scripts/` |
| Rôle | exécuter `api_to_minio.py` dans un pod | faire tourner Airflow (scheduler, API server, workers) |
| Déployée par | `DltPipelineOperator` | le chart Helm ([values.yaml](values.yaml)) |

L'image du pipeline ne contient **pas** Airflow, et l'image Airflow ne contient
**pas** dlt : un pipeline qui plante n'emporte pas l'ordonnanceur, et les montées
de version des deux mondes sont indépendantes.

Ajouter un nouveau pattern d'ingestion revient à écrire un script dans `scripts/`
et un DAG de quelques lignes, sans dupliquer la plomberie Kubernetes.

---

## Structure du dépôt

```
dags/
  dag_api_to_minio_V2.py    DAG : API REST -> MinIO (config JSON unique)
  dlt_pipeline_operator.py  Opérateur réutilisable : valide la config, mappe les
                            secrets, lance le pod
scripts/
  api_to_minio.py           Logique dlt (REST API -> S3/MinIO) + entrypoint :
                            lit PIPELINE_CONFIG et lance le pipeline
  pipeline_config.py        Schémas Pydantic (sources, destinations) + helpers credentials
  _common.py                setup_logging() + load_config() partagés par les scripts
dockerfile                  Image d'exécution des pipelines (python:3.12-slim + dlt)
requirements-pipeline.txt   Dépendances de cette image : dlt + pydantic, rien d'autre
airflow.dockerfile          Image Airflow (apache/airflow:3.2.2 + dags + scripts)
values.yaml                 Values Helm du chart officiel airflow (KubernetesExecutor)
minio.yaml                  Manifests MinIO (PVC + Deployment + Service) pour le cluster
pod_templates/              Template de pod worker (inutilisé, voir État actuel)
.dlt/config.toml            Tuning dlt local uniquement (non embarqué dans les images)
```

> `logs/`, `config/`, `.venv` et `.dlt` sont dans le [.gitignore](.gitignore) : ce sont
> des résidus d'exécutions locales, aucun n'est nécessaire au déploiement.

---

## Prérequis

- **Docker**, pour construire et pousser les deux images
- **Kubernetes** avec un registry accessible sur `localhost:5000`, **kubectl** et **Helm**
- ~4 Go de RAM disponibles pour la stack Airflow

Tout passe par le cluster : les DAGs lancent des `KubernetesPodOperator` avec
`in_cluster=True`, il n'y a pas de mode local.

---

## Exécution sur Kubernetes

### 1. Construire et pousser les deux images

**Image d'exécution des pipelines** — `scripts/` + dlt, lancée dans un pod dédié par
`DltPipelineOperator`. Le tag se passe au DAG via le paramètre `image_tag`.

```bash
docker build -t localhost:5000/dlt-pipeline:1.0.0 -f dockerfile . && docker push localhost:5000/dlt-pipeline:1.0.0
```

**Image Airflow** — `dags/` + `scripts/`, déployée par le chart Helm. Le tag doit
correspondre à `images.airflow.tag` dans [values.yaml](values.yaml).

```bash
docker build -t localhost:5000/airflow-custom:1.1.0 -f airflow.dockerfile . && docker push localhost:5000/airflow-custom:1.1.0
```

### 2. Déployer MinIO

```bash
kubectl create namespace airflow
```

```bash
kubectl apply -f minio.yaml
```

Console MinIO : port-forward sur `9001` (`minioadmin` / `minioadmin`), API sur `9000`.
Créez-y le bucket attendu par le pipeline (`api-minio` avec la config par défaut de la V2).

### 3. Créer les secrets

**Credentials MinIO**, lus par le pod de pipeline. Les clés doivent porter **exactement**
le nom des variables d'environnement attendues (voir
[Secrets](#secrets-et-variables-denvironnement)).

```bash
kubectl create secret generic minio-credentials -n airflow --from-literal=MINIO_ENDPOINT=http://minio.airflow.svc.cluster.local:9000 --from-literal=MINIO_ACCESS_KEY=minioadmin --from-literal=MINIO_SECRET_KEY=minioadmin
```

**Connexion Airflow pour les logs distants.** `values.yaml` monte `airflow-s3-conn` dans
`AIRFLOW_CONN_AWS_DEFAULT` : sans ce secret, les pods Airflow restent bloqués en
`CreateContainerConfigError`. La valeur est une connexion Airflow sérialisée en JSON.

```bash
kubectl create secret generic airflow-s3-conn -n airflow --from-literal=connection='{"conn_type":"aws","login":"minioadmin","password":"minioadmin","extra":{"endpoint_url":"http://minio.airflow.svc.cluster.local:9000","region_name":"us-east-1"}}'
```

Créez aussi le bucket `airflow-logs` dans MinIO, cible de
`remote_base_log_folder`. Si vous ne voulez pas du logging distant, retirez le bloc
`config.logging` et `extraEnv` de [values.yaml](values.yaml) et passez
`logs.persistence.enabled: true`.

### 4. Installer Airflow via Helm

```bash
helm repo add apache-airflow https://airflow.apache.org
```

```bash
helm upgrade --install airflow apache-airflow/airflow -n airflow --version 1.22.0 -f values.yaml
```

Le chart 1.22.0 correspond à Airflow 3.2.2, la version de base de l'image custom : épinglez-le
pour éviter que le chart et l'image divergent.

`values.yaml` configure le `KubernetesExecutor`, l'image custom et le logging distant vers
`s3://airflow-logs/` via la connexion `aws_default`. Il renseigne `images.pod_template` en
plus de `images.airflow` : sans cela, le chart génère un pod template sur `apache/airflow`
standard et les pods de tâche du `KubernetesExecutor` tournent **sans les DAGs**.

### 5. Lancer un pipeline

```bash
kubectl port-forward -n airflow svc/airflow-api-server 8080:8080
```

Sur <http://localhost:8080>, dépausez `rest_api_to_minio_v2`, puis **Trigger DAG w/
config** et ajustez le JSON `pipeline_config`.

Le paramètre `image_tag` du DAG vaut `latest` par défaut : renseignez-y le tag de
l'image de pipeline réellement poussée (`1.0.0` ci-dessus) ou taguez aussi l'image en
`latest`.

---

## Configuration d'un pipeline

### V2 — un seul JSON

Le DAG V2 expose un unique paramètre `pipeline_config`, validé par
[`DltPipelineConfig`](scripts/pipeline_config.py) avant tout lancement de pod.
Exemple par défaut (PokéAPI → MinIO) :

```json
{
  "pipeline_name": "rest_api_to_minio",
  "dataset_name": "api_minio",
  "load_mode": "full",
  "source": {
    "kind": "rest_api",
    "base_url": "https://pokeapi.co/api/v2/",
    "resources": [
      {
        "name": "pokemon",
        "endpoint": { "path": "pokemon", "params": { "limit": 151 } },
        "write_disposition": "replace"
      }
    ]
  },
  "destination": {
    "kind": "filesystem_s3",
    "bucket_name": "api-minio"
  }
}
```

Le champ `kind` est un discriminant Pydantic. Sources et destinations déclarées :

| `source.kind` | Champs |
|---|---|
| `rest_api` | `base_url`, `resources`, `default_params`, `retry_attempts`, `retry_backoff`, `retry_max_delay` |
| `sql_database` | `schema_name`, `table_names`, `table_configs`, `credentials_env_prefix` (défaut `SRC_POSTGRES`) |

| `destination.kind` | Champs |
|---|---|
| `filesystem_s3` | `bucket_name`, `layout`, `file_format`, `credentials_env_prefix` (défaut `MINIO`) |
| `filesystem_azure` | `container_name`, `layout`, `file_format`, `credentials_env_prefix` (défaut `ADLS`) |
| `postgres` | `schema_name`, `credentials_env_prefix` (défaut `DST_POSTGRES`) |

Chaque entrée de `resources` est soit un nom de ressource, soit un objet dlt acceptant
`name`, `endpoint`, `primary_key`, `write_disposition`, `table_format`, `file_format`.

### Exécuter un pipeline hors Airflow

L'image de pipeline se lance seule, ce qui est pratique pour déboguer sans cluster :

```bash
docker run --rm -e PIPELINE_CONFIG="$(cat ma-config.json)" -e MINIO_ENDPOINT=http://minio:9000 -e MINIO_ACCESS_KEY=minioadmin -e MINIO_SECRET_KEY=minioadmin localhost:5000/dlt-pipeline:1.0.0
```

### Ajouter un nouveau pattern d'ingestion

1. Écrire `scripts/<mon_pattern>.py` qui appelle `load_config()` de
   [scripts/_common.py](scripts/_common.py) et exécute le pipeline dlt.
2. Si nécessaire, ajouter la source/destination dans
   [scripts/pipeline_config.py](scripts/pipeline_config.py) (modèle Pydantic +
   `required_env_vars`) et le mapping de secret dans `SECRET_NAME_BY_PREFIX` de
   [dags/dlt_pipeline_operator.py](dags/dlt_pipeline_operator.py).
3. Créer un DAG instanciant `DltPipelineOperator(script="<mon_pattern>.py", ...)`.

---

## Secrets et variables d'environnement

`DltPipelineOperator` déduit les secrets nécessaires de la config et les monte dans le pod
comme variables d'environnement. Le préfixe détermine le secret Kubernetes lu :

| Préfixe | Secret Kubernetes | Variables injectées |
|---|---|---|
| `MINIO` | `minio-credentials` | `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` |
| `ADLS` | `adls-credentials` | `ADLS_ACCOUNT_NAME`, `ADLS_ACCOUNT_KEY` |
| `SRC_POSTGRES` | `src-postgres-credentials` | `SRC_POSTGRES_HOST`, `_PORT`, `_USER`, `_PASSWORD`, `_DATABASE` |
| `DST_POSTGRES` | `dst-postgres-credentials` | `DST_POSTGRES_HOST`, `_PORT`, `_USER`, `_PASSWORD`, `_DATABASE` |

Autres variables :

- `PIPELINE_CONFIG` — JSON de configuration, injecté par l'opérateur
- `LOG_LEVEL` — niveau de log des scripts (défaut `INFO`)
- `DLT_DATA_DIR` — état local de dlt, fixé à `/tmp/dlt` dans l'image de pipeline

La configuration d'Airflow lui-même (identifiants de l'UI, Fernet key, connexion à la
base de métadonnées) est gérée par le chart Helm, pas par ce dépôt.

---

## État actuel du POC

Points à connaître avant de reprendre le projet :

- **Le layout par défaut écrase le chargement précédent.** `{table_name}` ne contient
  ni `{load_id}` ni `{file_id}` : chaque exécution réécrit le même objet
  `<dataset>/pokemon.parquet`. C'est cohérent avec `write_disposition: "replace"`, mais
  il n'y a aucun historique des chargements. Layout par défaut de dlt si vous voulez le
  conserver : `{table_name}/{load_id}.{file_id}.{ext}`.
- **Les versions de dlt ne sont pas figées** : `requirements-pipeline.txt` demande
  `dlt~=1.28`, et la version résolue change le nom des fichiers écrits (1.28 produit
  `pokemon` sans extension, 1.30 produit `pokemon.parquet`). Épinglez une version exacte
  si le nommage des objets compte pour vos consommateurs en aval.
- **Toutes les combinaisons du schéma ne sont pas implémentées** : `pipeline_config.py`
  décrit les sources `sql_database` et les destinations `filesystem_azure` / `postgres`,
  mais seul le couple `rest_api` → `filesystem_s3` est codé dans `api_to_minio.py`. Le
  script rejette explicitement les autres combinaisons avec un `ValueError` parlant.
- **`load_mode: "incremental"`** est accepté par le schéma mais n'a pas d'effet : le
  script journalise un `WARNING` et exécute un chargement complet.
- Le paramètre `image_tag` des DAGs vaut `latest` par défaut, alors que les images sont
  taguées par version : pensez à le renseigner au déclenchement.
- **[pod_templates/custom_pod.yaml](pod_templates/custom_pod.yaml) n'est branché nulle
  part** : `values.yaml` ne renseigne pas `podTemplate`, et le pod template est désormais
  généré par le chart depuis `images.pod_template`. Le fichier est redondant, à supprimer
  ou à câbler explicitement.
- Le dépôt ne contient pas de tests.
