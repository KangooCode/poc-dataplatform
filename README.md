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
- [Exécution locale (Docker Compose)](#exécution-locale-docker-compose)
- [Exécution sur Kubernetes](#exécution-sur-kubernetes)
- [Configuration d'un pipeline](#configuration-dun-pipeline)
- [Secrets et variables d'environnement](#secrets-et-variables-denvironnement)
- [État actuel du POC](#état-actuel-du-poc)

---

## Architecture

```
Airflow (scheduler / DAG)
      |
      |  DltPipelineOperator (hérite de KubernetesPodOperator)
      |   1. valide le JSON de config avec Pydantic
      |   2. déduit les secrets K8s nécessaires
      |   3. injecte PIPELINE_CONFIG dans l'env du pod
      v
Pod Kubernetes dédié  (image localhost:5000/airflow-custom:<tag>)
      |  python /opt/airflow/scripts/<script>.py
      v
Pipeline dlt  -->  Source (REST API, SQL)  -->  Destination (MinIO/S3, ADLS, Postgres)
```

Deux générations d'implémentation coexistent dans le dépôt :

| | **V1** | **V2** (cible) |
|---|---|---|
| DAG | [dags/dag_api_to_minio.py](dags/dag_api_to_minio.py) | [dags/dag_api_to_minio_V2.py](dags/dag_api_to_minio_V2.py) |
| `dag_id` | `rest_api_to_minio` | `rest_api_to_minio_v2` |
| Opérateur | `KubernetesPodOperator` brut | [`DltPipelineOperator`](dags/dlt_pipeline_operator.py) |
| Configuration | ~12 params Airflow → ~12 variables `PIPELINE_*` | 1 seul JSON → `PIPELINE_CONFIG` |
| Validation | aucune (échec dans le pod) | Pydantic dans le scheduler, **avant** de lancer le pod |
| Secrets | liste codée en dur dans le DAG | déduits de la config ([`required_env_vars`](scripts/pipeline_config.py)) |
| Entrypoint | [entrypoint.py](entrypoint.py) | `scripts/<script>.py` directement |

La V2 est la direction retenue (cf. commit `71214ef`) : ajouter un nouveau pattern
d'ingestion revient à écrire un script dans `scripts/` et un DAG de quelques lignes,
sans dupliquer la plomberie Kubernetes.

---

## Structure du dépôt

```
dags/
  dag_api_to_minio.py       DAG V1 : API REST -> MinIO (params à plat)
  dag_api_to_minio_V2.py    DAG V2 : API REST -> MinIO (config JSON unique)
  dlt_pipeline_operator.py  Opérateur réutilisable : valide la config, mappe les
                            secrets, lance le pod
scripts/
  api_to_minio.py           Logique dlt (REST API -> S3/MinIO) + entrypoint V2 :
                            lit PIPELINE_CONFIG et lance le pipeline
  pipeline_config.py        Schémas Pydantic (sources, destinations) + helpers credentials
  _common.py                setup_logging() + load_config() partagés par les scripts
entrypoint.py               Entrypoint de la V1 (lit les variables PIPELINE_*)
dockerfile                  Image custom : airflow 3.2.2 + requirements + dags + scripts
requirements.txt            dlt[postgres, filesystem, s3], providers Airflow, etc.
docker-compose.yaml         Stack Airflow locale (CeleryExecutor) + 3 Postgres + Redis
values.yaml                 Values Helm du chart officiel airflow (KubernetesExecutor)
minio.yaml                  Manifests MinIO (PVC + Deployment + Service) pour le cluster
pod_templates/              Template de pod worker pour l'executor Kubernetes
.dlt/config.toml            Tuning dlt (workers de normalize/load, taille des fichiers)
```

> `config/`, `logs/`, `.env`, `.venv` et `.dlt` sont dans le [.gitignore](.gitignore) :
> après un clone il faut les recréer (voir ci-dessous).

---

## Prérequis

- **Docker** (mode local)
- **Kubernetes** avec un registry local sur `localhost:5000`, **kubectl** et **Helm**
  (mode cible — c'est le seul mode où les DAGs s'exécutent réellement, voir plus bas)
- ~4 Go de RAM disponibles pour la stack Airflow

---

## Exécution locale (Docker Compose)

La stack Compose fournit l'UI et le scheduler Airflow, plus deux bases Postgres de test
(`postgres-db1`, `postgres-db2`) qui servent de source/cible pour les pipelines SQL.

### 1. Créer le fichier `.env`

```bash
printf 'AIRFLOW_UID=50000\nAIRFLOW_GID=0\n_AIRFLOW_WWW_USER_USERNAME=admin\n_AIRFLOW_WWW_USER_PASSWORD=admin\nFERNET_KEY=\n' > .env
```

`FERNET_KEY` peut rester vide pour un POC ; sinon générez-la :

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```


## Exécution sur Kubernetes

### 1. Construire et pousser l'image du pipeline

L'image contient `dags/`, `scripts/`, `entrypoint.py` et les dépendances dlt.
Le tag doit correspondre à celui référencé par les DAGs et par `values.yaml`.

```bash
docker build -t localhost:5000/airflow-custom:1.0.2 -f dockerfile .
```

```bash
docker push localhost:5000/airflow-custom:1.0.0
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

```bash
kubectl create secret generic minio-credentials -n airflow --from-literal=MINIO_ENDPOINT=http://minio.airflow.svc.cluster.local:9000 --from-literal=MINIO_ACCESS_KEY=minioadmin --from-literal=MINIO_SECRET_KEY=minioadmin
```

Les clés du secret doivent porter **exactement** le nom des variables d'environnement
attendues (voir [Secrets](#secrets-et-variables-denvironnement)).

### 4. Installer Airflow via Helm

```bash
helm repo add apache-airflow https://airflow.apache.org
```

```bash
helm upgrade --install airflow apache-airflow/airflow -n airflow -f values.yaml
```

`values.yaml` configure le `KubernetesExecutor`, l'image custom, et le logging distant
vers `s3://airflow-logs/` via la connexion `aws_default` (secret `airflow-s3-conn`).

### 5. Lancer un pipeline

Ouvrez l'UI Airflow, dépausez le DAG voulu (`rest_api_to_minio_v2` pour la V2,
`rest_api_to_minio` pour la V1), puis **Trigger DAG w/ config** et ajustez le JSON
`pipeline_config` (V2) ou les paramètres individuels (V1).

Le tag d'image par défaut des DAGs est `latest` : renseignez le paramètre `image_tag`
avec le tag réellement poussé (`1.0.2` ci-dessus) ou taguez aussi l'image en `latest`.

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

### V1 — paramètres à plat

Le DAG V1 expose chaque option comme un `Param` Airflow (`base_url`, `resources`,
`bucket_name`, `dataset_name`, `pipeline_name`, `load_mode`, `default_params`, `layout`,
`retry_*`, `max_table_nesting`), transmis au pod sous forme de variables `PIPELINE_*`
lues par [entrypoint.py](entrypoint.py).

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

- `PIPELINE_CONFIG` — JSON de configuration (V2), injecté par l'opérateur
- `PIPELINE_*` — options individuelles (V1), injectées par le DAG
- `LOG_LEVEL` — niveau de log des scripts (défaut `INFO`)

Côté Compose, `.env` fournit `AIRFLOW_UID`, `AIRFLOW_GID`, les identifiants de l'UI et
`FERNET_KEY`.

---

## État actuel du POC

Points à connaître avant de reprendre le projet :

- **Le layout par défaut ne produit pas d'extension de fichier.** `{table_name}` ne
  contient aucun des placeholders `{load_id}` / `{file_id}` / `{ext}` : dlt écrit donc un
  objet nommé `<dataset>/pokemon` (sans `.parquet`), réécrit à l'identique à chaque
  exécution. C'est cohérent avec `write_disposition: "replace"`, mais la plupart des
  lecteurs et des tables externes s'appuient sur l'extension. Layout par défaut de dlt si
  vous voulez l'historique et l'extension : `{table_name}/{load_id}.{file_id}.{ext}`.
- **Toutes les combinaisons du schéma ne sont pas implémentées** : `pipeline_config.py`
  décrit les sources `sql_database` et les destinations `filesystem_azure` / `postgres`,
  mais seul le couple `rest_api` → `filesystem_s3` est codé dans `api_to_minio.py`. Le
  script rejette explicitement les autres combinaisons avec un `ValueError` parlant.
- **`load_mode: "incremental"`** est accepté par le schéma mais n'a pas d'effet : le
  script journalise un `WARNING` et exécute un chargement complet.
- Les tags d'image ne sont pas alignés (`values.yaml` → `1.0.2`,
  `pod_templates/custom_pod.yaml` → `1.0.0`, DAGs → `latest` par défaut). Le `docker push`
  de la section Kubernetes pousse `1.0.0` alors que le `docker build` construit `1.0.2`.
- Le dépôt ne contient pas de tests.
