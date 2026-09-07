from __future__ import annotations

from datetime import datetime

from kubernetes.client import models as k8s

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

DAG_ID = "rest_api_to_minio"

IMAGE_REPOSITORY = "localhost:5000/airflow-custom"
NAMESPACE = "airflow"

MINIO_SECRETS = [
    Secret(deploy_type="env", deploy_target="MINIO_ENDPOINT", secret="minio-credentials", key="MINIO_ENDPOINT"),
    Secret(deploy_type="env", deploy_target="MINIO_ACCESS_KEY", secret="minio-credentials", key="MINIO_ACCESS_KEY"),
    Secret(deploy_type="env", deploy_target="MINIO_SECRET_KEY", secret="minio-credentials", key="MINIO_SECRET_KEY"),
]

default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id=DAG_ID,
    description="Ingestion API -> MinIO via dlt, exécutée dans un pod dédié (KubernetesPodOperator)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dlt", "minio", "rest-api", "ingestion", "k8s"],
    params={
        "image_tag": Param(default="latest", type="string", title="Tag de l'image du pipeline"),
        "base_url": Param(
            default="https://api.example.com",
            type="string",
            title="URL de base de l'API",
        ),
        "resources": Param(
            default='["my_resource"]',
            type="string",
            title="Resources (JSON)",
            description=(
                "Liste JSON des ressources à ingérer : noms simples ou objets "
                'de config (ex: [{"name": "users", "primary_key": "id", '
                '"write_disposition": "merge"}])'
            ),
        ),
        "bucket_name": Param(default="my-bucket", type="string", title="Bucket MinIO"),
        "dataset_name": Param(default="my_dataset", type="string", title="Nom du dataset dlt"),
        "pipeline_name": Param(
            default="rest_api_to_minio",
            type="string",
            title="Nom du pipeline dlt (identifiant d'état/checkpoint)",
        ),
        "load_mode": Param(
            default="full",
            type="string",
            enum=["full", "incremental"],
            title="Mode de chargement",
        ),
        "default_params": Param(
            default="{}",
            type="string",
            title="Default params (JSON)",
            description="Paramètres HTTP appliqués par défaut à toutes les resources",
        ),
        "layout": Param(default="{table_name}", type="string", title="Layout des fichiers Parquet"),
        "retry_attempts": Param(default=5, type="integer", minimum=0, title="Nombre de tentatives HTTP"),
        "retry_backoff": Param(default=1.0, type="number", minimum=0, title="Facteur de backoff"),
        "retry_max_delay": Param(
            default=60.0, type="number", minimum=0, title="Délai max entre tentatives (s)"
        ),
        "max_table_nesting": Param(
            default=2, type="integer", minimum=0, title="Nesting max des tables imbriquées"
        ),
    },
) as dag:

    run_pipeline = KubernetesPodOperator(
        task_id="run_rest_api_to_minio_pipeline",
        name="rest-api-to-minio",
        namespace=NAMESPACE,
        image=IMAGE_REPOSITORY + ":{{ params.image_tag }}",
        image_pull_policy="IfNotPresent",
        cmds=["python", "/opt/airflow/entrypoint.py"],
        env_vars={
            "PIPELINE_BASE_URL": "{{ params.base_url }}",
            "PIPELINE_RESOURCES": "{{ params.resources }}",
            "PIPELINE_BUCKET_NAME": "{{ params.bucket_name }}",
            "PIPELINE_DATASET_NAME": "{{ params.dataset_name }}",
            "PIPELINE_PIPELINE_NAME": "{{ params.pipeline_name }}",
            "PIPELINE_LOAD_MODE": "{{ params.load_mode }}",
            "PIPELINE_DEFAULT_PARAMS": "{{ params.default_params }}",
            "PIPELINE_LAYOUT": "{{ params.layout }}",
            "PIPELINE_RETRY_ATTEMPTS": "{{ params.retry_attempts }}",
            "PIPELINE_RETRY_BACKOFF": "{{ params.retry_backoff }}",
            "PIPELINE_RETRY_MAX_DELAY": "{{ params.retry_max_delay }}",
            "PIPELINE_MAX_TABLE_NESTING": "{{ params.max_table_nesting }}",
        },
        secrets=MINIO_SECRETS,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_pod",
        startup_timeout_seconds=300,
    )