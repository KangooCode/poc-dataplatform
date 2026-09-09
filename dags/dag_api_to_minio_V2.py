from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param

from dlt_pipeline_operator import DltPipelineOperator

DAG_ID = "rest_api_to_minio"

DEFAULT_CONFIG = {
    "pipeline_name": "rest_api_to_minio",
    "dataset_name": "api_minio",
    "load_mode": "full",
    "source": {
        "kind": "rest_api",
        "base_url": "https://pokeapi.co/api/v2/",
        "resources": [
            {
                "name": "pokemon",
                "endpoint": {"path": "pokemon", "params": {"limit": 151}},
                "write_disposition": "replace",
            }
        ],
    },
    "destination": {
        "kind": "filesystem_s3",
        "bucket_name": "api-minio",
    },
}

with DAG(
    dag_id=DAG_ID,
    description="Ingestion API -> MinIO via dlt (DltPipelineOperator + script dedie)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "data-eng", "retries": 1},
    tags=["dlt", "minio", "rest-api", "ingestion", "k8s"],
    params={
        "image_tag": Param(default="latest", type="string", title="Tag de l'image du pipeline"),
        "pipeline_config": Param(
            default=json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2),
            type="string",
            title="Configuration du pipeline (JSON)",
            description="Schema complet : voir scripts/pipeline_config.py (DltPipelineConfig). "
            "Doit correspondre au pattern implemente par scripts/rest_api_to_minio.py.",
        ),
    },
) as dag:

    run_pipeline = DltPipelineOperator(
        task_id="run_rest_api_to_minio_pipeline",
        script="api_to_minio.py",
        pipeline_config_json="{{ params.pipeline_config }}",
        image_tag="{{ params.image_tag }}",
    )
