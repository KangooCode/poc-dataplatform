from datetime import datetime
import os
import sys
import logging
import time

from airflow.decorators import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)

SCRIPTS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "scripts",
)

if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from pipeline_dlt import run_rest_api_pipeline


@dag(
    dag_id="rest_api_to_postgres",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "dest_conn_id": Param("postgres-db2", type="string"),
        "schema_name": Param("public", type="string"),

        # Configuration de l'API
        "pipeline_name": Param("pokeapi", type="string"),
        "base_url": Param("https://pokeapi.co/api/v2/", type="string"),
        "endpoint": Param("pokemon", type="string"),

        # Paramètres de l'endpoint
        "limit": Param(1000, type="integer"),
        "write_disposition": Param(
            "merge",
            enum=["replace", "append", "merge"],
        ),
        "primary_key": Param("name", type=["string", "null"]),
    },
    tags=["sync", "rest_api"],
)
def rest_api_to_postgres():

    @task
    def run(**context):
        start = time.perf_counter()

        p = context["params"]

        resources = [{
            "name": p["endpoint"],
            "write_disposition": p["write_disposition"],
        }]

        if p["primary_key"]:
            resources[0]["primary_key"] = p["primary_key"]

        run_rest_api_pipeline(
            dest_conn_id=p["dest_conn_id"],
            schema_name=p["schema_name"],
            pipeline_name=p["pipeline_name"],
            base_url=p["base_url"],
            default_params={
                "limit": p["limit"],
            },
            resources=resources,
        )

        logger.info(
            "Pipeline runtime: %.2fs",
            time.perf_counter() - start,
        )

    run()


dag = rest_api_to_postgres()