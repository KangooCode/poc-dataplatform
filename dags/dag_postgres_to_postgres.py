from datetime import datetime
import os
import sys
import logging
import time

from airflow.decorators import dag, task
from airflow.models.param import Param

logger = logging.getLogger(__name__)

SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from pipeline_dlt import run_pipeline


@dag(
    dag_id="origine_postgres_to_postgres",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "source_conn_id": Param("postgres-db1", type="string"),
        "dest_conn_id": Param("postgres-db2", type="string"),
        "table_name": Param("origine", type="string"),
        "schema_name": Param("public", type="string"),
        "primary_key": Param("id", type="string"),
    },
    tags=["sync", "postgres"],
)
def sync_origine_postgres_to_postgres():

    @task
    def run(**context):
        start = time.perf_counter()

        p = context["params"]

        run_pipeline(
            source_conn_id=p["source_conn_id"],
            dest_conn_id=p["dest_conn_id"],
            table_name=p["table_name"],
            schema_name=p["schema_name"],
            primary_key=p["primary_key"],
        )

        logger.info("Pipeline runtime: %.2fs", time.perf_counter() - start)

    run()


dag = sync_origine_postgres_to_postgres()