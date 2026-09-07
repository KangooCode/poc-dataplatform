"""
DAG d'exemple : synchronisation Oracle (source) -> PostgreSQL (destination).

Nécessite les providers Airflow "apache-airflow-providers-oracle" et le
driver "oracledb" installés dans l'environnement.

Le conn_id source doit correspondre à une Airflow Connection de type
"oracle", et le conn_id destination à une Connection de type "postgres".
Attention : le sens inverse (Postgres -> Oracle) n'est pas supporté par
pipeline_dbt_link.py (voir la note en tête de ce script) et lèvera une
NotImplementedError si tenté.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
import os
import sys


SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)


from pipeline_dlt import run_pipeline


@dag(
    dag_id="sync_origine_oracle_to_postgres",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "source_conn_id": Param("oracle_db1", type="string", title="Conn ID source (oracle)"),
        "dest_conn_id": Param("postgres_db2", type="string", title="Conn ID destination (postgres)"),
        "table_name": Param("origine", type="string"),
        "schema_name": Param("public", type="string"),
        "primary_key": Param("id", type="string"),
    },
    tags=["sync", "oracle", "postgres"],
)
def sync_origine_oracle_to_postgres():
    @task
    def run(**context) -> None:
        p = context["params"]
        run_pipeline(
            source_conn_id=p["source_conn_id"],
            dest_conn_id=p["dest_conn_id"],
            table_name=p["table_name"],
            schema_name=p["schema_name"],
            primary_key=p["primary_key"],
        )

    run()


sync_origine_oracle_to_postgres()
