import logging
from datetime import datetime, timezone
from urllib.parse import quote_plus

import dlt
from dlt.sources import incremental
from dlt.sources.sql_database import sql_database
from dlt.sources.rest_api import check_connection, rest_api_source
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

# =========================================================
# AIRFLOW CONN → SQLALCHEMY URI (SOURCE)
# =========================================================
def _build_sqlalchemy_uri(conn_id: str) -> str:
    conn = BaseHook.get_connection(conn_id)

    if conn.conn_type != "postgres":
        raise ValueError(f"Only Postgres supported, got: {conn.conn_type}")

    login = conn.login or ""
    password = quote_plus(conn.password or "")
    host = conn.host or ""
    port = conn.port or 5432
    dbname = conn.schema

    return f"postgresql+psycopg2://{login}:{password}@{host}:{port}/{dbname}"


# =========================================================
# AIRFLOW CONN → DLT DESTINATION
# =========================================================
def build_dlt_postgres_credentials(conn_id: str) -> str:
    conn = BaseHook.get_connection(conn_id)

    login = conn.login or ""
    password = conn.password or ""
    host = conn.host or ""
    port = conn.port or 5432
    dbname = conn.schema

    return f"postgresql://{login}:{password}@{host}:{port}/{dbname}"


# =========================================================
# DLT SOURCE
# =========================================================
@dlt.source
def origine_source(
    conn_str: str,
    table_name: str,
    primary_key: str,
    backend: str = "pyarrow",          # "connectorx" possible
    chunk_size: int = 100_000,
    incremental_column: str | None = None,
):

    source = sql_database(
        conn_str,
        backend=backend,
        chunk_size=chunk_size,
    ).with_resources(table_name)

    hints = {
        "primary_key": primary_key,
    }

    if incremental_column:
        hints["incremental"] = incremental(incremental_column)

    source.resources[table_name].apply_hints(**hints)

    return source


# =========================================================
# REST API SOURCE (Pokemon)
# =========================================================
def load_rest_api(
    dest_credentials: str,
    schema_name: str,
    pipeline_name: str,
    base_url: str,
    resources: list[dict | str],
    default_params: dict | None = None,
):

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.postgres(
            credentials=dest_credentials
        ),
        dataset_name=schema_name,
    )

    source = rest_api_source(
        {
            "client": {
                "base_url": base_url,
            },
            "resource_defaults": {
                "endpoint": {
                    "params": default_params or {},
                },
            },
            "resources": resources,
        }
    )

    # Vérifie le premier endpoint déclaré
    resource_name = (
        resources[0]["name"]
        if isinstance(resources[0], dict)
        else resources[0]
    )

    can_connect, error_msg = check_connection(
        source,
        resource_name,
    )

    if not can_connect:
        raise ConnectionError(error_msg)

    logger.info("=== START LOAD %s ===", pipeline_name)

    load_info = pipeline.run(source,
                             write_disposition={
                                 "disposition": "merge",
                                 "strategy": "upsert",
                                 })

    logger.info(load_info.asstr(verbosity=2))

    logger.info("=== LOAD DONE ===")

    return load_info


# =========================================================
# UPSERT
# =========================================================
def run_upsert(
    source_uri: str,
    dest_credentials: str,
    table_name: str,
    schema_name: str,
    primary_key: str,
    backend: str = "pyarrow",
    chunk_size: int = 100_000,
    incremental_column: str | None = None,
):

    pipeline = dlt.pipeline(
        pipeline_name="pg-to-pg",
        destination=dlt.destinations.postgres(
            credentials=dest_credentials
        ),
        dataset_name=schema_name,
    )

    pipeline.drop()

    logger.info("=== START UPSERT %s ===", table_name)

    result = pipeline.run(
        origine_source(
            conn_str=source_uri,
            table_name=table_name,
            primary_key=primary_key,
            backend=backend,
            chunk_size=chunk_size,
            incremental_column=incremental_column,
        ),

        # Utilise COPY PostgreSQL
        loader_file_format="csv",

        # UPSERT natif PostgreSQL
        write_disposition={
            "disposition": "merge",
            "strategy": "upsert",
        },
    )

    logger.info(result.asstr(verbosity=2))

    return result


# =========================================================
# ORCHESTRATION
# =========================================================
def run_pipeline(
    source_conn_id: str,
    dest_conn_id: str,
    table_name: str,
    schema_name: str,
    primary_key: str,

    # Paramètres de performance
    backend: str = "pyarrow",          # ou "connectorx"
    chunk_size: int = 100_000,

    # Ex : updated_at ou id
    incremental_column: str | None = None,
):

    source_uri = _build_sqlalchemy_uri(source_conn_id)

    dest_credentials = build_dlt_postgres_credentials(dest_conn_id)

    logger.info("-------DEBUT EXECUTION-------\n"
                "Nom du projet: Test POC DLThub\n"
                "Nom du job: pipeline_bdd\n"
                "Description du job: Test POC DLThub avec un mouvement de donnée entre deux BDD\n" 
                f"Date Debut: {datetime.now(timezone.utc)}\n"
                "-------------------\n"
                "PARAMETRES UTILISES\n"
                "-------------------\n"
                "1) Source\n"
                f"- Serveur: {source_uri}\n"
                f"- Base de donnees: {source_conn_id}\n"
                f"- Schema: {schema_name}\n"
                f"- Table: {table_name}\n"
                "2) Cible\n"
                f"- Serveur: {dest_credentials}\n"
                f"- Base de donnees: {dest_conn_id}\n"
                f"- Schema: {schema_name}\n"
                f"- Table: {table_name}\n"
                "--------------------------\n"
                )

    logger.info(
        "PIPELINE START | table=%s | schema=%s | time=%s",
        table_name,
        schema_name,
        datetime.now(timezone.utc),
    )

    run_upsert(
        source_uri=source_uri,
        dest_credentials=dest_credentials,
        table_name=table_name,
        schema_name=schema_name,
        primary_key=primary_key,
        backend=backend,
        chunk_size=chunk_size,
        incremental_column=incremental_column,
    )

    logger.info("PIPELINE SUCCESS")


# =========================================================
# ORCHESTRATION (Pokemon API -> Postgres)
# =========================================================
def run_rest_api_pipeline(
    dest_conn_id: str,
    schema_name: str,
    pipeline_name: str,
    base_url: str,
    resources: list[dict | str],
    default_params: dict | None = None,
):

    dest_credentials = build_dlt_postgres_credentials(
        dest_conn_id
    )

    return load_rest_api(
        dest_credentials=dest_credentials,
        schema_name=schema_name,
        pipeline_name=pipeline_name,
        base_url=base_url,
        resources=resources,
        default_params=default_params,
    )