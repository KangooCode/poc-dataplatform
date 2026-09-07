"""DLT pipelines callable from Airflow.

This module is intentionally independent from the billetique ingestion engine.
Airflow owns the business configuration and passes the table-level parameters to
these functions. DLT owns extraction, incremental state and loading.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

import dlt
from airflow.hooks.base import BaseHook
from dlt.pipeline.exceptions import PipelineStepFailed
from dlt.sources import incremental
from dlt.sources.helpers.requests import Client as DltHttpClient
from dlt.sources.helpers.requests import RequestException as DltRequestException
from dlt.sources.helpers.requests import Session as DltSession
from dlt.sources.rest_api import check_connection, rest_api_source
from dlt.sources.sql_database import sql_database, sql_table

logger = logging.getLogger(__name__)

# Exceptions considered transient: retrying has a chance of succeeding.
# Anything else (schema errors, bad credentials, bad config, 4xx client
# errors other than 429) is raised immediately since a retry would not help.
# DltRequestException covers connection errors, timeouts and HTTP errors
# raised by DLT's own HTTP client (dlt.sources.helpers.requests).
_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    OSError,
    DltRequestException,
    PipelineStepFailed,
)


# =========================================================
# AIRFLOW CONNECTIONS
# =========================================================
def _get_postgres_connection(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    if conn.conn_type != "postgres":
        raise ValueError(f"Only Postgres supported, got: {conn.conn_type}")
    return conn


def _build_sqlalchemy_uri(conn_id: str) -> str:
    """Build a source URI without exposing credentials in logs."""
    conn = _get_postgres_connection(conn_id)
    login = quote_plus(conn.login or "")
    password = quote_plus(conn.password or "")
    host = conn.host or ""
    port = conn.port or 5432
    database = quote_plus(conn.schema or "")
    return f"postgresql+psycopg2://{login}:{password}@{host}:{port}/{database}"


def build_dlt_postgres_credentials(conn_id: str) -> str:
    """Build credentials for a PostgreSQL DLT destination."""
    conn = _get_postgres_connection(conn_id)
    login = quote_plus(conn.login or "")
    password = quote_plus(conn.password or "")
    host = conn.host or ""
    port = conn.port or 5432
    database = quote_plus(conn.schema or "")
    return f"postgresql://{login}:{password}@{host}:{port}/{database}"


# =========================================================
# DESTINATIONS
# =========================================================
def build_adls_destination(
    bucket_url: str,
    layout: str = "{table_name}",
    deltalake_storage_options: dict[str, str] | None = None,
):
    """Build a filesystem destination for ADLS Gen2 or an Azurite-compatible URL.

    Storage credentials are resolved by dlt from its configured destination
    credentials/environment. The bucket URL is deliberately supplied by Airflow.
    """
    return dlt.destinations.filesystem(
        bucket_url=bucket_url,
        layout=layout,
        deltalake_storage_options=deltalake_storage_options
        or {"timeout": "60s", "max_retries": "3"},
    )


# =========================================================
# RETRY / BACKOFF HELPERS
# =========================================================
def _build_retrying_session(
    total_retries: int = 5,
    backoff_factor: float = 1.0,
    max_retry_delay: float = 60.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> DltSession:
    """Build an HTTP session using DLT's own retrying HTTP client.

    Wraps ``dlt.sources.helpers.requests.Client``, DLT's native ``Session``
    factory, which already retries on 429/5xx responses and on connection or
    timeout errors, applying exponential backoff (``backoff_factor *
    2**(attempt-1)``, capped at ``max_retry_delay``) and honouring the
    ``Retry-After`` header when present. No direct use of ``requests`` or
    ``urllib3`` here; this session is injected into the REST API client via
    ``client.session`` so a single flaky response does not fail the whole
    extraction.
    """
    return DltHttpClient(
        request_max_attempts=total_retries,
        request_backoff_factor=backoff_factor,
        request_max_retry_delay=max_retry_delay,
        status_codes=status_forcelist,
        respect_retry_after_header=True,
    ).session


def _run_pipeline_with_retry(
    pipeline: dlt.Pipeline,
    source,
    *,
    max_attempts: int = 3,
    base_delay: float = 5.0,
    max_delay: float = 60.0,
    retryable_exceptions: tuple[type[BaseException], ...] = _RETRYABLE_EXCEPTIONS,
    **run_kwargs: Any,
):
    """Run ``pipeline.run(source, **run_kwargs)`` with exponential backoff.

    Only ``retryable_exceptions`` trigger a retry; every other exception
    propagates immediately. Delay grows as ``base_delay * 2**(attempt-1)``,
    capped at ``max_delay``, with a small random jitter to avoid retry storms
    across concurrent Airflow tasks.
    """
    attempt = 1
    while True:
        try:
            return pipeline.run(source, **run_kwargs)
        except retryable_exceptions as exc:
            if attempt >= max_attempts:
                logger.error(
                    "PIPELINE RETRY EXHAUSTED | pipeline=%s | attempts=%s | error=%s",
                    pipeline.pipeline_name,
                    attempt,
                    exc,
                )
                raise
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay += random.uniform(0, delay * 0.1)
            logger.warning(
                "PIPELINE RETRY | pipeline=%s | attempt=%s/%s | retrying_in=%.1fs | error=%s",
                pipeline.pipeline_name,
                attempt,
                max_attempts,
                delay,
                exc,
            )
            time.sleep(delay)
            attempt += 1


# =========================================================
# SOURCES
# =========================================================
def origine_source(
    conn_str: str,
    table_name: str,
    source_schema: str,
    primary_key: str | None = None,
    backend: str = "pyarrow",
    chunk_size: int = 100_000,
    incremental_column: str | None = None,
    load_mode: str = "full",
):
    """Build one PostgreSQL resource for the requested Airflow task.

    ``full`` replaces a Parquet table. ``delta`` appends rows after the
    persisted DLT cursor into a Delta table. No anonymisation or business SQL is
    performed here.
    """
    normalized_mode = load_mode.lower()
    if normalized_mode not in {"full", "delta"}:
        raise ValueError("load_mode must be 'full' or 'delta'")
    if normalized_mode == "delta" and not incremental_column:
        raise ValueError("incremental_column is required when load_mode='delta'")

    resource_kwargs = {
        "credentials": conn_str,
        "table": table_name,
        "schema": source_schema,
        "reflection_level": "full_with_precision",
        "backend": backend,
        "chunk_size": chunk_size,
    }

    if normalized_mode == "delta":
        resource_kwargs.update(
            {
                "incremental": incremental(incremental_column),
                "write_disposition": "append",
            }
        )
    else:
        resource_kwargs["write_disposition"] = "replace"

    resource = sql_table(**resource_kwargs)
    hints: dict[str, object] = {}
    if primary_key:
        hints["primary_key"] = primary_key
    if normalized_mode == "delta":
        hints.update({"table_format": "delta", "write_disposition": "append"})
    else:
        hints["file_format"] = "parquet"
    resource.apply_hints(**hints)
    return resource


# =========================================================
# POSTGRES -> ADLS
# =========================================================
def run_postgres_to_adls(
    source_conn_id: str,
    table_name: str,
    source_schema: str,
    bucket_url: str,
    dataset_name: str,
    load_mode: str,
    primary_key: str | None = None,
    incremental_column: str | None = None,
    pipeline_name: str = "pg-to-adls",
    backend: str = "pyarrow",
    chunk_size: int = 100_000,
    layout: str = "{table_name}",
):
    """Load one Airflow-selected PostgreSQL table into ADLS."""
    source_uri = _build_sqlalchemy_uri(source_conn_id)
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=build_adls_destination(bucket_url=bucket_url, layout=layout),
        dataset_name=dataset_name,
    )
    source = origine_source(
        conn_str=source_uri,
        table_name=table_name,
        source_schema=source_schema,
        primary_key=primary_key,
        backend=backend,
        chunk_size=chunk_size,
        incremental_column=incremental_column,
        load_mode=load_mode,
    )

    logger.info(
        "POSTGRES TO ADLS START | source_conn=%s | schema=%s | table=%s | mode=%s",
        source_conn_id,
        source_schema,
        table_name,
        load_mode,
    )
    result = pipeline.run(source)
    logger.info("POSTGRES TO ADLS DONE | table=%s", table_name)
    logger.info(result.asstr(verbosity=2))
    return result


# =========================================================
# POSTGRES -> POSTGRES
# =========================================================
def run_upsert(
    source_uri: str,
    dest_credentials: str,
    table_name: str,
    source_schema: str,
    destination_schema: str,
    primary_key: str,
    backend: str = "pyarrow",
    chunk_size: int = 100_000,
    incremental_column: str | None = None,
):
    pipeline = dlt.pipeline(
        pipeline_name="pg-to-pg",
        destination=dlt.destinations.postgres(credentials=dest_credentials),
        dataset_name=destination_schema,
    )
    source = sql_database(
        source_uri,
        backend=backend,
        chunk_size=chunk_size,
    ).with_resources(table_name)
    hints: dict[str, object] = {"primary_key": primary_key}
    if incremental_column:
        hints["incremental"] = incremental(incremental_column)
    source.resources[table_name].apply_hints(**hints)

    logger.info("POSTGRES TO POSTGRES START | schema=%s | table=%s", source_schema, table_name)
    result = pipeline.run(
        source,
        loader_file_format="csv",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
    )
    logger.info("POSTGRES TO POSTGRES DONE | table=%s", table_name)
    logger.info(result.asstr(verbosity=2))
    return result


def run_pipeline(
    source_conn_id: str,
    dest_conn_id: str,
    table_name: str,
    schema_name: str,
    primary_key: str,
    backend: str = "pyarrow",
    chunk_size: int = 100_000,
    incremental_column: str | None = None,
):
    return run_upsert(
        source_uri=_build_sqlalchemy_uri(source_conn_id),
        dest_credentials=build_dlt_postgres_credentials(dest_conn_id),
        table_name=table_name,
        source_schema=schema_name,
        destination_schema=schema_name,
        primary_key=primary_key,
        backend=backend,
        chunk_size=chunk_size,
        incremental_column=incremental_column,
    )


# =========================================================
# REST API -> POSTGRES
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
        destination=dlt.destinations.postgres(credentials=dest_credentials),
        dataset_name=schema_name,
    )
    source = rest_api_source(
        {
            "client": {"base_url": base_url},
            "resource_defaults": {"endpoint": {"params": default_params or {}}},
            "resources": resources,
        }
    )
    resource_name = resources[0]["name"] if isinstance(resources[0], dict) else resources[0]
    can_connect, error_msg = check_connection(source, resource_name)
    if not can_connect:
        raise ConnectionError(error_msg)
    result = pipeline.run(
        source,
        write_disposition={"disposition": "merge", "strategy": "upsert"},
    )
    logger.info(result.asstr(verbosity=2))
    return result


def run_rest_api_pipeline(
    dest_conn_id: str,
    schema_name: str,
    pipeline_name: str,
    base_url: str,
    resources: list[dict | str],
    default_params: dict | None = None,
):
    return load_rest_api(
        dest_credentials=build_dlt_postgres_credentials(dest_conn_id),
        schema_name=schema_name,
        pipeline_name=pipeline_name,
        base_url=base_url,
        resources=resources,
        default_params=default_params,
    )


# =========================================================
# REST API -> ADLS
# =========================================================
def rest_api_to_adls_source(
    base_url: str,
    resources: list[dict | str],
    default_params: dict | None = None,
    session: DltSession | None = None,
):
    return rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "session": session or _build_retrying_session(),
            },
            "resource_defaults": {"endpoint": {"params": default_params or {}}},
            "resources": resources,
        }
    )


def load_rest_api_to_adls(
    bucket_url: str,
    dataset_name: str,
    pipeline_name: str,
    base_url: str,
    resources: list[dict | str],
    load_mode: str = "full",
    default_params: dict | None = None,
    primary_key: str | None = None,
    layout: str = "{table_name}",
    max_retry_attempts: int = 3,
    retry_base_delay: float = 5.0,
):
    """Load one or more REST API resources into ADLS (or an Azurite-compatible URL).

    ``full`` replaces a Parquet table per resource. ``delta`` appends rows
    into a Delta table, relying on each resource's own incremental/cursor
    configuration (set on ``resources`` before calling this function, the
    same way it is done for ``rest_api_source`` elsewhere in this module).

    Error handling:
      - Connection is verified with ``check_connection`` before any load is
        attempted; failures are raised as ``ConnectionError`` so Airflow can
        distinguish "source unreachable" from "load failed".
      - Transient HTTP errors (429/5xx, timeouts, dropped connections) are
        retried with exponential backoff at two levels: within each HTTP
        request (via the retrying session) and around the whole
        ``pipeline.run`` call (via ``_run_pipeline_with_retry``), in case a
        retry-exhausted request still bubbles up as a pipeline failure.
      - Non-transient errors (bad schema, bad config, auth failure) are not
        retried and propagate immediately.
    """
    normalized_mode = load_mode.lower()
    if normalized_mode not in {"full", "delta"}:
        raise ValueError("load_mode must be 'full' or 'delta'")
    if not resources:
        raise ValueError("resources must contain at least one resource")

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=build_adls_destination(bucket_url=bucket_url, layout=layout),
        dataset_name=dataset_name,
    )
    source = rest_api_to_adls_source(
        base_url=base_url,
        resources=resources,
        default_params=default_params,
    )

    resource_name = resources[0]["name"] if isinstance(resources[0], dict) else resources[0]
    try:
        can_connect, error_msg = check_connection(source, resource_name)
    except _RETRYABLE_EXCEPTIONS as exc:
        raise ConnectionError(f"Unable to reach {base_url}: {exc}") from exc
    if not can_connect:
        raise ConnectionError(error_msg)

    hints: dict[str, object] = {}
    if primary_key:
        hints["primary_key"] = primary_key
    if normalized_mode == "delta":
        hints.update({"table_format": "delta", "write_disposition": "append"})
    else:
        hints.update({"file_format": "parquet", "write_disposition": "replace"})
    for resource in source.resources.values():
        resource.apply_hints(**hints)

    logger.info(
        "API TO ADLS START | pipeline=%s | base_url=%s | dataset=%s | mode=%s",
        pipeline_name,
        base_url,
        dataset_name,
        load_mode,
    )
    try:
        result = _run_pipeline_with_retry(
            pipeline,
            source,
            max_attempts=max_retry_attempts,
            base_delay=retry_base_delay,
        )
    except Exception:
        logger.exception(
            "API TO ADLS FAILED | pipeline=%s | base_url=%s | dataset=%s",
            pipeline_name,
            base_url,
            dataset_name,
        )
        raise
    logger.info("API TO ADLS DONE | pipeline=%s", pipeline_name)
    logger.info(result.asstr(verbosity=2))
    return result


def run_rest_api_to_adls_pipeline(
    bucket_url: str,
    dataset_name: str,
    pipeline_name: str,
    base_url: str,
    resources: list[dict | str],
    load_mode: str = "full",
    default_params: dict | None = None,
    primary_key: str | None = None,
    layout: str = "{table_name}",
    max_retry_attempts: int = 3,
    retry_base_delay: float = 5.0,
):
    """Airflow-facing entrypoint for the API -> ADLS pattern.

    Airflow passes the table/dataset-level parameters (as with the other
    ``run_*`` entrypoints in this module); ADLS storage credentials are
    resolved by DLT from its configured destination credentials/environment,
    same as ``run_postgres_to_adls``.
    """
    return load_rest_api_to_adls(
        bucket_url=bucket_url,
        dataset_name=dataset_name,
        pipeline_name=pipeline_name,
        base_url=base_url,
        resources=resources,
        load_mode=load_mode,
        default_params=default_params,
        primary_key=primary_key,
        layout=layout,
        max_retry_attempts=max_retry_attempts,
        retry_base_delay=retry_base_delay,
    )