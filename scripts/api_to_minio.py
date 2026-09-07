from __future__ import annotations

import logging
import os

import dlt
from dlt.sources.helpers.requests import Client as DltHttpClient
from dlt.sources.helpers.requests import Session as DltSession
from dlt.sources.rest_api import rest_api_source


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_HTTP_RETRY_ATTEMPTS = 5
DEFAULT_HTTP_BACKOFF_FACTOR = 1.0
DEFAULT_HTTP_MAX_RETRY_DELAY = 60.0

DEFAULT_MAX_TABLE_NESTING = 2
DEFAULT_LAYOUT = "{table_name}"

MINIO_ENDPOINT_ENV = "MINIO_ENDPOINT"
MINIO_ACCESS_KEY_ENV = "MINIO_ACCESS_KEY"
MINIO_SECRET_KEY_ENV = "MINIO_SECRET_KEY"


# ---------------------------------------------------------------------------
# MinIO credentials
# ---------------------------------------------------------------------------

def _get_minio_credentials() -> dict[str, str]:
    endpoint = os.getenv(MINIO_ENDPOINT_ENV)
    access_key = os.getenv(MINIO_ACCESS_KEY_ENV)
    secret_key = os.getenv(MINIO_SECRET_KEY_ENV)

    missing = [
        env_name
        for env_name, value in (
            (MINIO_ENDPOINT_ENV, endpoint),
            (MINIO_ACCESS_KEY_ENV, access_key),
            (MINIO_SECRET_KEY_ENV, secret_key),
        )
        if not value
    ]

    if missing:
        raise ValueError(
            "Missing required MinIO environment variables: "
            + ", ".join(missing)
        )

    return {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "endpoint_url": endpoint,
    }


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def http_session(
    *,
    retry_attempts: int = DEFAULT_HTTP_RETRY_ATTEMPTS,
    backoff_factor: float = DEFAULT_HTTP_BACKOFF_FACTOR,
    max_retry_delay: float = DEFAULT_HTTP_MAX_RETRY_DELAY,
) -> DltSession:

    if retry_attempts < 0:
        raise ValueError("retry_attempts must be >= 0")

    if backoff_factor < 0:
        raise ValueError("backoff_factor must be >= 0")

    if max_retry_delay < 0:
        raise ValueError("max_retry_delay must be >= 0")

    return DltHttpClient(
        request_max_attempts=retry_attempts,
        request_backoff_factor=backoff_factor,
        request_max_retry_delay=max_retry_delay,
        status_codes=(429, 500, 502, 503, 504),
        respect_retry_after_header=True,
    ).session


# ---------------------------------------------------------------------------
# REST API source
# ---------------------------------------------------------------------------

def run_rest_api_source(
    *,
    base_url: str,
    resources: list[dict | str],
    default_params: dict | None,
    session: DltSession,
    max_table_nesting: int,
):

    if not base_url:
        raise ValueError("base_url must not be empty")

    if not resources:
        raise ValueError("resources must contain at least one resource")

    if max_table_nesting < 0:
        raise ValueError("max_table_nesting must be >= 0")

    return rest_api_source(
        {
            "client": {
                "base_url": base_url,
                "session": session,
            },
            "resource_defaults": {
                "endpoint": {
                    "params": default_params or {},
                }
            },
            "resources": resources,
        },
        max_table_nesting=max_table_nesting,
    )


# ---------------------------------------------------------------------------
# MinIO destination
# ---------------------------------------------------------------------------

def _build_minio_destination(
    *,
    bucket_name: str,
    layout: str,
):
    if not bucket_name:
        raise ValueError("bucket_name must not be empty")

    if not layout:
        raise ValueError("layout must not be empty")

    credentials = _get_minio_credentials()

    return dlt.destinations.filesystem(
        bucket_url=f"s3://{bucket_name}",
        layout=layout,
        file_format="parquet",
        credentials=credentials,
    )


# ---------------------------------------------------------------------------
# Resource hints
# ---------------------------------------------------------------------------

def _apply_resource_hints(
    source,
    resources: list[dict | str],
) -> None:

    for resource_config in resources:

        if isinstance(resource_config, str):
            resource_name = resource_config
            config = {}
        else:
            config = resource_config
            resource_name = config.get("name")

        if not resource_name:
            raise ValueError(
                "Each resource must define a 'name'"
            )

        resource = source.resources.get(resource_name)

        if resource is None:
            raise ValueError(
                f"Resource '{resource_name}' was not found in dlt source"
            )

        hints: dict[str, object] = {}

        primary_key = config.get("primary_key")
        if primary_key:
            hints["primary_key"] = primary_key

        write_disposition = config.get("write_disposition")
        if write_disposition:
            hints["write_disposition"] = write_disposition

        table_format = config.get("table_format")
        if table_format:
            hints["table_format"] = table_format

        file_format = config.get("file_format")
        if file_format:
            hints["file_format"] = file_format

        if hints:
            resource.apply_hints(**hints)

        logger.debug(
            "Resource configured | resource=%s | hints=%s",
            resource_name,
            hints,
        )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_rest_api_to_minio_pipeline(
    *,
    bucket_name: str,
    dataset_name: str,
    pipeline_name: str,
    base_url: str,
    resources: list[dict | str],
    load_mode: str = "full",
    default_params: dict | None = None,
    layout: str = DEFAULT_LAYOUT,
    retry_attempts: int = DEFAULT_HTTP_RETRY_ATTEMPTS,
    retry_backoff: float = DEFAULT_HTTP_BACKOFF_FACTOR,
    retry_max_delay: float = DEFAULT_HTTP_MAX_RETRY_DELAY,
    max_table_nesting: int = DEFAULT_MAX_TABLE_NESTING,
):

    if not resources:
        raise ValueError(
            "resources must contain at least one resource"
        )

    logger.info(
        "API -> MinIO START | "
        "pipeline=%s | dataset=%s | base_url=%s | "
        "bucket=%s | mode=%s | resources=%d",
        pipeline_name,
        dataset_name,
        base_url,
        bucket_name,
        load_mode,
        len(resources),
    )

    session = http_session(
        retry_attempts=retry_attempts,
        backoff_factor=retry_backoff,
        max_retry_delay=retry_max_delay,
    )

    source = run_rest_api_source(
        base_url=base_url,
        resources=resources,
        default_params=default_params,
        session=session,
        max_table_nesting=max_table_nesting,
    )

    _apply_resource_hints(
        source=source,
        resources=resources,
    )

    destination = _build_minio_destination(
        bucket_name=bucket_name,
        layout=layout,
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
    )

    try:
        result = pipeline.run(source)

    except Exception:
        logger.exception(
            "API -> MinIO FAILED | "
            "pipeline=%s | dataset=%s | base_url=%s | mode=%s",
            pipeline_name,
            dataset_name,
            base_url,
            load_mode,
        )
        raise

    logger.info(
        "API -> MinIO SUCCESS | "
        "pipeline=%s | dataset=%s | mode=%s",
        pipeline_name,
        dataset_name,
        load_mode,
    )

    logger.debug(
        "DLT load info | pipeline=%s | load_info=%s",
        pipeline_name,
        result.asstr(verbosity=1),
    )

    return result