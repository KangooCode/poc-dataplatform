from __future__ import annotations

import json
import logging
import os
import sys

from api_to_minio import (
    DEFAULT_HTTP_BACKOFF_FACTOR,
    DEFAULT_HTTP_MAX_RETRY_DELAY,
    DEFAULT_HTTP_RETRY_ATTEMPTS,
    DEFAULT_LAYOUT,
    DEFAULT_MAX_TABLE_NESTING,
    run_rest_api_to_minio_pipeline,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("api_to_minio.entrypoint")


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _json_env(name: str, default: str) -> object:
    raw = os.getenv(name, default)
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in env var {name}: {raw!r}") from exc


def main() -> int:
    resources = _json_env("PIPELINE_RESOURCES", "[]")
    default_params = _json_env("PIPELINE_DEFAULT_PARAMS", "{}")

    logger.info(
        "Starting pipeline | base_url=%s | bucket=%s | dataset=%s | resources=%d",
        os.getenv("PIPELINE_BASE_URL"),
        os.getenv("PIPELINE_BUCKET_NAME"),
        os.getenv("PIPELINE_DATASET_NAME"),
        len(resources) if isinstance(resources, list) else 0,
    )

    run_rest_api_to_minio_pipeline(
        bucket_name=_required("PIPELINE_BUCKET_NAME"),
        dataset_name=_required("PIPELINE_DATASET_NAME"),
        pipeline_name=_required("PIPELINE_PIPELINE_NAME"),
        base_url=_required("PIPELINE_BASE_URL"),
        resources=resources,
        load_mode=os.getenv("PIPELINE_LOAD_MODE", "full"),
        default_params=default_params,
        layout=os.getenv("PIPELINE_LAYOUT", DEFAULT_LAYOUT),
        retry_attempts=int(os.getenv("PIPELINE_RETRY_ATTEMPTS", DEFAULT_HTTP_RETRY_ATTEMPTS)),
        retry_backoff=float(os.getenv("PIPELINE_RETRY_BACKOFF", DEFAULT_HTTP_BACKOFF_FACTOR)),
        retry_max_delay=float(
            os.getenv("PIPELINE_RETRY_MAX_DELAY", DEFAULT_HTTP_MAX_RETRY_DELAY)
        ),
        max_table_nesting=int(
            os.getenv("PIPELINE_MAX_TABLE_NESTING", DEFAULT_MAX_TABLE_NESTING)
        ),
    )

    logger.info("Pipeline finished successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())