from __future__ import annotations

import json
import logging
import os

from pydantic import ValidationError

from pipeline_config import DltPipelineConfig


def setup_logging() -> logging.Logger:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    return logging.getLogger("dlt_pipeline")


def load_config() -> DltPipelineConfig:
    config = os.getenv("PIPELINE_CONFIG")
    if not config:
        raise ValueError("Missing required environment variable: PIPELINE_CONFIG")

    try:
        payload = json.loads(config)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in env var PIPELINE_CONFIG: {config}") from exc

    try:
        return DltPipelineConfig.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid PIPELINE_CONFIG: {exc}") from exc
