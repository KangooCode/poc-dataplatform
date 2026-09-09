from __future__ import annotations

import os
from typing import Literal, Union

from pydantic import BaseModel, Field, field_validator


class RestApiSourceConfig(BaseModel):
    kind: Literal["rest_api"] = "rest_api"
    base_url: str
    resources: list[Union[str, dict]] = Field(min_length=1)
    default_params: dict = Field(default_factory=dict)
    retry_attempts: int = Field(default=5, ge=0)
    retry_backoff: float = Field(default=1.0, ge=0)
    retry_max_delay: float = Field(default=60.0, ge=0)

    @field_validator("base_url")
    @classmethod
    def _base_url_not_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("base_url must not be empty")
        return v


class SqlDatabaseSourceConfig(BaseModel):
    kind: Literal["sql_database"] = "sql_database"
    schema_name: str = "public"
    table_names: list[str] = Field(min_length=1)
    table_configs: dict[str, dict] = Field(default_factory=dict)
    credentials_env_prefix: str = "SRC_POSTGRES"  # -> SRC_POSTGRES_HOST, _PORT, _USER, _PASSWORD, _DATABASE


SourceConfig = Union[RestApiSourceConfig, SqlDatabaseSourceConfig]


class FilesystemS3DestinationConfig(BaseModel):
    kind: Literal["filesystem_s3"] = "filesystem_s3"
    bucket_name: str
    layout: str = "{table_name}"
    file_format: str = "parquet"
    credentials_env_prefix: str = "MINIO"  # -> MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

    @field_validator("bucket_name")
    @classmethod
    def _bucket_not_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("bucket_name must not be empty")
        return v


class FilesystemAzureDestinationConfig(BaseModel):
    kind: Literal["filesystem_azure"] = "filesystem_azure"
    container_name: str
    layout: str = "{table_name}"
    file_format: str = "parquet"
    credentials_env_prefix: str = "ADLS"  # -> ADLS_ACCOUNT_NAME, ADLS_ACCOUNT_KEY

    @field_validator("container_name")
    @classmethod
    def _container_not_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("container_name must not be empty")
        return v


class PostgresDestinationConfig(BaseModel):
    kind: Literal["postgres"] = "postgres"
    schema_name: str = "public"
    credentials_env_prefix: str = "DST_POSTGRES"  # -> DST_POSTGRES_HOST, _PORT, _USER, _PASSWORD, _DATABASE


DestinationConfig = Union[
    FilesystemS3DestinationConfig,
    FilesystemAzureDestinationConfig,
    PostgresDestinationConfig,
]

class DltPipelineConfig(BaseModel):
    pipeline_name: str
    dataset_name: str
    load_mode: Literal["full", "incremental"] = "full"
    max_table_nesting: int = Field(default=2, ge=0)
    source: SourceConfig = Field(discriminator="kind")
    destination: DestinationConfig = Field(discriminator="kind")

    @field_validator("pipeline_name", "dataset_name")
    @classmethod
    def _not_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("must not be empty")
        return v


def required_env_vars(config: DltPipelineConfig) -> list[str]:
    names: list[str] = []

    if isinstance(config.source, SqlDatabaseSourceConfig):
        p = config.source.credentials_env_prefix
        names += [f"{p}_HOST", f"{p}_PORT", f"{p}_USER", f"{p}_PASSWORD", f"{p}_DATABASE"]

    dest = config.destination
    if isinstance(dest, FilesystemS3DestinationConfig):
        p = dest.credentials_env_prefix
        names += [f"{p}_ENDPOINT", f"{p}_ACCESS_KEY", f"{p}_SECRET_KEY"]
    elif isinstance(dest, FilesystemAzureDestinationConfig):
        p = dest.credentials_env_prefix
        names += [f"{p}_ACCOUNT_NAME", f"{p}_ACCOUNT_KEY"]
    elif isinstance(dest, PostgresDestinationConfig):
        p = dest.credentials_env_prefix
        names += [f"{p}_HOST", f"{p}_PORT", f"{p}_USER", f"{p}_PASSWORD", f"{p}_DATABASE"]

    return names


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_postgres_credentials(prefix: str) -> dict:
    return {
        "host": _get_required_env(f"{prefix}_HOST"),
        "port": int(os.getenv(f"{prefix}_PORT", "5432")),
        "username": _get_required_env(f"{prefix}_USER"),
        "password": _get_required_env(f"{prefix}_PASSWORD"),
        "database": _get_required_env(f"{prefix}_DATABASE"),
    }


def get_s3_credentials(prefix: str) -> dict:
    return {
        "endpoint_url": _get_required_env(f"{prefix}_ENDPOINT"),
        "aws_access_key_id": _get_required_env(f"{prefix}_ACCESS_KEY"),
        "aws_secret_access_key": _get_required_env(f"{prefix}_SECRET_KEY"),
    }


def get_azure_credentials(prefix: str) -> dict:
    return {
        "azure_storage_account_name": _get_required_env(f"{prefix}_ACCOUNT_NAME"),
        "azure_storage_account_key": _get_required_env(f"{prefix}_ACCOUNT_KEY"),
    }
