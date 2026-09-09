from __future__ import annotations

import json
from typing import Any

from kubernetes.client import models as k8s
from pydantic import ValidationError

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

from pipeline_config import DltPipelineConfig, required_env_vars


DEFAULT_IMAGE_REPOSITORY = "localhost:5000/dlt-pipeline"
DEFAULT_NAMESPACE = "airflow"
DEFAULT_SCRIPTS_DIR = "/app"
DEFAULT_CONTAINER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},
    limits={"cpu": "1", "memory": "1Gi"},
)

SECRET_NAME_BY_PREFIX = {
    "MINIO": "minio-credentials",
    "ADLS": "adls-credentials",
    "SRC_POSTGRES": "src-postgres-credentials",
    "DST_POSTGRES": "dst-postgres-credentials",
}


def _secrets_for_config(config: DltPipelineConfig) -> list[Secret]:
    env_names = required_env_vars(config)
    secrets: list[Secret] = []

    for env_name in env_names:
        prefix = next(
            (p for p in SECRET_NAME_BY_PREFIX if env_name.startswith(p + "_")),
            None,
        )
        if prefix is None:
            raise AirflowException(
                f"No Kubernetes secret mapping found for required env var '{env_name}'. "
                f"Update SECRET_NAME_BY_PREFIX in dlt_pipeline_operator.py."
            )
        secrets.append(
            Secret(
                deploy_type="env",
                deploy_target=env_name,
                secret=SECRET_NAME_BY_PREFIX[prefix],
                key=env_name,
            )
        )

    return secrets


class DltPipelineOperator(KubernetesPodOperator):

    template_fields = (*KubernetesPodOperator.template_fields, "pipeline_config_json")

    def __init__(
        self,
        *,
        script: str,
        pipeline_config_json: str,
        task_id: str,
        image_tag: str = "latest",
        image_repository: str = DEFAULT_IMAGE_REPOSITORY,
        namespace: str = DEFAULT_NAMESPACE,
        scripts_dir: str = DEFAULT_SCRIPTS_DIR,
        container_resources: k8s.V1ResourceRequirements | None = None,
        name: str | None = None,
        **kwargs: Any,
    ) -> None:
        if not script:
            raise ValueError("'script' must be set to the pattern-specific script filename")

        self.script = script
        self.pipeline_config_json = pipeline_config_json

        super().__init__(
            task_id=task_id,
            name=name or task_id.replace("_", "-"),
            namespace=namespace,
            image=f"{image_repository}:{image_tag}",
            image_pull_policy="IfNotPresent",
            cmds=["python", f"{scripts_dir}/{script}"],
            container_resources=container_resources or DEFAULT_CONTAINER_RESOURCES,
            get_logs=True,
            in_cluster=True,
            on_finish_action="delete_pod",
            startup_timeout_seconds=300,
            env_vars={},
            secrets=[],
            **kwargs,
        )

    def execute(self, context):
        try:
            payload = json.loads(self.pipeline_config_json)
            config = DltPipelineConfig.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            raise AirflowException(
                f"pipeline_config_json invalide, le pod ne sera pas lance : {exc}"
            ) from exc

        self.env_vars = [
            k8s.V1EnvVar(name="PIPELINE_CONFIG", value=json.dumps(payload))
        ]
        self.secrets = _secrets_for_config(config)

        return super().execute(context)
