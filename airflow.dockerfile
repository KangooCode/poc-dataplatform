FROM apache/airflow:3.2.2

USER airflow

COPY --chown=airflow:root dags/ /opt/airflow/dags/

COPY --chown=airflow:root scripts/ /opt/airflow/scripts/

ENV PYTHONPATH=/opt/airflow/dags:/opt/airflow/scripts

WORKDIR /opt/airflow
