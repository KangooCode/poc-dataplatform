FROM apache/airflow:3.2.2

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY entrypoint.py /opt/airflow/entrypoint.py

COPY scripts/ /opt/airflow/scripts

COPY dags/ /opt/airflow/dags/

ENV PYTHONPATH=/opt/airflow/dags:/opt/airflow/scripts

WORKDIR /opt/airflow