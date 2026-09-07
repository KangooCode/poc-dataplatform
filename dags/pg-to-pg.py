from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess

def runner():
    result = subprocess.run(
        ["python", "/opt/airflow/scripts/pipeline_dlt.py"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(result.stderr)

with DAG(
    dag_id="pg-to-pg",
    start_date=datetime(2026, 6, 25),
    schedule=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="pg-to-pg",
        python_callable=runner,
    )