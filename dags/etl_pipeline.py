from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_sql = BashOperator(
        task_id="extract_sql",
        bash_command="cd /app && export PYTHONPATH=/app && python3 -m src.extract.extract_sql",
    )

    transform_spark = BashOperator(
        task_id="transform_spark",
        bash_command="cd /app && export PYTHONPATH=/app && python3 -m src.transform.spark_transform",
    )

    extract_sql >> transform_spark
