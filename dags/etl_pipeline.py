from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, "/opt/airflow")

from src.extract.extract_csv import extract_csv
from src.extract.scrape_books import scrape_books
from src.extract.extract_sql import extract_sales_data
from src.extract.extract_api import extract_books_api
from src.load.load_to_minio import upload_files

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL complet",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "spark", "minio"],
) as dag:

    t_extract_csv = PythonOperator(task_id="extract_csv", python_callable=extract_csv)
    t_scrape_books = PythonOperator(task_id="scrape_books", python_callable=scrape_books)
    t_extract_sql = PythonOperator(task_id="extract_sql", python_callable=extract_sales_data)
    t_extract_api = PythonOperator(task_id="extract_api", python_callable=extract_books_api)
    t_load_minio = PythonOperator(task_id="load_to_minio", python_callable=upload_files)

    t_spark = BashOperator(
        task_id="spark_transform",
        bash_command="""
            docker exec spark-container \
            /opt/spark/bin/spark-submit \
            --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            /app/src/transform/spark_transform.py
        """,
        retries=2,
    )

    [t_extract_csv, t_scrape_books, t_extract_sql, t_extract_api] >> t_load_minio >> t_spark
