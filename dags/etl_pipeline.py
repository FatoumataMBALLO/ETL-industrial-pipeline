import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Configuration des chemins pour Airflow
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/src')

# 2. Fonctions de "Wrapper" pour éviter les erreurs d'import au chargement
def run_scrape():
    from extract.scrape_books import scrape_books
    return scrape_books()

def run_load(**kwargs):
    from load.load_to_minio import upload_files
    return upload_files(
        local_file_path="/opt/airflow/books_data.csv",
        source_name='web',
        **kwargs
    )

def run_spark_logic(**kwargs):
    # Import local à l'intérieur de la fonction pour ne pas bloquer le DAG
    from transform.spark_transform import main
    return main()

# 3. Définition du DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 3, 27),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# ... (le début du fichier reste identique)

with DAG(
    'etl_industrial_pipeline_v3',
    default_args=default_args,
    schedule=None,  # <-- C'est ici qu'on corrige !
    catchup=False
) as dag:

# ... (le reste reste identique)

    t_scrape = PythonOperator(
        task_id='scrape_books',
        python_callable=run_scrape
    )

    t_load = PythonOperator(
        task_id='load_to_minio',
        python_callable=run_load
    )

    t_spark = PythonOperator(
        task_id='spark_transform',
        python_callable=run_spark_logic
    )

    t_scrape >> t_load >> t_spark