import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# --- CONFIGURATION DES CHEMINS (FORCE) ---
# Dans Docker, le dossier 'docker' est dans /opt/airflow/docker
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/docker')

# --- IMPORTS DES SCRIPTS ---
try:
    from src.extract.extract_csv import extract_csv
    from src.extract.scrape_books import scrape_books
    from src.extract.extract_sql import extract_sql
    from src.extract.extract_api import extract_api
    from src.load.load_to_minio import upload_files
    
    # On importe le fichier spark_transform.py qui est dans le dossier docker
    import spark_transform
    # On récupère la fonction main
    spark_main = spark_transform.main
    
    print("✅ Configuration des imports terminée avec succès.")
except Exception as e:
    print(f"❌ Erreur d'importation : {e}")
    # Fonction de secours pour éviter que le DAG ne disparaisse
    def spark_main():
        print("Erreur : le script spark_transform n'a pas pu être chargé.")

# --- ARGUMENTS PAR DÉFAUT ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DÉFINITION DU DAG ---
with DAG(
    'etl_industrial_pipeline_v2',
    default_args=default_args,
    description='Pipeline ETL Industriel complet',
    schedule_interval=None,
    catchup=False,
    tags=['production']
) as dag:

    t1 = PythonOperator(task_id='extract_csv', python_callable=extract_csv)
    t2 = PythonOperator(task_id='scrape_books', python_callable=scrape_books)
    t3 = PythonOperator(task_id='extract_sql', python_callable=extract_sql)
    t4 = PythonOperator(task_id='extract_api', python_callable=extract_api)
    t5 = PythonOperator(task_id='load_to_minio', python_callable=upload_files)

    # Utilisation de la fonction récupérée plus haut
    t6 = PythonOperator(task_id='spark_transform', python_callable=spark_main)

    [t1, t2, t3, t4] >> t5 >> t6