from minio import Minio
from datetime import datetime
import os

# On ajoute **kwargs pour accepter les arguments envoyés par Airflow
def upload_files(local_file_path, source_name, **kwargs):
    # Connexion avec tes identifiants validés
    client = Minio(
        "minio:9000", 
        access_key="admin", 
        secret_key="password123", 
        secure=False
    )
    
    bucket_name = "etl-data"

    # Sécurité : Création du bucket s'il n'existe pas
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Construction du chemin partitionné
    now = datetime.now()
    remote_path = f"bronze/{source_name}/year={now.year}/month={now.month:02d}/day={now.day:02d}/data.csv"

    # Vérification du fichier local
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Le fichier {local_file_path} est introuvable.")

    # Upload
    client.fput_object(bucket_name, remote_path, local_file_path)
    print(f"🚀 Upload réussi : {remote_path}")