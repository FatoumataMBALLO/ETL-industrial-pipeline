from minio import Minio
import os
from src.utils.config import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
from src.utils.logger import get_logger

logger = get_logger("load_to_minio")

# CORRECTION : On définit l'endpoint sur "minio:9000" pour le réseau Docker
MINIO_ENDPOINT_DOCKER = "minio:9000"

client = Minio(
    MINIO_ENDPOINT_DOCKER,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def upload_files():
    folder = "data/raw"
    # Créer le bucket s'il n'existe pas pour éviter une erreur
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        logger.info(f"Bucket {MINIO_BUCKET} créé.")

    if not os.path.exists(folder):
        logger.error(f"Le dossier {folder} n'existe pas.")
        return

    for file in os.listdir(folder):
        if file.endswith(".csv"):
            filepath = os.path.join(folder, file)
            logger.info(f"Uploading {file} to MinIO (Bronze Layer)...")
            # Les données brutes vont dans le dossier 'raw/' du bucket (Bronze)
            client.fput_object(MINIO_BUCKET, f"raw/{file}", filepath)
            
    logger.info("✅ Upload vers la Bronze Layer terminé")

if __name__ == "__main__":
    upload_files()