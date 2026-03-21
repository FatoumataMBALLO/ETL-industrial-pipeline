from minio import Minio
import os
from src.utils.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
from src.utils.logger import get_logger

logger = get_logger("load_to_minio")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def upload_files():
    folder = "data/raw"
    for file in os.listdir(folder):
        if file.endswith(".csv"):
            filepath = os.path.join(folder, file)
            logger.info(f"Uploading {file} to MinIO...")
            client.fput_object(MINIO_BUCKET, f"raw/{file}", filepath)
    logger.info("✅ Upload terminé")

if __name__ == "__main__":
    upload_files()
