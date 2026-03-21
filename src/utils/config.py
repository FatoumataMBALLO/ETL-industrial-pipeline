from dotenv import load_dotenv
import os

load_dotenv()

MINIO_ENDPOINT    = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET      = os.getenv("MINIO_BUCKET", "etl-data")

POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "etl-postgres")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "etl")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_URL      = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

SPARK_MINIO_ENDPOINT = os.getenv("SPARK_MINIO_ENDPOINT", "http://minio:9000")
