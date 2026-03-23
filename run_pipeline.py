import subprocess
import time
from minio import Minio
from dotenv import load_dotenv
import os
from src.utils.monitoring import track_time, print_report, metrics

load_dotenv()

print("🚀 Starting ETL Pipeline")

# 1. Start Docker
@track_time("Docker startup")
def start_docker():
    subprocess.run(
        ["docker", "compose", "-f", "docker/docker-compose.yml", "up", "-d"],
        check=True
    )
    time.sleep(5)

# 2. MinIO init
@track_time("MinIO init")
def init_minio():
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "password123"),
        secure=False
    )
    bucket_name = os.getenv("MINIO_BUCKET", "etl-data")
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"   ✅ Bucket '{bucket_name}' créé.")
    else:
        print(f"   ℹ️ Bucket '{bucket_name}' existe déjà.")

# 3. Extract CSV
@track_time("Extract CSV")
def extract_csv():
    subprocess.run(["python", "-m", "src.extract.extract_csv"], check=True)

# 4. Scraping
@track_time("Scrape books")
def scrape_books():
    subprocess.run(["python", "-m", "src.extract.scrape_books"], check=True)

# 5. Extract SQL
@track_time("Extract SQL")
def extract_sql():
    subprocess.run(["python", "-m", "src.extract.extract_sql"], check=True)

# 6. Extract API (non bloquant)
@track_time("Extract API")
def extract_api():
    result = subprocess.run(["python", "-m", "src.extract.extract_api"])
    if result.returncode != 0:
        print("   ⚠️ API indisponible, on continue sans les données API.")
        metrics["Extract API"]["status"] = "⚠️ skipped"

# 7. Load to MinIO
@track_time("Load to MinIO")
def load_minio():
    subprocess.run(["python", "-m", "src.load.load_to_minio"], check=True)

# 8. Spark Transform
@track_time("Spark transform")
def spark_transform():
    subprocess.run([
        "docker", "exec", "--user", "root", "spark-container",
        "mkdir", "-p", "/home/spark/.ivy2/cache", "/home/spark/.ivy2/jars"
    ], check=True)
    subprocess.run([
        "docker", "exec", "--user", "root", "spark-container",
        "chmod", "-R", "777", "/home/spark/.ivy2"
    ], check=True)
    subprocess.run([
        "docker", "exec",
        "-e", f"SPARK_MINIO_ENDPOINT={os.getenv('SPARK_MINIO_ENDPOINT', 'http://minio:9000')}",
        "-e", f"MINIO_ACCESS_KEY={os.getenv('MINIO_ACCESS_KEY', 'admin')}",
        "-e", f"MINIO_SECRET_KEY={os.getenv('MINIO_SECRET_KEY', 'password123')}",
        "-e", f"POSTGRES_HOST={os.getenv('POSTGRES_HOST', 'etl-postgres')}",
        "-e", f"POSTGRES_PORT={os.getenv('POSTGRES_PORT', '5432')}",
        "-e", f"POSTGRES_DB={os.getenv('POSTGRES_DB', 'etl')}",
        "-e", f"POSTGRES_USER={os.getenv('POSTGRES_USER', 'postgres')}",
        "-e", f"POSTGRES_PASSWORD={os.getenv('POSTGRES_PASSWORD', 'postgres')}",
        "-e", "PYTHONPATH=/app",
        "spark-container",
        "/opt/spark/bin/spark-submit",
        "--packages", "org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "/app/src/transform/spark_transform.py"
    ], check=True)

# Exécution
print("🐳 Starting Docker containers...")
start_docker()
print("🪣 Initialisation MinIO...")
init_minio()
print("📥 Extract CSV...")
extract_csv()
print("🌐 Scraping books...")
scrape_books()
print("🗄️ Extract SQL...")
extract_sql()
print("🔌 Extract API Open Library...")
extract_api()
print("📤 Loading to MinIO...")
load_minio()
print("⚙️ Running Spark job...")
spark_transform()

print_report()
print("✅ Pipeline finished successfully!")
