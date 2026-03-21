import subprocess
import time
from minio import Minio
from dotenv import load_dotenv
import os

load_dotenv()

print("🚀 Starting ETL Pipeline")

# 1. Start Docker
print("🐳 Starting Docker containers...")
subprocess.run(
    ["docker", "compose", "-f", "docker/docker-compose.yml", "up", "-d"],
    check=True
)
time.sleep(5)

# 2. Initialisation MinIO
print("🪣 Initialisation MinIO...")
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
print("📥 Extract CSV...")
subprocess.run(["python", "-m", "src.extract.extract_csv"], check=True)

# 4. Scraping
print("🌐 Scraping books...")
subprocess.run(["python", "-m", "src.extract.scrape_books"], check=True)

# 5. Extract SQL
print("🗄️ Extract SQL...")
subprocess.run(["python", "-m", "src.extract.extract_sql"], check=True)

# 6. Extract API (non bloquant)
print("🔌 Extract API Open Library...")
result = subprocess.run(["python", "-m", "src.extract.extract_api"])
if result.returncode != 0:
    print("   ⚠️ API indisponible, on continue sans les données API.")
else:
    print("   ✅ Données API extraites.")

# 7. Load to MinIO
print("📤 Loading to MinIO...")
subprocess.run(["python", "-m", "src.load.load_to_minio"], check=True)

# 8. Spark Transform
print("⚙️ Running Spark job...")
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

print("✅ Pipeline finished successfully!")
