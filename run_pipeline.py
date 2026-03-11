import subprocess

print("🚀 Starting ETL Pipeline")

subprocess.run([
    "docker",
    "exec",
    "-e", "PYTHONPATH=/app",
    "spark-container",
    "/opt/spark/bin/spark-submit",
    "--packages",
    "org.postgresql:postgresql:42.7.3",
    "/app/src/transform/spark_transform.py"
])

print("✅ Pipeline finished")