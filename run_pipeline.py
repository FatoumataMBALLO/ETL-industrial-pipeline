import subprocess

print("🚀 Starting ETL Pipeline")

subprocess.run([
    "docker",
    "exec",
    "-e", "PYTHONPATH=/app",
    "spark-container",
    "/opt/spark/bin/spark-submit",
    "/app/src/transform/spark_transform.py"
])

print("✅ Pipeline finished")