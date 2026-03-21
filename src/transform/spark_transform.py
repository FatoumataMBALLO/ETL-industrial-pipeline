from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("spark_transform")

MINIO_ENDPOINT    = os.getenv("SPARK_MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY", "password123")
POSTGRES_HOST     = os.getenv("POSTGRES_HOST", "etl-postgres")
POSTGRES_PORT     = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB       = os.getenv("POSTGRES_DB", "etl")
POSTGRES_USER     = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_URL      = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def get_spark():
    return SparkSession.builder \
        .appName("ETL Transform") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def transform_sales(spark):
    logger.info("Transformation des données sales...")
    df = spark.read.csv("/app/data/input/data.csv", header=True, inferSchema=True)
    df = df.withColumn("total", col("price") * col("quantity"))
    if "date" in df.columns:
        df = df.withColumn("year", year(col("date"))) \
               .withColumn("month", month(col("date")))
        partition_cols = ["year", "month"]
    else:
        partition_cols = []

    df.write.jdbc(
        url=POSTGRES_URL,
        table="transactions",
        mode="overwrite",
        properties={"user": POSTGRES_USER, "password": POSTGRES_PASSWORD, "driver": "org.postgresql.Driver"}
    )
    logger.info("✅ Sales écrits dans PostgreSQL")

    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet("s3a://etl-data/processed/sales/")
    logger.info("✅ Sales écrits dans MinIO")

def transform_books(spark):
    logger.info("Transformation des données books...")
    df = spark.read.csv("/app/data/raw/books.csv", header=True, inferSchema=True)
    df.write.mode("overwrite").parquet("s3a://etl-data/processed/books/")
    logger.info("✅ Books écrits dans MinIO")

def transform_api_books(spark):
    import os
    if not os.path.exists("/app/data/raw/api_books.csv"):
        logger.warning("api_books.csv absent, étape ignorée.")
        return
    logger.info("Transformation des données API books...")
    df = spark.read.csv("/app/data/raw/api_books.csv", header=True, inferSchema=True)
    df.write.mode("overwrite").parquet("s3a://etl-data/processed/api_books/")
    logger.info("✅ API books écrits dans MinIO")

def main():
    spark = get_spark()
    try:
        transform_sales(spark)
        transform_books(spark)
        transform_api_books(spark)
        logger.info("🎉 Toutes les transformations terminées avec succès !")
    except Exception as e:
        logger.error(f"Erreur lors de la transformation : {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
# (déjà géré dans main(), mais on sécurise transform_api_books)
