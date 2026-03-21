from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ETL Transform") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # ✅ Étape 1 : Lire le CSV local
    df = spark.read.csv("/app/data/input/data.csv", header=True, inferSchema=True)

    # ✅ Étape 2 : Transformation
    df = df.withColumn("total", df["price"] * df["quantity"])

    # ✅ Étape 3 : Écrire dans PostgreSQL
    url = "jdbc:postgresql://etl-postgres:5432/etl"
    properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(
        url=url,
        table="transactions",
        mode="overwrite",
        properties=properties
    )

    # ✅ Étape 4 : Lire les données raw locales et écrire en Parquet dans MinIO
    df_raw = spark.read.csv("/app/data/raw/", header=True, inferSchema=True)
    df_raw.write.mode("overwrite").parquet("s3a://etl-data/processed/")

    spark.stop()


if __name__ == "__main__":
    main()