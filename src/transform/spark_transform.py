import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, current_timestamp, lit

def main():
    print("🚀 Spark : Nettoyage (Silver) et Enrichissement (Gold)...")
    
    spark = SparkSession.builder \
        .appName("ETL_Production_Pipeline") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    try:
        # --- SOURCE 1 & 2 : Lecture depuis la couche BRONZE (MinIO) ---
        now = datetime.now()
        path = f"s3a://etl-data/bronze/web/year={now.year}/month={now.month:02d}/day={now.day:02d}/data.csv"
        
        print(f"📖 Lecture Bronze : {path}")
        df_bronze = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)
        cols = df_bronze.columns

        # --- SOURCE 3 : Lecture depuis POSTGRES (Table de référence) ---
        jdbc_url = "jdbc:postgresql://etl-postgres:5432/etl"
        df_ref = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "ref_categories") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # 2. Transformation et Nettoyage
        df_clean = df_bronze.select(
            regexp_replace(trim(col(cols[0])), r"[\x00]", "").alias("book_title"),
            regexp_replace(col(cols[1]), r"[^0-9.]", "").cast("double").alias("price_gbp"),
            (col(cols[2]) if len(cols) > 2 else lit("N/A")).alias("rating")
        ).withColumn("extracted_at", current_timestamp())

        # 3. Jointure (Enrichissement)
        df_final = df_clean.join(df_ref, lit(True), "left")

        # 4. Écriture SILVER PARTITIONNÉE
        silver_path = "s3a://etl-data/silver/books_partitioned"
        df_final.write \
            .partitionBy("rating") \
            .mode("overwrite") \
            .parquet(silver_path)
        
        print("🥈 Couche SILVER partitionnée créée dans MinIO")

        # 5. Écriture finale dans Postgres
        df_final.write.format("jdbc") \
            .option("url", jdbc_url).option("dbtable", "dim_books") \
            .option("user", "postgres").option("password", "postgres") \
            .option("driver", "org.postgresql.Driver").mode("overwrite").save()
        
        print("✅ PIPELINE TERMINE : 3 sources intégrées.")

    except Exception as e:
        print(f"❌ ERREUR : {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()