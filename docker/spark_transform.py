import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, current_timestamp, lit

def main():
    print("🚀 Spark : Nettoyage final et structuration de la table de production...")
    
    spark = SparkSession.builder \
        .appName("ETL_Production_Pipeline") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    try:
        # 1. Lecture du fichier CSV
        path = "s3a://etl-data/raw/books.csv"
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(path)

        cols = df.columns
        print(f"📊 Colonnes détectées : {cols}")

        # 2. Transformation propre
        # On crée la base avec Titre, Prix et Date
        df_final = df.select(
            regexp_replace(trim(col(cols[0])), r"[\x00]", "").alias("book_title"),
            col(cols[1]).cast("double").alias("price_gbp")
        ).withColumn("extracted_at", current_timestamp())

        # Gestion dynamique de la colonne 'rating'
        if len(cols) > 2:
            df_final = df_final.withColumn("rating", col(cols[2]))
        else:
            # 'lit' crée une valeur texte par défaut si la colonne manque
            df_final = df_final.withColumn("rating", lit("N/A"))

        # 3. Écriture finale dans 'dim_books'
        jdbc_url = "jdbc:postgresql://etl-postgres:5432/etl"
        df_final.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "dim_books") \
            .option("user", "postgres") \
            .option("password", "postgres") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        print(f"✅ PRODUCTION TERMINEE : Table 'dim_books' est prête.")

    except Exception as e:
        print(f"❌ ERREUR : {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()