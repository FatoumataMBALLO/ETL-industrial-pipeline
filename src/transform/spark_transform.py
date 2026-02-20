from pyspark.sql import SparkSession
from src.utils.logger import get_logger

logger = get_logger(__name__)

def main():
    spark = SparkSession.builder \
        .appName("ETL Transform") \
        .getOrCreate()

    input_path = "/app/data/input/data.csv"
    output_path = "/app/data/output"

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Exemple transformation
    df_clean = df.dropDuplicates()

    df_clean.write.mode("overwrite").parquet(output_path)

    logger.info("Transformation completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()