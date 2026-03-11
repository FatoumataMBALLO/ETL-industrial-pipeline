from pyspark.sql import SparkSession


def main():

    spark = SparkSession.builder \
        .appName("ETL Transform") \
        .getOrCreate()

    input_path = "/app/data/input/data.csv"

    # Lire le CSV
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Transformation simple
    df = df.withColumn("total", df.price * df.quantity)

    # Configuration PostgreSQL
    url = "jdbc:postgresql://etl-postgres:5432/etl"

    properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Écrire dans PostgreSQL
    df.write.jdbc(
        url=url,
        table="transactions",
        mode="overwrite",
        properties=properties
    )

    spark.stop()


if __name__ == "__main__":
    main()