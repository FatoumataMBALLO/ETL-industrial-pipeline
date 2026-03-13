df = spark.read.csv("data/input/data.csv", header=True, inferSchema=True)
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
    df = spark.read.csv(
    "s3a://etl-data/raw/data.csv",
    header=True,
    inferSchema=True
)