# ETL Industrial Pipeline

Pipeline ETL industriel complet : extraction multi-sources, transformation distribuée avec Apache Spark, et stockage dans une architecture data lakehouse.

## Architecture
cat > .env.example << 'EOF'
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=your_access_key
MINIO_SECRET_KEY=your_secret_key
MINIO_BUCKET=etl-data
POSTGRES_HOST=etl-postgres
POSTGRES_PORT=5432
POSTGRES_DB=etl
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
SPARK_MINIO_ENDPOINT=http://minio:9000
