import psycopg2
import pandas as pd
import os
from datetime import datetime
from src.utils.logger import get_logger

logger = get_logger("extract_sql")

DB_CONFIG = {
    "host": "localhost",
    "database": "etl_db",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

def extract_sales_data():
    try:
        logger.info("Connexion à la base PostgreSQL")

        conn = psycopg2.connect(**DB_CONFIG)
        query = "SELECT * FROM sales;"
        df = pd.read_sql(query, conn)

        conn.close()

        os.makedirs("data/raw", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"data/raw/sales_{timestamp}.csv"

        df.to_csv(filepath, index=False)

        logger.info(f"Données SQL sauvegardées dans {filepath}")

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction SQL : {e}")

if __name__ == "__main__":
    extract_sales_data()
