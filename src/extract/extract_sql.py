import sqlite3
import pandas as pd
from datetime import datetime
import os


def extract_sales_data():
    # Chemin base SQLite
    db_path = "data/sales.db"

    if not os.path.exists(db_path):
        print("Base SQLite introuvable.")
        return

    conn = sqlite3.connect(db_path)

    query = "SELECT * FROM sales"

    df = pd.read_sql_query(query, conn)

    conn.close()

    if df.empty:
        print("Aucune donnée trouvée dans la table sales.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/raw/sales_{timestamp}.csv"

    df.to_csv(output_path, index=False)

    print(f"Fichier généré : {output_path}")


if __name__ == "__main__":
    extract_sales_data()

