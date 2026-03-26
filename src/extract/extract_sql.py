import psycopg2
import csv
import os
from dotenv import load_dotenv

load_dotenv()

def extract_sql():
    """Fonction principale appelée par le DAG et run_pipeline"""
    print("📥 Extraction des données SQL en cours...")
    
    # Récupération des config
    host = os.getenv("POSTGRES_HOST", "localhost")
    db = os.getenv("POSTGRES_DB", "etl")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")

    try:
        conn = psycopg2.connect(host=host, database=db, user=user, password=password)
        cur = conn.cursor()
        
        # On vérifie si la table existe (à adapter selon ta table source)
        cur.execute("SELECT * FROM users LIMIT 100;") 
        rows = cur.fetchall()
        
        os.makedirs("data/raw", exist_ok=True)
        with open("data/raw/extract_sql.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([desc[0] for desc in cur.description])
            writer.writerows(rows)
            
        print("✅ Données extraites avec succès dans data/raw/extract_sql.csv")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Erreur lors de l'extraction SQL : {e}")

# Pour pouvoir tester le script seul
if __name__ == "__main__":
    extract_sql()