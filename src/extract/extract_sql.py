import os
import psycopg2

def extract_sql(target_path=None):
    # On récupère le mot de passe depuis les variables d'environnement Docker
    password = os.getenv('POSTGRES_PASSWORD', 'postgres') 
    
    conn = psycopg2.connect(
        host="etl-postgres",
        database="etl",
        user="postgres",
        password=password # Plus de mot de passe en dur !
    )
    # ... suite du code d'extraction