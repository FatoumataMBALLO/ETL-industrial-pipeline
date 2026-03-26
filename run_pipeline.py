import sys
import os

# Force le chemin racine
sys.path.append(os.getcwd())

try:
    print("🔍 Vérification des imports...")
    from src.extract.extract_csv import extract_csv
    from src.extract.scrape_books import scrape_books
    from src.extract.extract_sql import extract_sql
    from src.extract.extract_api import extract_api
    from src.load.load_to_minio import upload_files
    # Pour Spark, on importe souvent la fonction main
    from docker.spark_transform import main as spark_transform
    print("✅ Tous les imports sont OK.")
except ImportError as e:
    print(f"❌ Erreur d'importation : {e}")
    print("👉 Vérifie que le nom de la fonction dans le fichier correspond bien au nom du fichier.")
    sys.exit(1)

def run():
    print("\n🚀 DÉMARRAGE DU PIPELINE INDUSTRIALISÉ\n" + "="*30)
    extract_csv()
    scrape_books()
    extract_sql()
    extract_api()
    upload_files()
    spark_transform()
    print("="*30 + "\n🏁 PIPELINE TERMINÉ AVEC SUCCÈS !")

if __name__ == "__main__":
    run()