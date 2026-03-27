import os
import sys

# On ajoute le chemin pour que Python trouve tes modules src
sys.path.append(os.getcwd())

from src.extract.scrape_books import scrape_books
# Importe tes autres sources si tu veux les tester aussi
# from src.extract.extract_sql import extract_sql

def run_manual_check():
    print("🧪 Démarrage du test manuel...")
    
    # Simulation du chemin de partitionnement demandé par le prof
    test_partition = "bronze/web/year=2026/month=03/day=26/"
    
    try:
        # Test du Scraping
        print(f"1. Test Scraping avec partition : {test_partition}")
        result_file = scrape_books(target_path=test_partition)
        
        if os.path.exists(result_file):
            print(f"✅ SUCCÈS : Fichier créé dans {result_file}")
        else:
            print("❌ ÉCHEC : Le fichier n'a pas été généré.")
            
    except Exception as e:
        print(f"💥 ERREUR CRITIQUE : {e}")

if __name__ == "__main__":
    run_manual_check()