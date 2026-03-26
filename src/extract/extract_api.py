import requests
import json
import os

def extract_api():
    print("📥 Extraction des données API...")
    url = "https://jsonplaceholder.typicode.com/posts" # Exemple
    try:
        response = requests.get(url)
        data = response.json()
        
        os.makedirs("data/raw", exist_ok=True)
        with open("data/raw/extract_api.json", "w") as f:
            json.dump(data, f)
        print("✅ Extraction API terminée.")
    except Exception as e:
        print(f"❌ Erreur API : {e}")

if __name__ == "__main__":
    extract_api()