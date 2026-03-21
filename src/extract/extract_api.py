import requests
import pandas as pd
import os
from src.utils.logger import get_logger

logger = get_logger("extract_api")

API_URL = "https://openlibrary.org/search.json"

def extract_books_api(query: str = "python programming", limit: int = 50):
    try:
        logger.info(f"Appel API Open Library pour : '{query}'")

        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        session.mount("https://", adapter)

        response = session.get(
            API_URL,
            params={"q": query, "limit": limit},
            timeout=30  # augmenté à 30s
        )
        response.raise_for_status()
        data = response.json()

        books = []
        for doc in data.get("docs", []):
            books.append({
                "title":         doc.get("title", ""),
                "author":        ", ".join(doc.get("author_name", [])),
                "year":          doc.get("first_publish_year", None),
                "isbn":          doc.get("isbn", [None])[0],
                "publisher":     ", ".join(doc.get("publisher", [])[:2]),
                "language":      ", ".join(doc.get("language", [])),
                "edition_count": doc.get("edition_count", 0),
            })

        df = pd.DataFrame(books)
        os.makedirs("data/raw", exist_ok=True)
        output_path = "data/raw/api_books.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"{len(df)} livres extraits depuis l'API → {output_path}")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Erreur API : {e}")
        raise
    except Exception as e:
        logger.error(f"Erreur inattendue : {e}")
        raise

if __name__ == "__main__":
    extract_books_api()
