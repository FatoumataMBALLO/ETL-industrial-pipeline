import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from src.utils.logger import get_logger

logger = get_logger("scrape_books")

URL = "https://books.toscrape.com/"

def scrape_books():
    try:
        logger.info("Début du scraping du site books.toscrape.com")

        response = requests.get(URL)
        soup = BeautifulSoup(response.text, "html.parser")

        books = []

        for book in soup.select(".product_pod"):
            title = book.h3.a["title"]
            price_text = book.select_one(".price_color").text
            price = price_text.replace("£", "").replace("Â", "").strip()
            price = float(price)

            books.append({
                "title": title,
                "price": float(price)
            })

        df = pd.DataFrame(books)

        os.makedirs("data/raw", exist_ok=True)

        filepath = "data/raw/books.csv"
        df.to_csv(filepath, index=False)

        logger.info(f"Données scrapées sauvegardées dans {filepath}")

    except Exception as e:
        logger.error(f"Erreur lors du scraping : {e}")


if __name__ == "__main__":
    scrape_books()