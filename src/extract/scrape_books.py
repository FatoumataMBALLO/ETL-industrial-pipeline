import csv
import os
import requests
from bs4 import BeautifulSoup

def scrape_books():
    """
    Extrait les données et les place dans un dossier partagé par Docker.
    """
    print("🌐 Démarrage du scraping...")
    url = "https://books.toscrape.com/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    books = soup.find_all('article', class_='product_pod')

    # Chemin FIXE et PARTAGÉ (accessible par tous les containers)
    local_file = "/opt/airflow/books_data.csv"

    with open(local_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['title', 'price', 'rating']) 
        for book in books:
            title = book.h3.a['title']
            price = book.find('p', class_='price_color').text.replace('£', '')
            rating = book.p['class'][1]
            writer.writerow([title, price, rating])

    print(f"✅ Fichier sauvegardé physiquement : {local_file}")
    return local_file