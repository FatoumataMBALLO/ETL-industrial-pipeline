import pytest
from unittest.mock import patch, MagicMock
import requests

MOCK_HTML = """
<html><body>
  <article class="product_pod">
    <h3><a title="A Great Book" href="#">A</a></h3>
    <p class="price_color">£12.99</p>
  </article>
</body></html>
"""

def test_scrape_books_parses_html():
    mock_resp = MagicMock()
    mock_resp.text = MOCK_HTML
    with patch("requests.get", return_value=mock_resp), \
         patch("pandas.DataFrame.to_csv"), \
         patch("os.makedirs"), \
         patch("src.extract.scrape_books.logger"):
        from src.extract.scrape_books import scrape_books
        scrape_books()

def test_scrape_books_handles_error():
    with patch("requests.get", side_effect=requests.exceptions.ConnectionError), \
         patch("src.extract.scrape_books.logger") as mock_logger:
        from src.extract.scrape_books import scrape_books
        scrape_books()
        mock_logger.error.assert_called_once()
