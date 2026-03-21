import pytest
import requests
from unittest.mock import patch, MagicMock

MOCK_RESPONSE = {
    "docs": [{
        "title": "Python 101",
        "author_name": ["John"],
        "first_publish_year": 2020,
        "isbn": ["123"],
        "publisher": ["O'Reilly"],
        "language": ["eng"],
        "edition_count": 3
    }]
}

def test_extract_api_returns_dataframe():
    mock_resp = MagicMock()
    mock_resp.json.return_value = MOCK_RESPONSE
    mock_resp.raise_for_status.return_value = None
    with patch("requests.get", return_value=mock_resp), \
         patch("pandas.DataFrame.to_csv"), \
         patch("os.makedirs"), \
         patch("src.extract.extract_api.logger"):
        from src.extract.extract_api import extract_books_api
        df = extract_books_api()
        assert len(df) == 1
        assert df.iloc[0]["title"] == "Python 101"

def test_extract_api_handles_network_error():
    with patch("requests.get", side_effect=requests.exceptions.ConnectionError), \
         patch("src.extract.extract_api.logger"):
        from src.extract.extract_api import extract_books_api
        with pytest.raises(requests.exceptions.RequestException):
            extract_books_api()
