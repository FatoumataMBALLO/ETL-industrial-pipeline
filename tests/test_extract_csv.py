import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

def test_extract_csv_success():
    df = pd.DataFrame({"price": [10, 20], "quantity": [1, 2]})
    with patch("pandas.read_csv", return_value=df), \
         patch("pandas.DataFrame.to_csv"), \
         patch("os.makedirs"), \
         patch("src.extract.extract_csv.logger"):
        from src.extract.extract_csv import extract_csv
        extract_csv()

def test_extract_csv_handles_missing_file():
    with patch("pandas.read_csv", side_effect=FileNotFoundError), \
         patch("src.extract.extract_csv.logger") as mock_logger:
        from src.extract.extract_csv import extract_csv
        extract_csv()
        mock_logger.error.assert_called_once()
