import pytest
from unittest.mock import patch, MagicMock

def test_upload_files_uploads_csvs():
    mock_client = MagicMock()
    with patch("src.load.load_to_minio.client", mock_client), \
         patch("src.load.load_to_minio.logger"), \
         patch("os.listdir", return_value=["test.csv"]), \
         patch("os.path.join", return_value="/fake/path/test.csv"):
        from src.load.load_to_minio import upload_files
        upload_files()
        mock_client.fput_object.assert_called_once()

def test_upload_ignores_non_csv():
    mock_client = MagicMock()
    with patch("src.load.load_to_minio.client", mock_client), \
         patch("src.load.load_to_minio.logger"), \
         patch("os.listdir", return_value=["file.parquet", "file.json"]):
        from src.load.load_to_minio import upload_files
        upload_files()
        mock_client.fput_object.assert_not_called()
