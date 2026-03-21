import os
from unittest.mock import patch

def test_config_loads_env_variables():
    with patch.dict(os.environ, {
        "MINIO_ENDPOINT": "custom:9000",
        "MINIO_ACCESS_KEY": "mykey",
        "MINIO_SECRET_KEY": "mysecret",
        "MINIO_BUCKET": "my-bucket",
    }):
        import importlib
        import src.utils.config as config
        importlib.reload(config)
        assert config.MINIO_ENDPOINT == "custom:9000"
        assert config.MINIO_ACCESS_KEY == "mykey"
