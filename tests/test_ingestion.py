import pytest
import sys
import os
from unittest.mock import MagicMock, patch

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../backend'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../data'))

# Mock models first
sys.modules['models'] = MagicMock()
from ingest_mtg import download_file, upload_to_s3

def test_download_file(tmp_path):
    # Mock requests.get
    with patch('requests.get') as mock_get:
        mock_get.return_value.__enter__.return_value.iter_content.return_value = [b'data']
        mock_get.return_value.__enter__.return_value.raise_for_status = MagicMock()
        
        dest = tmp_path / "test.mp3"
        assert download_file("http://example.com/test.mp3", dest) == True
        assert dest.exists()
        assert dest.read_bytes() == b'data'

def test_upload_to_s3():
    mock_s3 = MagicMock()
    # Mock environment variables for valid creds
    with patch.dict(os.environ, {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test", 
        "AWS_ENDPOINT_URL": "http://s3.local",
        "AWS_BUCKET_NAME": "test-bucket"
    }):
        url = upload_to_s3(mock_s3, "local.mp3", "remote.mp3")
        assert url == "http://s3.local/test-bucket/remote.mp3"
        mock_s3.upload_file.assert_called_with("local.mp3", "test-bucket", "remote.mp3")
