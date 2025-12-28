"""Unit tests for ParquetDownloader service."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import requests

from src.services.parquet_downloader import ParquetDownloader, download_parquet


@pytest.fixture
def mock_response():
    """Mock successful HTTP response."""
    response = Mock()
    response.status_code = 200
    response.headers = {"Content-Type": "application/octet-stream"}
    response.iter_content = Mock(return_value=[b"PAR1", b"fake", b"data"])
    return response


@pytest.fixture
def downloader(tmp_path):
    """ParquetDownloader instance with temp directory."""
    return ParquetDownloader(output_dir=str(tmp_path))


class TestParquetDownloader:
    """Test ParquetDownloader class."""
    
    def test_init_creates_output_dir(self, tmp_path):
        """Test initialization creates output directory."""
        output_dir = tmp_path / "downloads"
        downloader = ParquetDownloader(output_dir=str(output_dir))
        
        assert output_dir.exists()
        assert downloader.output_dir == output_dir
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_success(self, mock_get, downloader, mock_response, tmp_path):
        """Test successful file download."""
        mock_get.return_value = mock_response
        url = "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-e1a/PT/SPO-PT02022_00008_100.parquet"
        
        filepath = downloader.download(url)
        
        # Verify request
        mock_get.assert_called_once_with(
            url,
            stream=True,
            timeout=300,
            headers={"User-Agent": "DiscoMap/1.0"},
        )
        
        # Verify file created
        assert filepath.exists()
        assert filepath.name == "SPO-PT02022_00008_100.parquet"
        assert filepath.parent == tmp_path
        
        # Verify content written
        content = filepath.read_bytes()
        assert content == b"PAR1fakedata"
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_with_custom_filename(self, mock_get, downloader, mock_response, tmp_path):
        """Test download with custom filename."""
        mock_get.return_value = mock_response
        url = "https://example.com/file.parquet"
        
        filepath = downloader.download(url, filename="custom_name.parquet")
        
        assert filepath.name == "custom_name.parquet"
        assert filepath.exists()
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_generates_filename_from_url(self, mock_get, downloader, mock_response, tmp_path):
        """Test filename generation from URL."""
        mock_get.return_value = mock_response
        url = "https://example.com/path/to/myfile.parquet"
        
        filepath = downloader.download(url)
        
        assert filepath.name == "myfile.parquet"
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_adds_parquet_extension(self, mock_get, downloader, mock_response):
        """Test .parquet extension added if missing."""
        mock_get.return_value = mock_response
        url = "https://example.com/file"
        
        filepath = downloader.download(url)
        
        assert filepath.name == "file.parquet"
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_http_error(self, mock_get, downloader):
        """Test handling of HTTP errors."""
        mock_get.return_value.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        
        with pytest.raises(requests.HTTPError):
            downloader.download("https://example.com/missing.parquet")
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_warns_on_unexpected_content_type(self, mock_get, downloader, caplog):
        """Test warning logged for unexpected content type."""
        response = Mock()
        response.status_code = 200
        response.headers = {"Content-Type": "text/html"}
        response.iter_content = Mock(return_value=[b"data"])
        mock_get.return_value = response
        
        downloader.download("https://example.com/file.parquet")
        
        assert "Unexpected Content-Type" in caplog.text
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_batch_success(self, mock_get, downloader, mock_response):
        """Test batch download of multiple files."""
        mock_get.return_value = mock_response
        urls = [
            "https://example.com/file1.parquet",
            "https://example.com/file2.parquet",
            "https://example.com/file3.parquet",
        ]
        
        filepaths = downloader.download_batch(urls)
        
        assert len(filepaths) == 3
        assert mock_get.call_count == 3
        assert all(fp.exists() for fp in filepaths)
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_batch_with_max_files(self, mock_get, downloader, mock_response):
        """Test batch download respects max_files limit."""
        mock_get.return_value = mock_response
        urls = [f"https://example.com/file{i}.parquet" for i in range(10)]
        
        filepaths = downloader.download_batch(urls, max_files=3)
        
        assert len(filepaths) == 3
        assert mock_get.call_count == 3
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_batch_handles_errors(self, mock_get, downloader, mock_response, caplog):
        """Test batch download continues on errors."""
        # First call succeeds, second fails, third succeeds
        mock_get.side_effect = [
            mock_response,
            requests.HTTPError("500 Server Error"),
            mock_response,
        ]
        
        urls = [
            "https://example.com/file1.parquet",
            "https://example.com/file2.parquet",
            "https://example.com/file3.parquet",
        ]
        
        filepaths = downloader.download_batch(urls)
        
        # Should have 2 successful downloads
        assert len(filepaths) == 2
        assert "Failed to download" in caplog.text
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_chunk_size_parameter(self, mock_get, downloader, mock_response):
        """Test custom chunk_size parameter."""
        mock_get.return_value = mock_response
        
        downloader.download("https://example.com/file.parquet", chunk_size=16384)
        
        # Verify iter_content called with custom chunk_size
        mock_response.iter_content.assert_called_with(chunk_size=16384)


class TestDownloadParquetFunction:
    """Test standalone download_parquet function."""
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_download_parquet_function(self, mock_get, mock_response, tmp_path):
        """Test quick download function."""
        mock_get.return_value = mock_response
        url = "https://example.com/test.parquet"
        
        filepath = download_parquet(url, output_dir=str(tmp_path))
        
        assert filepath.exists()
        assert filepath.name == "test.parquet"
        assert filepath.parent == tmp_path


class TestRealDownload:
    """Integration test with real URL (optional - requires network)."""
    
    @pytest.mark.integration
    @pytest.mark.skipif(True, reason="Requires network access - enable manually")
    def test_real_download(self, tmp_path):
        """Test actual download from Azure Blob Storage."""
        downloader = ParquetDownloader(output_dir=str(tmp_path))
        url = "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-e1a/PT/SPO-PT02022_00008_100.parquet"
        
        filepath = downloader.download(url)
        
        assert filepath.exists()
        assert filepath.stat().st_size > 0
        
        # Verify it's a valid Parquet file (magic bytes)
        with open(filepath, "rb") as f:
            magic_bytes = f.read(4)
            assert magic_bytes == b"PAR1"
