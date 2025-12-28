"""Unit tests for ParquetDownloader service."""

import pytest
import time
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from concurrent.futures import ThreadPoolExecutor
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


class TestParallelDownloadPerformance:
    """Performance tests for parallel downloads."""
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_parallel_download_performance(self, mock_get, tmp_path):
        """Test parallel download is faster than sequential."""
        # Simulate slow downloads (100ms each)
        def slow_response(*args, **kwargs):
            time.sleep(0.1)
            response = Mock()
            response.status_code = 200
            response.headers = {"Content-Type": "application/octet-stream"}
            response.iter_content = Mock(return_value=[b"PAR1", b"data"])
            return response
        
        mock_get.side_effect = slow_response
        
        downloader = ParquetDownloader(output_dir=str(tmp_path))
        urls = [f"https://example.com/file{i}.parquet" for i in range(10)]
        
        # Sequential download
        start_time = time.time()
        for url in urls:
            downloader.download(url)
        sequential_time = time.time() - start_time
        
        # Parallel download (same files, different names)
        mock_get.side_effect = slow_response
        parallel_urls = [f"https://example.com/parallel{i}.parquet" for i in range(10)]
        
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(downloader.download, url) for url in parallel_urls]
            filepaths = [f.result() for f in futures]
        parallel_time = time.time() - start_time
        
        # Parallel should be at least 2x faster
        assert parallel_time < sequential_time / 2
        assert len(filepaths) == 10
        assert all(fp.exists() for fp in filepaths)
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_concurrent_downloads_no_corruption(self, mock_get, tmp_path):
        """Test concurrent downloads don't corrupt files."""
        # Different content for each file
        def unique_response(*args, **kwargs):
            url = args[0]
            file_num = url.split("file")[-1].split(".")[0]
            content = f"PAR1-{file_num}".encode()
            
            response = Mock()
            response.status_code = 200
            response.headers = {"Content-Type": "application/octet-stream"}
            response.iter_content = Mock(return_value=[content])
            return response
        
        mock_get.side_effect = unique_response
        
        downloader = ParquetDownloader(output_dir=str(tmp_path))
        urls = [f"https://example.com/file{i}.parquet" for i in range(20)]
        
        # Download all files concurrently
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(downloader.download, url) for url in urls]
            filepaths = [f.result() for f in futures]
        
        # Verify all files exist and have correct content
        assert len(filepaths) == 20
        for i, filepath in enumerate(filepaths):
            assert filepath.exists()
            content = filepath.read_bytes()
            expected = f"PAR1-{i}".encode()
            assert content == expected
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_parallel_download_with_errors(self, mock_get, tmp_path):
        """Test parallel downloads handle errors gracefully."""
        call_count = [0]
        
        def mixed_response(*args, **kwargs):
            call_count[0] += 1
            # Every 3rd request fails
            if call_count[0] % 3 == 0:
                raise requests.HTTPError("500 Server Error")
            
            response = Mock()
            response.status_code = 200
            response.headers = {"Content-Type": "application/octet-stream"}
            response.iter_content = Mock(return_value=[b"PAR1", b"data"])
            return response
        
        mock_get.side_effect = mixed_response
        
        downloader = ParquetDownloader(output_dir=str(tmp_path))
        urls = [f"https://example.com/file{i}.parquet" for i in range(10)]
        
        # Download with ThreadPoolExecutor
        successful = []
        failed = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(downloader.download, url): url for url in urls}
            
            for future in futures:
                try:
                    filepath = future.result()
                    successful.append(filepath)
                except requests.HTTPError:
                    failed.append(futures[future])
        
        # Should have ~7 successful and ~3 failed
        assert len(successful) >= 6
        assert len(failed) >= 2
        assert len(successful) + len(failed) == 10
        assert all(fp.exists() for fp in successful)
    
    @patch("src.services.parquet_downloader.requests.get")
    def test_high_concurrency_stress(self, mock_get, tmp_path):
        """Stress test with high concurrency (50 parallel downloads)."""
        response = Mock()
        response.status_code = 200
        response.headers = {"Content-Type": "application/octet-stream"}
        response.iter_content = Mock(return_value=[b"PAR1", b"test"])
        mock_get.return_value = response
        
        downloader = ParquetDownloader(output_dir=str(tmp_path))
        urls = [f"https://example.com/stress{i}.parquet" for i in range(50)]
        
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(downloader.download, url) for url in urls]
            filepaths = [f.result() for f in futures]
        elapsed = time.time() - start_time
        
        # All downloads should complete
        assert len(filepaths) == 50
        assert all(fp.exists() for fp in filepaths)
        
        # Should complete in reasonable time (< 5 seconds)
        assert elapsed < 5.0
        
        # No duplicate files
        filenames = [fp.name for fp in filepaths]
        assert len(filenames) == len(set(filenames))

