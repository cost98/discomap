"""Performance tests for ParquetDownloader service."""

import pytest
import time
from pathlib import Path
from unittest.mock import Mock, patch
from concurrent.futures import ThreadPoolExecutor
import requests

from src.services.parquet_downloader import ParquetDownloader


@pytest.fixture
def downloader(tmp_path):
    """ParquetDownloader instance with temp directory."""
    return ParquetDownloader(output_dir=str(tmp_path))


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
