"""Real-world performance benchmark using actual Parquet URLs from CSV.

Tests download performance with real Azure Blob Storage URLs:
- Uses ParquetFilesUrls CSV with 6989 files
- Measures sequential vs parallel download performance
- Provides time estimates for full dataset downloads
"""

import pytest
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.services.parquet_downloader import ParquetDownloader


def load_urls_from_csv(csv_path: str, max_urls: int = None) -> list[str]:
    """Load URLs from CSV file."""
    urls = []
    with open(csv_path, 'r') as f:
        # Skip header
        next(f)
        for line in f:
            url = line.strip()
            if url:
                urls.append(url)
                if max_urls and len(urls) >= max_urls:
                    break
    return urls


@pytest.mark.benchmark
@pytest.mark.skipif(not Path("ParquetFilesUrls (54).csv").exists(), 
                    reason="Requires ParquetFilesUrls (54).csv file")
class TestRealWorldDownloadPerformance:
    """Performance tests with real Parquet URLs from CSV."""
    
    def test_sequential_vs_parallel_real_files(self, tmp_path):
        """Compare sequential vs parallel download with 20 real files."""
        csv_path = "ParquetFilesUrls (54).csv"
        urls = load_urls_from_csv(csv_path, max_urls=20)
        
        assert len(urls) == 20, f"Expected 20 URLs, got {len(urls)}"
        
        downloader = ParquetDownloader(output_dir=str(tmp_path / "sequential"))
        
        # Sequential download
        start_time = time.time()
        seq_files = []
        for url in urls:
            filepath = downloader.download(url)
            seq_files.append(filepath)
        seq_time = time.time() - start_time
        
        # Parallel download
        downloader_parallel = ParquetDownloader(output_dir=str(tmp_path / "parallel"))
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(downloader_parallel.download, url) for url in urls]
            par_files = [f.result() for f in futures]
        par_time = time.time() - start_time
        
        # Verify downloads
        assert len(seq_files) == 20
        assert len(par_files) == 20
        assert all(f.exists() for f in seq_files)
        assert all(f.exists() for f in par_files)
        
        # Parallel should be faster
        assert par_time < seq_time
        speedup = seq_time / par_time
        
        print(f"\n{'='*60}")
        print(f"SEQUENTIAL: {seq_time:.2f}s ({20/seq_time:.2f} files/sec)")
        print(f"PARALLEL:   {par_time:.2f}s ({20/par_time:.2f} files/sec)")
        print(f"SPEEDUP:    {speedup:.2f}x")
        print(f"{'='*60}")
    
    def test_parallel_workers_comparison(self, tmp_path):
        """Compare different worker counts with 50 real files."""
        csv_path = "ParquetFilesUrls (54).csv"
        urls = load_urls_from_csv(csv_path, max_urls=50)
        
        assert len(urls) == 50
        
        results = []
        
        for workers in [5, 10, 20]:
            output_dir = tmp_path / f"workers_{workers}"
            downloader = ParquetDownloader(output_dir=str(output_dir))
            
            start_time = time.time()
            total_bytes = 0
            
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(downloader.download, url) for url in urls]
                filepaths = [f.result() for f in futures]
            
            elapsed = time.time() - start_time
            total_bytes = sum(f.stat().st_size for f in filepaths)
            
            results.append({
                'workers': workers,
                'time': elapsed,
                'files_per_sec': 50 / elapsed,
                'mb_per_sec': (total_bytes / (1024 * 1024)) / elapsed,
            })
            
            assert len(filepaths) == 50
            assert all(f.exists() for f in filepaths)
        
        # Print results
        print(f"\n{'='*60}")
        print(f"WORKER COMPARISON (50 files)")
        print(f"{'='*60}")
        print(f"{'Workers':<10} {'Time (s)':<12} {'Files/sec':<12} {'MB/sec':<12}")
        print(f"{'-'*60}")
        for r in results:
            print(f"{r['workers']:<10} {r['time']:<12.2f} {r['files_per_sec']:<12.2f} {r['mb_per_sec']:<12.2f}")
        print(f"{'='*60}")
        
        # More workers should be faster (or at least not slower)
        assert results[-1]['files_per_sec'] >= results[0]['files_per_sec']
    
    def test_estimate_full_dataset_download_time(self, tmp_path):
        """Estimate time to download all 6989 files based on sample."""
        csv_path = "ParquetFilesUrls (54).csv"
        
        # Get total count
        all_urls = load_urls_from_csv(csv_path)
        total_files = len(all_urls)
        
        # Test with sample
        sample_size = 30
        urls = all_urls[:sample_size]
        
        downloader = ParquetDownloader(output_dir=str(tmp_path))
        
        start_time = time.time()
        total_bytes = 0
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(downloader.download, url) for url in urls]
            filepaths = [f.result() for f in futures]
        
        elapsed = time.time() - start_time
        total_bytes = sum(f.stat().st_size for f in filepaths)
        
        # Calculate metrics
        files_per_sec = sample_size / elapsed
        mb_per_sec = (total_bytes / (1024 * 1024)) / elapsed
        avg_file_size_mb = (total_bytes / (1024 * 1024)) / sample_size
        
        # Estimate for full dataset
        estimated_time_sec = total_files / files_per_sec
        estimated_time_min = estimated_time_sec / 60
        estimated_total_gb = (total_files * avg_file_size_mb) / 1024
        
        print(f"\n{'='*60}")
        print(f"FULL DATASET ESTIMATE")
        print(f"{'='*60}")
        print(f"Sample size:           {sample_size} files")
        print(f"Sample time:           {elapsed:.2f} seconds")
        print(f"Files/sec:             {files_per_sec:.2f}")
        print(f"MB/sec:                {mb_per_sec:.2f}")
        print(f"Avg file size:         {avg_file_size_mb:.2f} MB")
        print(f"\nFull dataset ({total_files} files):")
        print(f"  Estimated time:      {estimated_time_min:.1f} minutes")
        print(f"  Estimated data:      {estimated_total_gb:.2f} GB")
        print(f"  With 20 workers")
        print(f"{'='*60}")
        
        # Verify downloads worked
        assert len(filepaths) == sample_size
        assert all(f.exists() for f in filepaths)
        assert files_per_sec > 1.0  # Should download at least 1 file/sec
