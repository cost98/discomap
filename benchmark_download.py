"""Benchmark download performance with real URLs from CSV.

Tests downloading all files from ParquetFilesUrls CSV:
- Sequential download
- Parallel download with different worker counts
"""

import csv
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.services.parquet_downloader import ParquetDownloader


def load_urls(csv_path: str) -> list[str]:
    """Load URLs from CSV file."""
    urls = []
    with open(csv_path, 'r') as f:
        # Skip header
        next(f)
        for line in f:
            url = line.strip()
            if url:
                urls.append(url)
    return urls


def download_sequential(downloader: ParquetDownloader, urls: list[str]) -> dict:
    """Download files sequentially."""
    print(f"\n{'='*60}")
    print(f"SEQUENTIAL DOWNLOAD - {len(urls)} files")
    print(f"{'='*60}")
    
    start_time = time.time()
    successful = 0
    failed = 0
    total_bytes = 0
    
    for i, url in enumerate(urls, 1):
        try:
            filepath = downloader.download(url)
            file_size = filepath.stat().st_size
            total_bytes += file_size
            successful += 1
            
            if i % 100 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed
                print(f"Progress: {i}/{len(urls)} files ({rate:.1f} files/sec)")
        except Exception as e:
            print(f"Failed {url}: {e}")
            failed += 1
    
    elapsed = time.time() - start_time
    
    return {
        'mode': 'sequential',
        'total_files': len(urls),
        'successful': successful,
        'failed': failed,
        'elapsed_seconds': elapsed,
        'files_per_second': successful / elapsed,
        'total_mb': total_bytes / (1024 * 1024),
        'mb_per_second': (total_bytes / (1024 * 1024)) / elapsed,
    }


def download_parallel(
    downloader: ParquetDownloader,
    urls: list[str],
    max_workers: int = 10,
) -> dict:
    """Download files in parallel."""
    print(f"\n{'='*60}")
    print(f"PARALLEL DOWNLOAD - {len(urls)} files ({max_workers} workers)")
    print(f"{'='*60}")
    
    start_time = time.time()
    successful = 0
    failed = 0
    total_bytes = 0
    completed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(downloader.download, url): url for url in urls}
        
        for future in as_completed(futures):
            completed += 1
            url = futures[future]
            
            try:
                filepath = future.result()
                file_size = filepath.stat().st_size
                total_bytes += file_size
                successful += 1
                
                if completed % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed
                    print(f"Progress: {completed}/{len(urls)} files ({rate:.1f} files/sec)")
            except Exception as e:
                print(f"Failed {url}: {e}")
                failed += 1
    
    elapsed = time.time() - start_time
    
    return {
        'mode': f'parallel ({max_workers} workers)',
        'total_files': len(urls),
        'successful': successful,
        'failed': failed,
        'elapsed_seconds': elapsed,
        'files_per_second': successful / elapsed,
        'total_mb': total_bytes / (1024 * 1024),
        'mb_per_second': (total_bytes / (1024 * 1024)) / elapsed,
    }


def print_results(results: dict):
    """Print benchmark results."""
    print(f"\n{'='*60}")
    print(f"RESULTS - {results['mode']}")
    print(f"{'='*60}")
    print(f"Total files:      {results['total_files']}")
    print(f"Successful:       {results['successful']}")
    print(f"Failed:           {results['failed']}")
    print(f"Total time:       {results['elapsed_seconds']:.2f} seconds")
    print(f"Download rate:    {results['files_per_second']:.2f} files/sec")
    print(f"Total data:       {results['total_mb']:.2f} MB")
    print(f"Throughput:       {results['mb_per_second']:.2f} MB/sec")
    print(f"{'='*60}\n")


def main():
    """Run benchmark."""
    # Configuration
    csv_path = "ParquetFilesUrls (54).csv"
    output_dir = "data/raw/benchmark_test"
    sample_size = 20  # Test with first N files (or None for all)
    
    # Load URLs
    print(f"Loading URLs from {csv_path}...")
    all_urls = load_urls(csv_path)
    print(f"Loaded {len(all_urls)} URLs")
    
    # Use sample for testing
    urls = all_urls[:sample_size] if sample_size else all_urls
    print(f"Testing with {len(urls)} files")
    
    # Initialize downloader
    downloader = ParquetDownloader(output_dir=output_dir)
    
    # Run benchmarks
    all_results = []
    
    # Test 1: Sequential (small sample only)
    if len(urls) <= 50:
        results_seq = download_sequential(downloader, urls)
        print_results(results_seq)
        all_results.append(results_seq)
    
    # Test 2: Parallel with 5 workers
    results_p5 = download_parallel(downloader, urls, max_workers=5)
    print_results(results_p5)
    all_results.append(results_p5)
    
    # Test 3: Parallel with 10 workers
    results_p10 = download_parallel(downloader, urls, max_workers=10)
    print_results(results_p10)
    all_results.append(results_p10)
    
    # Test 4: Parallel with 20 workers
    results_p20 = download_parallel(downloader, urls, max_workers=20)
    print_results(results_p20)
    all_results.append(results_p20)
    
    # Summary comparison
    print(f"\n{'='*60}")
    print("SUMMARY COMPARISON")
    print(f"{'='*60}")
    print(f"{'Mode':<25} {'Files/sec':<15} {'MB/sec':<15} {'Total time':<15}")
    print(f"{'-'*60}")
    for r in all_results:
        print(f"{r['mode']:<25} {r['files_per_second']:<15.2f} {r['mb_per_second']:<15.2f} {r['elapsed_seconds']:<15.2f}")
    print(f"{'='*60}\n")
    
    # Estimate for all files
    if sample_size and len(all_urls) > sample_size:
        best_result = max(all_results, key=lambda x: x['files_per_second'])
        estimated_time = len(all_urls) / best_result['files_per_second']
        print(f"\nEstimated time for all {len(all_urls)} files with {best_result['mode']}:")
        print(f"  {estimated_time:.2f} seconds ({estimated_time/60:.2f} minutes)")


if __name__ == "__main__":
    main()
