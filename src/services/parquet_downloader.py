"""Service for downloading Parquet files from Azure Blob Storage.

Downloads EEA air quality data files from blob.core.windows.net URLs.
"""

import logging
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class ParquetDownloader:
    """Download Parquet files from URLs."""
    
    def __init__(self, output_dir: str = "data/raw/parquet"):
        """Initialize downloader.
        
        Args:
            output_dir: Directory to save downloaded files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"ParquetDownloader initialized. Output: {self.output_dir}")
    
    def download(
        self,
        url: str,
        filename: Optional[str] = None,
        chunk_size: int = 16384,  # 16KB - bilanciato
    ) -> Path:
        """Download Parquet file from URL.
        
        Args:
            url: URL to download from (e.g., blob.core.windows.net)
            filename: Custom filename (auto-generated from URL if None)
            chunk_size: Download chunk size in bytes
            
        Returns:
            Path to downloaded file
            
        Example:
            >>> downloader = ParquetDownloader()
            >>> url = "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-e1a/PT/SPO-PT02022_00008_100.parquet"
            >>> filepath = downloader.download(url)
            >>> print(f"Downloaded: {filepath}")
        """
        logger.info(f"Downloading: {url}")
        
        # Generate filename from URL if not provided
        if not filename:
            filename = url.split("/")[-1]
            if not filename.endswith(".parquet"):
                filename += ".parquet"
        
        # Download with streaming
        response = requests.get(
            url,
            stream=True,
            timeout=300,
            headers={"User-Agent": "DiscoMap/1.0"},
        )
        response.raise_for_status()
        
        # Check content type
        content_type = response.headers.get("Content-Type", "")
        if "parquet" not in content_type and "octet-stream" not in content_type:
            logger.warning(f"Unexpected Content-Type: {content_type}")
        
        # Save to file
        filepath = self.output_dir / filename
        
        total_bytes = 0
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)
        
        file_size_mb = total_bytes / (1024 * 1024)
        logger.info(f"Downloaded {file_size_mb:.2f} MB to: {filepath}")
        
        return filepath
    
    def download_batch(
        self,
        urls: list[str],
        max_files: Optional[int] = None,
    ) -> list[Path]:
        """Download multiple Parquet files.
        
        Args:
            urls: List of URLs to download
            max_files: Maximum number of files to download (None = all)
            
        Returns:
            List of paths to downloaded files
        """
        if max_files:
            urls = urls[:max_files]
        
        logger.info(f"Downloading {len(urls)} files...")
        
        downloaded = []
        for i, url in enumerate(urls, 1):
            try:
                filepath = self.download(url)
                downloaded.append(filepath)
                logger.info(f"Progress: {i}/{len(urls)}")
            except Exception as e:
                logger.error(f"Failed to download {url}: {e}")
        
        logger.info(f"Downloaded {len(downloaded)}/{len(urls)} files")
        return downloaded


def download_parquet(url: str, output_dir: str = "data/raw/parquet") -> Path:
    """Quick function to download a single Parquet file.
    
    Args:
        url: URL to download from
        output_dir: Directory to save file
        
    Returns:
        Path to downloaded file
        
    Example:
        >>> from src.services.parquet_downloader import download_parquet
        >>> url = "https://eeadmz1batchservice02.blob.core.windows.net/airquality-p-e1a/PT/SPO-PT02022_00008_100.parquet"
        >>> filepath = download_parquet(url)
    """
    downloader = ParquetDownloader(output_dir)
    return downloader.download(url)
