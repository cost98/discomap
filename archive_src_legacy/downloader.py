"""
EEA API Downloader Module

Production-ready client for downloading air quality data from EEA API.
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from .config import DATASET_NAMES, Config
from .logger import get_logger

logger = get_logger(__name__, Config.LOGS_DIR)


class DownloadError(Exception):
    """Custom exception for download errors."""

    pass


class EEADownloader:
    """
    Client for downloading air quality data from EEA API.

    Supports three download modes:
    1. Synchronous download (up to 600MB)
    2. Asynchronous download (large datasets)
    3. URL list download (for custom processing)
    """

    def __init__(self, output_dir: Optional[Path] = None, email: Optional[str] = None):
        """
        Initialize downloader.

        Args:
            output_dir: Directory for downloaded files (default: Config.DATA_RAW)
            email: Email for tracking (default: from config)
        """
        self.output_dir = Path(output_dir) if output_dir else Config.DATA_RAW
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.email = email or Config.EEA_EMAIL
        self.base_url = Config.EEA_API_BASE_URL

        logger.info(f"EEADownloader initialized. Output dir: {self.output_dir}")

    def _build_request_body(
        self,
        countries: Optional[List[str]] = None,
        cities: Optional[List[str]] = None,
        pollutants: Optional[List[str]] = None,
        dataset: int = Config.DATASET_E2A,
        datetime_start: Optional[str] = None,
        datetime_end: Optional[str] = None,
        aggregation_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Build request body for API call."""
        body = {
            "countries": countries or [],
            "cities": cities or [],
            "pollutants": pollutants or [],
            "dataset": dataset,
            "source": "DiscoMap Python Client",
        }

        if datetime_start:
            body["dateTimeStart"] = datetime_start
        if datetime_end:
            body["dateTimeEnd"] = datetime_end
        if aggregation_type:
            body["aggregationType"] = aggregation_type
        if self.email:
            body["email"] = self.email

        return body

    def _generate_filename(
        self, countries: Optional[List[str]], dataset: int, extension: str = "zip"
    ) -> str:
        """Generate filename for downloaded data."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dataset_name = DATASET_NAMES.get(dataset, f"dataset{dataset}").split()[0].lower()

        if countries and len(countries) <= 3:
            country_str = "_".join(countries)
        elif countries:
            country_str = f"{len(countries)}countries"
        else:
            country_str = "all"

        return f"eea_{country_str}_{dataset_name}_{timestamp}.{extension}"

    def download(
        self,
        countries: Optional[List[str]] = None,
        cities: Optional[List[str]] = None,
        pollutants: Optional[List[str]] = None,
        dataset: int = Config.DATASET_E2A,
        datetime_start: Optional[str] = None,
        datetime_end: Optional[str] = None,
        aggregation_type: Optional[str] = None,
        filename: Optional[str] = None,
        timeout: int = 300,
    ) -> Path:
        """
        Download parquet files synchronously.

        Best for: Small to medium datasets (< 600MB)

        Args:
            countries: List of country codes (e.g., ["IT", "ES"])
            cities: List of city names
            pollutants: List of pollutant codes (e.g., ["NO2", "PM10"])
            dataset: Dataset type (1=E2a, 2=E1a, 3=Airbase)
            datetime_start: Start datetime (ISO format)
            datetime_end: End datetime (ISO format)
            aggregation_type: "hour", "day", or "var"
            filename: Custom filename (auto-generated if None)
            timeout: Request timeout in seconds

        Returns:
            Path to downloaded file

        Raises:
            DownloadError: If download fails
        """
        logger.info(
            f"Starting download: countries={countries}, pollutants={pollutants}, "
            f"dataset={DATASET_NAMES.get(dataset)}"
        )

        request_body = self._build_request_body(
            countries, cities, pollutants, dataset, datetime_start, datetime_end, aggregation_type
        )
        
        # DEBUG: Log the exact request body being sent
        logger.info(f"📤 API Request Body: {request_body}")

        try:
            response = requests.post(
                f"{self.base_url}{Config.ENDPOINT_PARQUET}", json=request_body, timeout=timeout
            )

            if response.status_code == 206:
                logger.warning("Download exceeds 600MB limit. Consider using async mode.")

            response.raise_for_status()

            if not filename:
                filename = self._generate_filename(countries, dataset)

            filepath = self.output_dir / filename
            filepath.write_bytes(response.content)

            size_mb = len(response.content) / 1024 / 1024
            logger.info(f"Download complete: {filepath} ({size_mb:.2f} MB)")

            return filepath

        except requests.RequestException as e:
            logger.error(f"Download failed: {e}")
            logger.error(f"Request body was: {request_body}")
            raise DownloadError(f"Failed to download data: {e}") from e

    def download_async(
        self,
        countries: Optional[List[str]] = None,
        cities: Optional[List[str]] = None,
        pollutants: Optional[List[str]] = None,
        dataset: int = Config.DATASET_E2A,
        datetime_start: Optional[str] = None,
        datetime_end: Optional[str] = None,
        aggregation_type: Optional[str] = None,
        filename: Optional[str] = None,
        max_wait_seconds: Optional[int] = None,
    ) -> Path:
        """
        Download large datasets asynchronously.

        Best for: Large datasets (> 600MB)
        The API processes the request in background and provides a download URL.

        Args:
            max_wait_seconds: Maximum wait time (default: from config)
            Other args: Same as download()

        Returns:
            Path to downloaded file

        Raises:
            DownloadError: If download fails
            TimeoutError: If file generation takes too long
        """
        max_wait = max_wait_seconds or Config.ASYNC_TIMEOUT_SECONDS

        logger.info(f"Starting async download: countries={countries}, pollutants={pollutants}")

        request_body = self._build_request_body(
            countries, cities, pollutants, dataset, datetime_start, datetime_end, aggregation_type
        )

        try:
            # Initiate async processing
            response = requests.post(f"{self.base_url}{Config.ENDPOINT_ASYNC}", json=request_body)
            response.raise_for_status()

            download_url = response.text.strip()
            logger.info(f"File generation started. URL: {download_url}")

            # Poll until ready
            t_start = datetime.now()
            attempt = 0

            while True:
                elapsed = (datetime.now() - t_start).total_seconds()
                attempt += 1

                if elapsed > max_wait:
                    raise TimeoutError(f"File generation exceeded {max_wait}s timeout")

                file_response = requests.get(download_url)

                if file_response.status_code == 404:
                    logger.debug(
                        f"Polling attempt {attempt}: Still processing " f"({int(elapsed)}s elapsed)"
                    )
                    time.sleep(Config.ASYNC_POLL_INTERVAL)
                else:
                    file_response.raise_for_status()
                    break

            # Save file
            if not filename:
                filename = self._generate_filename(countries, dataset)

            filepath = self.output_dir / filename
            filepath.write_bytes(file_response.content)

            size_mb = len(file_response.content) / 1024 / 1024
            logger.info(
                f"Async download complete: {filepath} ({size_mb:.2f} MB) " f"in {int(elapsed)}s"
            )

            return filepath

        except requests.RequestException as e:
            logger.error(f"Async download failed: {e}")
            raise DownloadError(f"Failed to download data: {e}") from e

    def download_urls(
        self,
        countries: Optional[List[str]] = None,
        cities: Optional[List[str]] = None,
        pollutants: Optional[List[str]] = None,
        dataset: int = Config.DATASET_E2A,
        datetime_start: Optional[str] = None,
        datetime_end: Optional[str] = None,
        aggregation_type: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Path:
        """
        Download CSV with URLs to individual parquet files.

        Best for: Very large datasets or custom processing
        Most stable endpoint for heavy requests.

        Args:
            countries: List of country codes
            cities: List of city names
            pollutants: List of pollutant codes
            dataset: Dataset type
            datetime_start: Start datetime (ISO format)
            datetime_end: End datetime (ISO format)
            aggregation_type: "hour", "day", or "var"
            filename: Output CSV filename

        Returns:
            Path to CSV file with URLs

        Raises:
            DownloadError: If download fails
        """
        logger.info(f"Downloading URL list: countries={countries}, dataset={dataset}")

        request_body = self._build_request_body(
            countries, cities, pollutants, dataset, datetime_start, datetime_end, aggregation_type
        )
        
        # DEBUG: Log the exact request body being sent
        logger.info(f"📤 API Request Body: {request_body}")

        try:
            response = requests.post(f"{self.base_url}{Config.ENDPOINT_URLS}", json=request_body)
            response.raise_for_status()

            if not filename:
                filename = self._generate_filename(countries, dataset, "csv")

            filepath = self.output_dir / filename
            filepath.write_text(response.text, encoding="utf-8")

            url_count = len([l for l in response.text.split("\n") if l.strip()]) - 1
            logger.info(f"URL list saved: {filepath} ({url_count} URLs)")

            return filepath

        except requests.RequestException as e:
            logger.error(f"URL download failed: {e}")
            logger.error(f"Request body was: {request_body}")
            raise DownloadError(f"Failed to download URLs: {e}") from e

    def get_parquet_urls(
        self,
        countries: Optional[List[str]] = None,
        cities: Optional[List[str]] = None,
        pollutants: Optional[List[str]] = None,
        dataset: int = Config.DATASET_E2A,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        aggregation_type: Optional[str] = None,
    ) -> List[str]:
        """
        Get list of Parquet URLs from EEA API without downloading files.
        
        Args:
            countries: List of country codes
            cities: List of city names
            pollutants: List of pollutant codes
            dataset: Dataset type
            start_date: Start date (YYYY-MM-DD or ISO format)
            end_date: End date (YYYY-MM-DD or ISO format)
            aggregation_type: "hour", "day", or "var"
            
        Returns:
            List of Parquet file URLs
        """
        # Convert date formats if needed
        if start_date and len(start_date) == 10:
            start_date = f"{start_date}T00:00:00Z"
        if end_date and len(end_date) == 10:
            end_date = f"{end_date}T23:59:59Z"
            
        # Download CSV with URLs
        csv_path = self.download_urls(
            countries=countries,
            cities=cities,
            pollutants=pollutants,
            dataset=dataset,
            datetime_start=start_date,
            datetime_end=end_date,
            aggregation_type=aggregation_type
        )
        
        # Parse URLs from CSV
        urls = []
        with open(csv_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and line.startswith('http'):
                    urls.append(line)
        
        # Cleanup CSV
        csv_path.unlink()
        
        return urls

    def get_summary(
        self,
        countries: Optional[List[str]] = None,
        cities: Optional[List[str]] = None,
        pollutants: Optional[List[str]] = None,
        dataset: int = Config.DATASET_E2A,
    ) -> Dict[str, Any]:
        """
        Get download summary without downloading data.

        Useful for checking size and file count before downloading.

        Args:
            countries: List of country codes
            cities: List of city names
            pollutants: List of pollutant codes
            dataset: Dataset type

        Returns:
            Dictionary with summary information
        """
        logger.info(f"Fetching download summary: countries={countries}")

        request_body = self._build_request_body(countries, cities, pollutants, dataset)

        try:
            response = requests.post(f"{self.base_url}{Config.ENDPOINT_SUMMARY}", json=request_body)
            response.raise_for_status()

            summary = response.json()
            logger.info(
                f"Summary: {summary.get('fileCount', 0)} files, "
                f"{summary.get('totalSize', 0) / 1024 / 1024:.2f} MB"
            )

            return summary

        except requests.RequestException as e:
            logger.error(f"Summary request failed: {e}")
            raise DownloadError(f"Failed to get summary: {e}") from e
