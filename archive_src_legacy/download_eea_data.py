"""
EEA Air Quality Data Downloader

This script downloads air quality data from the EEA Download Service API.
Supports filtering by countries, cities, pollutants, datasets, and time ranges.
"""

import os
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import requests


class EEADownloader:
    """Download air quality data from EEA API."""

    BASE_URL = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"

    def __init__(self, output_dir: str = "data/raw"):
        """
        Initialize the downloader.

        Args:
            output_dir: Directory to save downloaded files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def download_parquet(
        self,
        countries: List[str] = None,
        cities: List[str] = None,
        pollutants: List[str] = None,
        dataset: int = 1,
        datetime_start: Optional[str] = None,
        datetime_end: Optional[str] = None,
        aggregation_type: Optional[str] = None,
        email: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> str:
        """
        Download parquet files with filters.

        Args:
            countries: List of country codes (e.g., ["IT", "ES"])
            cities: List of city names (e.g., ["Rome", "Madrid"])
            pollutants: List of pollutant codes or URIs (e.g., ["NO2", "PM10"])
            dataset: 1=E2a (UTD), 2=E1a (verified), 3=Airbase (historical)
            datetime_start: Start datetime (format: "YYYY-MM-DDTHH:MM:SSZ")
            datetime_end: End datetime (format: "YYYY-MM-DDTHH:MM:SSZ")
            aggregation_type: "hour", "day", or "var"
            email: Optional email for tracking
            filename: Custom filename for output (auto-generated if None)

        Returns:
            Path to downloaded file
        """
        endpoint = "ParquetFile"

        request_body = {
            "countries": countries or [],
            "cities": cities or [],
            "pollutants": pollutants or [],
            "dataset": dataset,
            "source": "Python Script",
        }

        if datetime_start:
            request_body["dateTimeStart"] = datetime_start
        if datetime_end:
            request_body["dateTimeEnd"] = datetime_end
        if aggregation_type:
            request_body["aggregationType"] = aggregation_type
        if email:
            request_body["email"] = email

        print(f"🌍 Downloading data from EEA API...")
        print(f"   Countries: {countries or 'All'}")
        print(f"   Pollutants: {pollutants or 'All'}")
        print(f"   Dataset: {self._get_dataset_name(dataset)}")

        response = requests.post(f"{self.BASE_URL}{endpoint}", json=request_body, timeout=300)

        if response.status_code == 206:
            print(
                "⚠️  Warning: Download exceeds 600MB limit. Consider using async or URLs endpoint."
            )

        response.raise_for_status()

        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            country_str = "_".join(countries[:3]) if countries else "all"
            filename = f"eea_data_{country_str}_{timestamp}.zip"

        filepath = self.output_dir / filename

        with open(filepath, "wb") as f:
            f.write(response.content)

        print(f"✅ Downloaded: {filepath} ({len(response.content) / 1024 / 1024:.2f} MB)")
        return str(filepath)

    def download_async(
        self,
        countries: List[str] = None,
        pollutants: List[str] = None,
        dataset: int = 1,
        datetime_start: Optional[str] = None,
        datetime_end: Optional[str] = None,
        email: Optional[str] = None,
        filename: Optional[str] = None,
        max_wait_seconds: int = 3600,
    ) -> str:
        """
        Download large datasets asynchronously.

        This endpoint is recommended for heavy requests (>600MB).
        The API will process the request in background and provide a download URL.

        Args:
            max_wait_seconds: Maximum time to wait for file generation (default: 1 hour)
            Other args: Same as download_parquet()

        Returns:
            Path to downloaded file
        """
        endpoint = "ParquetFile/async"

        request_body = {
            "countries": countries or [],
            "cities": [],
            "pollutants": pollutants or [],
            "dataset": dataset,
            "source": "Python Script",
        }

        if datetime_start:
            request_body["dateTimeStart"] = datetime_start
        if datetime_end:
            request_body["dateTimeEnd"] = datetime_end
        if email:
            request_body["email"] = email

        print(f"🌍 Starting async download from EEA API...")

        response = requests.post(f"{self.BASE_URL}{endpoint}", json=request_body)
        response.raise_for_status()

        download_url = response.text.strip()
        print(f"📦 File is being generated: {download_url}")
        print(f"⏳ Waiting for file to be ready...")

        # Poll until file is ready
        t_start = datetime.now()
        while True:
            elapsed = (datetime.now() - t_start).total_seconds()

            if elapsed > max_wait_seconds:
                raise TimeoutError(f"File generation exceeded {max_wait_seconds}s")

            parquet_response = requests.get(download_url)

            if parquet_response.status_code == 404:
                print(f"   Still processing... ({int(elapsed)}s elapsed)")
                time.sleep(20)
            else:
                break

        # Save file
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            country_str = "_".join(countries[:3]) if countries else "all"
            filename = f"eea_data_{country_str}_{timestamp}.zip"

        filepath = self.output_dir / filename

        with open(filepath, "wb") as f:
            f.write(parquet_response.content)

        print(f"✅ Downloaded: {filepath} ({len(parquet_response.content) / 1024 / 1024:.2f} MB)")
        return str(filepath)

    def download_urls(
        self,
        countries: List[str] = None,
        pollutants: List[str] = None,
        dataset: int = 1,
        filename: str = "parquet_urls.csv",
    ) -> str:
        """
        Download a CSV with URLs to individual parquet files.

        This is the most stable endpoint for heavy requests.
        You can then download individual files and apply custom filters.

        Args:
            countries: List of country codes
            pollutants: List of pollutant codes
            dataset: Dataset type (1, 2, or 3)
            filename: Output CSV filename

        Returns:
            Path to CSV file with URLs
        """
        endpoint = "ParquetFile/urls"

        request_body = {
            "countries": countries or [],
            "cities": [],
            "pollutants": pollutants or [],
            "dataset": dataset,
            "source": "Python Script",
        }

        print(f"📋 Downloading list of URLs...")

        response = requests.post(f"{self.BASE_URL}{endpoint}", json=request_body)
        response.raise_for_status()

        filepath = self.output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(response.text)

        # Count URLs
        urls = [line for line in response.text.split("\n") if line.strip()]
        print(f"✅ Saved {len(urls)-1} URLs to: {filepath}")

        return str(filepath)

    @staticmethod
    def _get_dataset_name(dataset: int) -> str:
        """Get human-readable dataset name."""
        names = {1: "E2a (Up-To-Date/UTD)", 2: "E1a (Verified)", 3: "Airbase (Historical)"}
        return names.get(dataset, f"Unknown ({dataset})")


def main():
    """Example usage of the EEA Downloader."""

    downloader = EEADownloader(output_dir="data/raw")

    # Example 1: Download recent data for Italy - NO2 and PM10
    print("\n" + "=" * 60)
    print("Example 1: Download Italy NO2 and PM10 (last 7 days)")
    print("=" * 60)

    downloader.download_parquet(
        countries=["IT"],
        pollutants=["NO2", "PM10"],
        dataset=1,  # E2a (Up-to-date)
        datetime_start="2025-11-16T00:00:00Z",
        datetime_end="2025-11-23T23:59:59Z",
        aggregation_type="hour",
        email="your.email@example.com",
        filename="italy_no2_pm10_recent.zip",
    )

    # Example 2: Download URLs for Spain - all pollutants
    print("\n" + "=" * 60)
    print("Example 2: Get URLs for Spain - verified data")
    print("=" * 60)

    downloader.download_urls(
        countries=["ES"], dataset=2, filename="spain_verified_urls.csv"  # E1a (Verified)
    )

    # Example 3: Large async download
    # Uncomment to test (takes longer)
    # print("\n" + "="*60)
    # print("Example 3: Async download - Multiple countries")
    # print("="*60)
    #
    # downloader.download_async(
    #     countries=["IT", "FR", "DE"],
    #     pollutants=["NO2", "O3", "PM10"],
    #     dataset=2,
    #     datetime_start="2023-01-01T00:00:00Z",
    #     datetime_end="2023-01-31T23:59:59Z",
    #     filename="multi_country_2023_jan.zip"
    # )


if __name__ == "__main__":
    main()
