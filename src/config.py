"""
Configuration Management for DiscoMap

Centralized configuration for API endpoints, paths, and settings.
"""

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Application configuration."""

    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    DATA_RAW = DATA_DIR / "raw"
    DATA_PROCESSED = DATA_DIR / "processed"
    LOGS_DIR = PROJECT_ROOT / "logs"

    # EEA API Configuration
    EEA_API_BASE_URL = os.getenv(
        "EEA_API_BASE_URL", "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"
    )
    EEA_EMAIL = os.getenv("EEA_EMAIL", "")

    # API Endpoints
    ENDPOINT_PARQUET = "ParquetFile"
    ENDPOINT_ASYNC = "ParquetFile/async"
    ENDPOINT_URLS = "ParquetFile/urls"
    ENDPOINT_SUMMARY = "DownloadSummary"

    # Dataset types
    DATASET_E2A = 1  # Up-To-Date (UTD) - 2023 onwards
    DATASET_E1A = 2  # Verified - 2013-2022
    DATASET_AIRBASE = 3  # Historical - 2002-2012

    # Aggregation types
    AGG_HOURLY = "hour"
    AGG_DAILY = "day"
    AGG_VARIABLE = "var"

    # Download limits
    MAX_DOWNLOAD_SIZE_MB = 600
    ASYNC_TIMEOUT_SECONDS = 3600
    ASYNC_POLL_INTERVAL = 20

    # Data quality thresholds
    MIN_DATA_CAPTURE_PCT = 75.0
    VALID_FLAGS = [1, 2, 3]  # Valid data flags
    INVALID_FLAGS = [-1, -99]  # Invalid data flags

    @classmethod
    def ensure_directories(cls):
        """Create necessary directories if they don't exist."""
        for directory in [cls.DATA_RAW, cls.DATA_PROCESSED, cls.LOGS_DIR]:
            directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_temp_dir(cls) -> Path:
        """Get temporary directory for extractions."""
        temp_dir = cls.DATA_DIR / "temp"
        temp_dir.mkdir(parents=True, exist_ok=True)
        return temp_dir


# Dataset name mapping
DATASET_NAMES = {
    Config.DATASET_E2A: "E2a (Up-To-Date/UTD)",
    Config.DATASET_E1A: "E1a (Verified)",
    Config.DATASET_AIRBASE: "Airbase (Historical)",
}

# Validity flag descriptions
VALIDITY_FLAGS = {
    1: "Valid",
    2: "Valid (below detection limit)",
    3: "Valid (below detection limit + ...)",
    -1: "Not valid",
    -99: "Not valid (station maintenance)",
}

# Verification flag descriptions
VERIFICATION_FLAGS = {1: "Verified", 2: "Preliminary verified", 3: "Not verified"}


if __name__ == "__main__":
    Config.ensure_directories()
    print("Configuration Summary")
    print("=" * 60)
    print(f"Project Root: {Config.PROJECT_ROOT}")
    print(f"Data Directory: {Config.DATA_DIR}")
    print(f"API Base URL: {Config.EEA_API_BASE_URL}")
    print(f"Email: {Config.EEA_EMAIL or '(not set)'}")
    print("\nDirectories created successfully!")
