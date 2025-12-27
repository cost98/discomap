"""
Tests for configuration module.
"""

import pytest

from src.config import Config


def test_config_paths():
    """Test Config path configuration."""
    config = Config()
    
    assert config.PROJECT_ROOT.exists()
    assert config.DATA_DIR == config.PROJECT_ROOT / "data"
    assert config.LOGS_DIR == config.PROJECT_ROOT / "logs"


def test_config_endpoints():
    """Test EEA API endpoints configuration."""
    config = Config()
    
    assert config.ENDPOINT_PARQUET == "ParquetFile"
    assert config.ENDPOINT_ASYNC == "ParquetFile/async"
    assert config.ENDPOINT_URLS == "ParquetFile/urls"


def test_config_datasets():
    """Test dataset type constants."""
    config = Config()
    
    assert config.DATASET_E2A == 1  # UTD 2023+
    assert config.DATASET_E1A == 2  # Verified 2013-2022
    assert config.DATASET_AIRBASE == 3  # Historical 2002-2012
