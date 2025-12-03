"""
Tests for Config Module
"""

from pathlib import Path

import pytest
from src.config import DATASET_NAMES, VALIDITY_FLAGS, Config


class TestConfig:
    """Test configuration management."""

    def test_project_paths(self):
        """Test that project paths are correctly defined."""
        assert Config.PROJECT_ROOT.exists()
        assert isinstance(Config.DATA_DIR, Path)
        assert isinstance(Config.DATA_RAW, Path)
        assert isinstance(Config.DATA_PROCESSED, Path)

    def test_dataset_constants(self):
        """Test dataset type constants."""
        assert Config.DATASET_E2A == 1
        assert Config.DATASET_E1A == 2
        assert Config.DATASET_AIRBASE == 3

    def test_aggregation_constants(self):
        """Test aggregation type constants."""
        assert Config.AGG_HOURLY == "hour"
        assert Config.AGG_DAILY == "day"
        assert Config.AGG_VARIABLE == "var"

    def test_dataset_names_mapping(self):
        """Test dataset name mapping."""
        assert "E2a" in DATASET_NAMES[Config.DATASET_E2A]
        assert "E1a" in DATASET_NAMES[Config.DATASET_E1A]
        assert "Airbase" in DATASET_NAMES[Config.DATASET_AIRBASE]

    def test_validity_flags_mapping(self):
        """Test validity flags are defined."""
        assert 1 in VALIDITY_FLAGS
        assert -1 in VALIDITY_FLAGS
        assert -99 in VALIDITY_FLAGS

    def test_ensure_directories(self):
        """Test directory creation."""
        Config.ensure_directories()
        assert Config.DATA_RAW.exists()
        assert Config.DATA_PROCESSED.exists()
        assert Config.LOGS_DIR.exists()

    def test_get_temp_dir(self):
        """Test temp directory creation."""
        temp_dir = Config.get_temp_dir()
        assert temp_dir.exists()
        assert temp_dir.name == "temp"
