"""
Test Suite for DiscoMap Package

Run with: pytest tests/ -v
"""

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


# Test fixtures and utilities
@pytest.fixture
def sample_dataframe():
    """Create a sample air quality DataFrame for testing."""
    n_records = 100

    return pd.DataFrame(
        {
            "Samplingpoint": [f"IT/SPO.IT{i:04d}A" for i in range(n_records)],
            "Pollutant": np.random.choice([5, 8], n_records),  # PM10, NO2
            "Start": pd.date_range("2025-11-16", periods=n_records, freq="h"),
            "End": pd.date_range("2025-11-16", periods=n_records, freq="h") + timedelta(hours=1),
            "Value": np.random.uniform(10, 50, n_records),
            "Unit": ["ug.m-3"] * n_records,
            "AggType": ["hour"] * n_records,
            "Validity": np.random.choice([1, 2, 3, -1], n_records),
            "Verification": np.random.choice([1, 2, 3], n_records),
            "ResultTime": [datetime.now()] * n_records,
            "DataCapture": np.random.uniform(80, 100, n_records),
            "FkObservationLog": ["test-uuid"] * n_records,
        }
    )


@pytest.fixture
def sample_dataframe_with_issues():
    """Create a DataFrame with data quality issues."""
    return pd.DataFrame(
        {
            "Samplingpoint": ["IT/SPO.IT0001A", "IT/SPO.IT0001A", None, "IT/SPO.IT0002A"],
            "Pollutant": [8, 8, 5, 8],
            "Start": pd.to_datetime(
                ["2025-11-16 01:00", "2025-11-16 01:00", "2025-11-16 02:00", "2025-11-16 03:00"]
            ),
            "End": pd.to_datetime(
                ["2025-11-16 02:00", "2025-11-16 02:00", "2025-11-16 03:00", "2025-11-16 04:00"]
            ),
            "Value": [25.5, 25.5, -10, 1500],  # Duplicate, negative, extreme
            "Unit": ["ug.m-3", "ug.m-3", "ug.m-3", "ug.m-3"],
            "AggType": ["hour", "hour", "hour", "hour"],
            "Validity": [1, 1, -1, 1],
            "Verification": [1, 1, 1, 1],
            "ResultTime": [datetime.now()] * 4,
            "DataCapture": [95.0, 95.0, 50.0, 95.0],
            "FkObservationLog": ["uuid1", "uuid1", "uuid2", "uuid3"],
        }
    )


@pytest.fixture
def temp_output_dir(tmp_path):
    """Create temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir
