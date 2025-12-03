"""
Tests for Parser Module
"""

from pathlib import Path

import pandas as pd
import pytest
from src.parser import ParserError, filter_dataframe


class TestFilterDataframe:
    """Test DataFrame filtering functionality."""

    def test_filter_by_pollutants_with_names(self, sample_dataframe):
        """Test filtering by pollutant names."""
        from src.pollutant_mapper import add_pollutant_names

        df = add_pollutant_names(sample_dataframe)

        filtered = filter_dataframe(df, pollutants=["NO2"])
        assert len(filtered) > 0
        assert all(filtered["PollutantName"] == "NO2")

    def test_filter_by_pollutants_with_codes(self, sample_dataframe):
        """Test filtering by pollutant codes."""
        filtered = filter_dataframe(sample_dataframe, pollutants=["8"])
        assert len(filtered) > 0
        assert all(filtered["Pollutant"] == 8)

    def test_filter_by_validity(self, sample_dataframe):
        """Test filtering by validity flags."""
        filtered = filter_dataframe(sample_dataframe, validity=[1])
        assert len(filtered) > 0
        assert all(filtered["Validity"] == 1)

    def test_filter_by_multiple_validity(self, sample_dataframe):
        """Test filtering by multiple validity flags."""
        filtered = filter_dataframe(sample_dataframe, validity=[1, 2])
        assert len(filtered) > 0
        assert all(filtered["Validity"].isin([1, 2]))

    def test_filter_by_date_range(self, sample_dataframe):
        """Test filtering by date range."""
        filtered = filter_dataframe(
            sample_dataframe, start_date="2025-11-16 10:00:00", end_date="2025-11-16 20:00:00"
        )
        assert len(filtered) > 0
        assert all(filtered["Start"] >= pd.to_datetime("2025-11-16 10:00:00"))
        assert all(filtered["End"] <= pd.to_datetime("2025-11-16 20:00:00"))

    def test_filter_by_sampling_points(self, sample_dataframe):
        """Test filtering by sampling points."""
        points = sample_dataframe["Samplingpoint"].unique()[:5].tolist()
        filtered = filter_dataframe(sample_dataframe, sampling_points=points)

        assert len(filtered) > 0
        assert all(filtered["Samplingpoint"].isin(points))

    def test_filter_by_data_capture(self, sample_dataframe):
        """Test filtering by minimum data capture."""
        filtered = filter_dataframe(sample_dataframe, min_data_capture=90.0)
        assert len(filtered) > 0
        assert all(filtered["DataCapture"] >= 90.0)

    def test_filter_combined_criteria(self, sample_dataframe):
        """Test filtering with multiple criteria."""
        from src.pollutant_mapper import add_pollutant_names

        df = add_pollutant_names(sample_dataframe)

        filtered = filter_dataframe(df, pollutants=["NO2"], validity=[1, 2], min_data_capture=85.0)

        assert len(filtered) >= 0  # May be empty if no records match
        if len(filtered) > 0:
            assert all(filtered["PollutantName"] == "NO2")
            assert all(filtered["Validity"].isin([1, 2]))
            assert all(filtered["DataCapture"] >= 85.0)

    def test_filter_empty_result(self, sample_dataframe):
        """Test filter that returns empty result."""
        filtered = filter_dataframe(sample_dataframe, pollutants=["NONEXISTENT"])
        assert len(filtered) == 0

    def test_filter_no_criteria(self, sample_dataframe):
        """Test filter with no criteria returns all data."""
        filtered = filter_dataframe(sample_dataframe)
        assert len(filtered) == len(sample_dataframe)


class TestExport:
    """Test export functionality."""

    def test_export_to_csv(self, sample_dataframe, temp_output_dir):
        """Test CSV export."""
        from src.parser import export_to_csv

        output_path = temp_output_dir / "test_export.csv"
        result = export_to_csv(sample_dataframe, output_path)

        assert output_path.exists()
        assert result == output_path

        # Read back and verify
        df_read = pd.read_csv(output_path)
        assert len(df_read) == len(sample_dataframe)

    def test_export_to_parquet(self, sample_dataframe, temp_output_dir):
        """Test Parquet export."""
        from src.parser import export_to_parquet

        output_path = temp_output_dir / "test_export.parquet"
        result = export_to_parquet(sample_dataframe, output_path)

        assert output_path.exists()
        assert result == output_path

        # Read back and verify
        df_read = pd.read_parquet(output_path)
        assert len(df_read) == len(sample_dataframe)
