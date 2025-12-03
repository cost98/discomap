"""
Tests for Analyzer Module
"""

import numpy as np
import pandas as pd
import pytest
from src.analyzer import (
    calculate_temporal_patterns,
    compare_stations,
    get_pollutant_statistics,
    get_summary,
    identify_exceedances,
)
from src.pollutant_mapper import add_pollutant_names


class TestGetSummary:
    """Test summary statistics generation."""

    def test_summary_basic_stats(self, sample_dataframe):
        """Test basic summary statistics."""
        summary = get_summary(sample_dataframe)

        assert summary["total_records"] == len(sample_dataframe)
        assert summary["sampling_points"] > 0
        assert len(summary["pollutants"]) > 0

    def test_summary_date_range(self, sample_dataframe):
        """Test date range calculation."""
        summary = get_summary(sample_dataframe)

        assert "date_range" in summary
        assert summary["date_range"] is not None
        assert "start" in summary["date_range"]
        assert "end" in summary["date_range"]
        assert "days" in summary["date_range"]

    def test_summary_with_pollutant_names(self, sample_dataframe):
        """Test summary with pollutant names."""
        df = add_pollutant_names(sample_dataframe)
        summary = get_summary(df)

        assert "NO2" in summary["pollutants"] or "PM10" in summary["pollutants"]

    def test_summary_validity_distribution(self, sample_dataframe):
        """Test validity distribution."""
        summary = get_summary(sample_dataframe)

        assert "validity_distribution" in summary
        assert len(summary["validity_distribution"]) > 0


class TestGetPollutantStatistics:
    """Test pollutant statistics."""

    def test_statistics_by_sampling_point(self, sample_dataframe):
        """Test statistics grouped by sampling point."""
        df = add_pollutant_names(sample_dataframe)
        stats = get_pollutant_statistics(df, pollutant="NO2")

        assert len(stats) > 0
        assert "Value_count" in stats.columns
        assert "Value_mean" in stats.columns
        assert "Value_std" in stats.columns

    def test_statistics_all_pollutants(self, sample_dataframe):
        """Test statistics for all pollutants."""
        stats = get_pollutant_statistics(sample_dataframe)
        assert len(stats) > 0

    def test_statistics_sorted_by_mean(self, sample_dataframe):
        """Test that statistics are sorted by mean value."""
        df = add_pollutant_names(sample_dataframe)
        stats = get_pollutant_statistics(df, pollutant="NO2")

        # Check if sorted descending
        means = stats["Value_mean"].values
        assert all(means[i] >= means[i + 1] for i in range(len(means) - 1))


class TestCalculateTemporalPatterns:
    """Test temporal pattern analysis."""

    def test_hourly_patterns(self, sample_dataframe):
        """Test hourly pattern calculation."""
        df = add_pollutant_names(sample_dataframe)
        hourly = calculate_temporal_patterns(df, pollutant="NO2", frequency="hour")

        assert len(hourly) <= 24  # Max 24 hours
        assert hourly.index.name == "Hour"

    def test_daily_patterns(self, sample_dataframe):
        """Test daily pattern calculation."""
        df = add_pollutant_names(sample_dataframe)
        daily = calculate_temporal_patterns(df, pollutant="NO2", frequency="day")

        assert daily.index.name == "Date"

    def test_weekday_patterns(self, sample_dataframe):
        """Test weekday pattern calculation."""
        df = add_pollutant_names(sample_dataframe)
        weekday = calculate_temporal_patterns(df, pollutant="NO2", frequency="weekday")

        assert weekday.index.name == "Weekday"
        # Should have day names
        assert any(day in str(weekday.index.tolist()) for day in ["Monday", "Sunday"])

    def test_invalid_frequency_raises_error(self, sample_dataframe):
        """Test that invalid frequency raises error."""
        df = add_pollutant_names(sample_dataframe)

        with pytest.raises(ValueError):
            calculate_temporal_patterns(df, pollutant="NO2", frequency="invalid")


class TestIdentifyExceedances:
    """Test threshold exceedance detection."""

    def test_identify_no_exceedances(self, sample_dataframe):
        """Test when no values exceed thresholds."""
        df = add_pollutant_names(sample_dataframe)

        # Set very high thresholds
        thresholds = {"NO2": 1000, "PM10": 1000}
        exceedances = identify_exceedances(df, thresholds)

        assert len(exceedances) == 0

    def test_identify_some_exceedances(self, sample_dataframe):
        """Test when some values exceed thresholds."""
        df = add_pollutant_names(sample_dataframe)

        # Set low thresholds
        thresholds = {"NO2": 20, "PM10": 20}
        exceedances = identify_exceedances(df, thresholds)

        if len(exceedances) > 0:
            assert "Threshold" in exceedances.columns
            assert "ExceedanceRatio" in exceedances.columns
            assert all(exceedances["Value"] > exceedances["Threshold"])

    def test_exceedance_ratio_calculation(self):
        """Test that exceedance ratio is correctly calculated."""
        df = pd.DataFrame(
            {
                "PollutantName": ["NO2", "NO2"],
                "Value": [50, 100],
                "Start": pd.to_datetime(["2025-11-16 01:00", "2025-11-16 02:00"]),
            }
        )

        thresholds = {"NO2": 40}
        exceedances = identify_exceedances(df, thresholds)

        assert len(exceedances) == 2
        assert exceedances["ExceedanceRatio"].iloc[0] == 50 / 40
        assert exceedances["ExceedanceRatio"].iloc[1] == 100 / 40


class TestCompareStations:
    """Test station comparison."""

    def test_compare_top_stations(self, sample_dataframe):
        """Test comparison of top stations."""
        df = add_pollutant_names(sample_dataframe)
        comparison = compare_stations(df, pollutant="NO2", top_n=5)

        assert len(comparison) <= 5
        assert "Value_count" in comparison.columns
        assert "Value_mean" in comparison.columns

    def test_compare_by_max(self, sample_dataframe):
        """Test comparison by max value."""
        df = add_pollutant_names(sample_dataframe)
        comparison = compare_stations(df, pollutant="NO2", metric="max")

        # Check sorted by max
        max_values = comparison["Value_max"].values
        assert all(max_values[i] >= max_values[i + 1] for i in range(len(max_values) - 1))

    def test_compare_invalid_metric_raises_error(self, sample_dataframe):
        """Test that invalid metric raises error."""
        df = add_pollutant_names(sample_dataframe)

        with pytest.raises(ValueError):
            compare_stations(df, pollutant="NO2", metric="invalid")
