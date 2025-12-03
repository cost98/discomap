"""
Tests for Validators Module
"""

import numpy as np
import pandas as pd
import pytest
from src.validators import (
    ValidationResult,
    check_duplicates,
    clean_dataframe,
    validate_dataframe,
    validate_schema,
    validate_values,
)


class TestValidateSchema:
    """Test schema validation."""

    def test_valid_schema(self, sample_dataframe):
        """Test that valid schema passes."""
        issues = validate_schema(sample_dataframe)
        assert len(issues) == 0

    def test_missing_required_column(self, sample_dataframe):
        """Test detection of missing columns."""
        df_incomplete = sample_dataframe.drop(columns=["Pollutant"])
        issues = validate_schema(df_incomplete)

        assert len(issues) > 0
        assert any("Pollutant" in issue for issue in issues)

    def test_wrong_column_type(self):
        """Test detection of wrong column types."""
        df = pd.DataFrame(
            {
                "Samplingpoint": ["IT/SPO.IT0001A"],
                "Pollutant": ["8"],  # Should be int
                "Start": ["2025-11-16"],  # Should be datetime
                "End": ["2025-11-17"],
                "Value": [25.5],
                "Unit": ["ug.m-3"],
                "AggType": ["hour"],
                "Validity": [1],
                "Verification": [1],
            }
        )

        issues = validate_schema(df)
        assert len(issues) > 0


class TestValidateValues:
    """Test value validation."""

    def test_detect_null_values(self):
        """Test detection of null values."""
        df = pd.DataFrame({"Samplingpoint": ["IT/SPO.IT0001A", None], "Value": [25.5, np.nan]})

        issues = validate_values(df)
        null_issues = [i for i in issues if i["type"] == "null_values"]
        assert len(null_issues) > 0

    def test_detect_negative_values(self, sample_dataframe):
        """Test detection of negative concentration values."""
        df = sample_dataframe.copy()
        df.loc[0, "Value"] = -10

        issues = validate_values(df)
        negative_issues = [i for i in issues if i["type"] == "negative_values"]
        assert len(negative_issues) > 0
        assert negative_issues[0]["count"] >= 1

    def test_detect_extreme_values(self, sample_dataframe):
        """Test detection of unrealistic high values."""
        df = sample_dataframe.copy()
        df.loc[0, "Value"] = 2000

        issues = validate_values(df)
        extreme_issues = [i for i in issues if i["type"] == "extreme_values"]
        assert len(extreme_issues) > 0

    def test_detect_invalid_validity_flags(self, sample_dataframe):
        """Test detection of invalid validity flags."""
        df = sample_dataframe.copy()
        df.loc[0, "Validity"] = 999  # Invalid flag

        issues = validate_values(df)
        validity_issues = [i for i in issues if i["type"] == "invalid_validity_flags"]
        assert len(validity_issues) > 0

    def test_detect_invalid_date_order(self):
        """Test detection of Start > End."""
        df = pd.DataFrame(
            {
                "Start": pd.to_datetime(["2025-11-16 02:00"]),
                "End": pd.to_datetime(["2025-11-16 01:00"]),  # Before Start
            }
        )

        issues = validate_values(df)
        date_issues = [i for i in issues if i["type"] == "invalid_date_order"]
        assert len(date_issues) > 0


class TestCheckDuplicates:
    """Test duplicate detection."""

    def test_no_duplicates(self, sample_dataframe):
        """Test DataFrame with no duplicates."""
        duplicates = check_duplicates(sample_dataframe)
        assert len(duplicates) == 0

    def test_detect_duplicates(self, sample_dataframe_with_issues):
        """Test detection of duplicate records."""
        duplicates = check_duplicates(sample_dataframe_with_issues)
        assert len(duplicates) > 0


class TestValidateDataframe:
    """Test comprehensive validation."""

    def test_validate_clean_dataframe(self, sample_dataframe):
        """Test validation of clean DataFrame."""
        result = validate_dataframe(sample_dataframe, strict=False)

        assert isinstance(result, ValidationResult)
        assert result.total_records == len(sample_dataframe)
        assert result.valid_records > 0

    def test_validate_problematic_dataframe(self, sample_dataframe_with_issues):
        """Test validation of DataFrame with issues."""
        result = validate_dataframe(sample_dataframe_with_issues, strict=False)

        assert len(result.issues) > 0
        assert result.invalid_records > 0

    def test_strict_validation(self, sample_dataframe_with_issues):
        """Test strict validation mode."""
        result = validate_dataframe(sample_dataframe_with_issues, strict=True)

        # Strict mode should fail with any issues
        if result.issues or result.warnings:
            assert not result.is_valid

    def test_validation_result_string(self, sample_dataframe):
        """Test ValidationResult string representation."""
        result = validate_dataframe(sample_dataframe)
        result_str = str(result)

        assert "Total Records" in result_str
        assert "Valid:" in result_str
        assert "Invalid:" in result_str


class TestCleanDataframe:
    """Test data cleaning."""

    def test_remove_invalid_data(self):
        """Test removal of invalid records."""
        df = pd.DataFrame(
            {
                "Samplingpoint": ["IT/SPO.IT0001A", "IT/SPO.IT0002A"],
                "Pollutant": [8, 8],
                "Start": pd.to_datetime(["2025-11-16 01:00", "2025-11-16 02:00"]),
                "End": pd.to_datetime(["2025-11-16 02:00", "2025-11-16 03:00"]),
                "Value": [25.5, 30.0],
                "Unit": ["ug.m-3", "ug.m-3"],
                "AggType": ["hour", "hour"],
                "Validity": [1, -1],  # Second is invalid
                "Verification": [1, 1],
                "ResultTime": [pd.Timestamp.now(), pd.Timestamp.now()],
                "DataCapture": [95.0, 95.0],
                "FkObservationLog": ["uuid1", "uuid2"],
            }
        )

        cleaned = clean_dataframe(df, remove_invalid=True)
        assert len(cleaned) == 1
        assert all(cleaned["Validity"] > 0)

    def test_remove_duplicates(self, sample_dataframe_with_issues):
        """Test removal of duplicate records."""
        cleaned = clean_dataframe(
            sample_dataframe_with_issues, remove_invalid=False, remove_duplicates=True
        )

        # Should remove duplicates
        assert len(cleaned) < len(sample_dataframe_with_issues)

    def test_remove_nulls(self):
        """Test removal of records with null values."""
        df = pd.DataFrame(
            {
                "Samplingpoint": ["IT/SPO.IT0001A", None, "IT/SPO.IT0003A"],
                "Pollutant": [8, 8, 8],
                "Start": pd.to_datetime(
                    ["2025-11-16 01:00", "2025-11-16 02:00", "2025-11-16 03:00"]
                ),
                "Value": [25.5, 30.0, 35.0],
            }
        )

        cleaned = clean_dataframe(df, remove_nulls=True, remove_invalid=False)
        assert len(cleaned) == 2
        assert not cleaned["Samplingpoint"].isnull().any()

    def test_clean_preserves_valid_data(self, sample_dataframe):
        """Test that cleaning preserves valid data."""
        initial_valid = (sample_dataframe["Validity"] > 0).sum()
        cleaned = clean_dataframe(sample_dataframe, remove_invalid=True)

        assert len(cleaned) == initial_valid

    def test_clean_all_options(self, sample_dataframe_with_issues):
        """Test cleaning with all options enabled."""
        cleaned = clean_dataframe(
            sample_dataframe_with_issues,
            remove_invalid=True,
            remove_duplicates=True,
            remove_nulls=True,
        )

        # Should be significantly smaller
        assert len(cleaned) < len(sample_dataframe_with_issues)

        # Should have no nulls in critical columns
        critical_cols = ["Samplingpoint", "Pollutant", "Start", "Value"]
        existing_critical = [c for c in critical_cols if c in cleaned.columns]
        assert not cleaned[existing_critical].isnull().any().any()
