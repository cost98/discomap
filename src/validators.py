"""
Data Validation Module

Quality checks and validation for air quality data.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .config import Config
from .logger import get_logger

logger = get_logger(__name__, Config.LOGS_DIR)


@dataclass
class ValidationResult:
    """Result of data validation."""

    is_valid: bool
    total_records: int
    valid_records: int
    invalid_records: int
    issues: List[Dict]
    warnings: List[str]

    def __str__(self) -> str:
        status = "VALID" if self.is_valid else "INVALID"
        return (
            f"Validation Result: {status}\n"
            f"Total Records: {self.total_records:,}\n"
            f"Valid: {self.valid_records:,} ({self.valid_records/self.total_records*100:.1f}%)\n"
            f"Invalid: {self.invalid_records:,}\n"
            f"Issues: {len(self.issues)}\n"
            f"Warnings: {len(self.warnings)}"
        )


def validate_schema(df: pd.DataFrame) -> List[str]:
    """
    Validate that DataFrame has required columns with correct types.

    Args:
        df: DataFrame to validate

    Returns:
        List of schema issues (empty if valid)
    """
    required_columns = {
        "Samplingpoint": object,
        "Pollutant": (int, object),
        "Start": "datetime64[ns]",
        "End": "datetime64[ns]",
        "Value": (float, int),
        "Unit": object,
        "AggType": object,
        "Validity": (int, float),
        "Verification": (int, float),
    }

    issues = []

    for column, expected_type in required_columns.items():
        if column not in df.columns:
            issues.append(f"Missing required column: {column}")
        else:
            actual_type = df[column].dtype
            if isinstance(expected_type, tuple):
                type_match = any(np.issubdtype(actual_type, t) for t in expected_type)
            else:
                type_match = np.issubdtype(actual_type, expected_type)

            if not type_match:
                issues.append(f"Column '{column}' has type {actual_type}, expected {expected_type}")

    return issues


def validate_values(df: pd.DataFrame) -> List[Dict]:
    """
    Validate data values for anomalies and inconsistencies.

    Args:
        df: DataFrame to validate

    Returns:
        List of validation issues
    """
    issues = []

    # Check for null values
    null_counts = df.isnull().sum()
    for column, count in null_counts.items():
        if count > 0:
            issues.append(
                {
                    "type": "null_values",
                    "column": column,
                    "count": int(count),
                    "percentage": count / len(df) * 100,
                }
            )

    # Check for negative concentration values
    if "Value" in df.columns:
        negative_values = (df["Value"] < 0).sum()
        if negative_values > 0:
            issues.append(
                {
                    "type": "negative_values",
                    "column": "Value",
                    "count": int(negative_values),
                    "percentage": negative_values / len(df) * 100,
                }
            )

    # Check for unrealistic high values (>1000 for most pollutants)
    if "Value" in df.columns:
        extreme_values = (df["Value"] > 1000).sum()
        if extreme_values > 0:
            issues.append(
                {
                    "type": "extreme_values",
                    "column": "Value",
                    "count": int(extreme_values),
                    "threshold": 1000,
                    "percentage": extreme_values / len(df) * 100,
                }
            )

    # Check for invalid validity flags
    if "Validity" in df.columns:
        valid_flags = [1, 2, 3, -1, -99, 4]
        invalid_flags = ~df["Validity"].isin(valid_flags)
        if invalid_flags.any():
            issues.append(
                {
                    "type": "invalid_validity_flags",
                    "column": "Validity",
                    "count": int(invalid_flags.sum()),
                    "valid_flags": valid_flags,
                }
            )

    # Check for invalid verification flags
    if "Verification" in df.columns:
        valid_flags = [1, 2, 3]
        invalid_flags = ~df["Verification"].isin(valid_flags)
        if invalid_flags.any():
            issues.append(
                {
                    "type": "invalid_verification_flags",
                    "column": "Verification",
                    "count": int(invalid_flags.sum()),
                    "valid_flags": valid_flags,
                }
            )

    # Check date consistency (Start should be before End)
    if "Start" in df.columns and "End" in df.columns:
        invalid_dates = (df["Start"] > df["End"]).sum()
        if invalid_dates > 0:
            issues.append(
                {
                    "type": "invalid_date_order",
                    "columns": ["Start", "End"],
                    "count": int(invalid_dates),
                    "description": "Start date is after End date",
                }
            )

    # Check data capture percentage
    if "DataCapture" in df.columns:
        invalid_capture = ((df["DataCapture"] < 0) | (df["DataCapture"] > 100)).sum()
        if invalid_capture > 0:
            issues.append(
                {
                    "type": "invalid_data_capture",
                    "column": "DataCapture",
                    "count": int(invalid_capture),
                    "valid_range": [0, 100],
                }
            )

    return issues


def validate_completeness(df: pd.DataFrame, expected_hours: Optional[int] = None) -> Dict:
    """
    Validate time series completeness.

    Args:
        df: DataFrame to validate
        expected_hours: Expected number of hours in time series

    Returns:
        Dictionary with completeness statistics
    """
    if "Start" not in df.columns or "Samplingpoint" not in df.columns:
        return {"error": "Missing required columns for completeness check"}

    completeness = {}

    for station in df["Samplingpoint"].unique():
        station_data = df[df["Samplingpoint"] == station]

        if len(station_data) == 0:
            continue

        # Calculate time range
        time_range = station_data["End"].max() - station_data["Start"].min()
        hours_in_range = time_range.total_seconds() / 3600

        # Calculate actual measurements
        actual_measurements = len(station_data)

        if expected_hours:
            expected = expected_hours
        else:
            expected = int(hours_in_range)

        if expected > 0:
            completeness_pct = (actual_measurements / expected) * 100
        else:
            completeness_pct = 100.0

        completeness[station] = {
            "measurements": actual_measurements,
            "expected": expected,
            "completeness_pct": round(completeness_pct, 2),
            "missing": max(0, expected - actual_measurements),
        }

    return completeness


def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check for duplicate records.

    Args:
        df: DataFrame to check

    Returns:
        DataFrame containing duplicate records
    """
    key_columns = ["Samplingpoint", "Pollutant", "Start", "End"]

    # Check if all key columns exist
    missing_cols = [col for col in key_columns if col not in df.columns]
    if missing_cols:
        logger.warning(f"Cannot check duplicates, missing columns: {missing_cols}")
        return pd.DataFrame()

    duplicates = df[df.duplicated(subset=key_columns, keep=False)]

    if len(duplicates) > 0:
        logger.warning(f"Found {len(duplicates)} duplicate records")

    return duplicates


def validate_dataframe(
    df: pd.DataFrame, strict: bool = False, min_data_capture: Optional[float] = None
) -> ValidationResult:
    """
    Comprehensive validation of air quality DataFrame.

    Args:
        df: DataFrame to validate
        strict: If True, any issue marks validation as failed
        min_data_capture: Minimum acceptable data capture percentage

    Returns:
        ValidationResult with detailed findings
    """
    logger.info(f"Validating dataframe with {len(df):,} records (strict={strict})")

    issues = []
    warnings = []

    # Schema validation
    schema_issues = validate_schema(df)
    if schema_issues:
        for issue in schema_issues:
            issues.append({"type": "schema", "description": issue})
        logger.error(f"Schema validation failed: {len(schema_issues)} issues")

    # Value validation
    value_issues = validate_values(df)
    issues.extend(value_issues)

    if value_issues:
        logger.warning(f"Found {len(value_issues)} value validation issues")

    # Check duplicates
    duplicates = check_duplicates(df)
    if len(duplicates) > 0:
        issues.append(
            {
                "type": "duplicates",
                "count": len(duplicates),
                "description": f"Found {len(duplicates)} duplicate records",
            }
        )

    # Data quality warnings
    if "Validity" in df.columns:
        invalid_pct = (df["Validity"] < 0).sum() / len(df) * 100
        if invalid_pct > 20:
            warnings.append(f"High percentage of invalid data: {invalid_pct:.1f}%")

    if min_data_capture and "DataCapture" in df.columns:
        low_capture = (df["DataCapture"] < min_data_capture).sum()
        if low_capture > 0:
            warnings.append(f"{low_capture:,} records below {min_data_capture}% data capture")

    # Count valid records
    valid_records = len(df)
    invalid_records = 0

    if "Validity" in df.columns:
        valid_records = (df["Validity"] > 0).sum()
        invalid_records = len(df) - valid_records

    # Determine if validation passed
    is_valid = True
    if strict and (issues or warnings):
        is_valid = False
    elif not strict and issues:
        # In non-strict mode, only fail on schema issues
        schema_issues = [i for i in issues if i.get("type") == "schema"]
        is_valid = len(schema_issues) == 0

    result = ValidationResult(
        is_valid=is_valid,
        total_records=len(df),
        valid_records=int(valid_records),
        invalid_records=int(invalid_records),
        issues=issues,
        warnings=warnings,
    )

    logger.info(f"Validation complete: {result}")

    return result


def clean_dataframe(
    df: pd.DataFrame,
    remove_invalid: bool = True,
    remove_duplicates: bool = True,
    remove_nulls: bool = False,
) -> pd.DataFrame:
    """
    Clean DataFrame by removing invalid, duplicate, or null records.

    Args:
        df: DataFrame to clean
        remove_invalid: Remove records with negative validity flags
        remove_duplicates: Remove duplicate records
        remove_nulls: Remove records with null values in critical columns

    Returns:
        Cleaned DataFrame
    """
    logger.info(f"Cleaning dataframe ({len(df):,} initial records)")

    cleaned = df.copy()
    initial_count = len(cleaned)

    # Remove invalid data (only negative validity flags indicate invalid data)
    # Validity 0 = unknown/not verified, should be kept
    # Validity > 0 = valid (1=valid, 2=below detection, 3=above detection, etc.)
    # Validity < 0 = invalid (-1=invalid, -2=invalid negative, -99=missing)
    if remove_invalid and "Validity" in cleaned.columns:
        before = len(cleaned)
        cleaned = cleaned[cleaned["Validity"] >= 0]  # Changed from > to >=
        removed = before - len(cleaned)
        if removed > 0:
            logger.info(f"Removed {removed:,} invalid records (Validity < 0)")

    # Remove duplicates
    if remove_duplicates:
        key_columns = ["Samplingpoint", "Pollutant", "Start", "End"]
        key_columns = [col for col in key_columns if col in cleaned.columns]
        before = len(cleaned)
        cleaned = cleaned.drop_duplicates(subset=key_columns, keep="first")
        removed = before - len(cleaned)
        if removed > 0:
            logger.info(f"Removed {removed:,} duplicate records")

    # Remove nulls
    if remove_nulls:
        critical_columns = ["Samplingpoint", "Pollutant", "Start", "Value"]
        critical_columns = [col for col in critical_columns if col in cleaned.columns]
        before = len(cleaned)
        cleaned = cleaned.dropna(subset=critical_columns)
        removed = before - len(cleaned)
        if removed > 0:
            logger.info(f"Removed {removed:,} records with null values")

    total_removed = initial_count - len(cleaned)
    logger.info(
        f"Cleaning complete: {len(cleaned):,} records remaining "
        f"({total_removed:,} removed, {total_removed/initial_count*100:.1f}%)"
    )

    return cleaned
