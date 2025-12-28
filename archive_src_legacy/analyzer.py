"""
Data Analysis Module

Statistical analysis and insights from air quality data.
"""

from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from .config import VALIDITY_FLAGS, VERIFICATION_FLAGS, Config
from .logger import get_logger

logger = get_logger(__name__, Config.LOGS_DIR)


def get_summary(df: pd.DataFrame) -> Dict:
    """
    Get comprehensive summary statistics of the dataset.

    Args:
        df: DataFrame with air quality data

    Returns:
        Dictionary with summary statistics
    """
    logger.info("Generating dataset summary...")

    summary = {
        "total_records": len(df),
        "date_range": None,
        "pollutants": [],
        "sampling_points": 0,
        "aggregation_types": [],
        "units": [],
        "validity_distribution": {},
        "verification_distribution": {},
        "data_quality": {},
    }

    # Date range
    if "Start" in df.columns and "End" in df.columns:
        summary["date_range"] = {
            "start": df["Start"].min(),
            "end": df["End"].max(),
            "days": (df["End"].max() - df["Start"].min()).days,
        }

    # Pollutants
    if "PollutantName" in df.columns:
        summary["pollutants"] = df["PollutantName"].unique().tolist()
    elif "Pollutant" in df.columns:
        summary["pollutants"] = [str(p) for p in df["Pollutant"].unique()]

    # Sampling points
    if "Samplingpoint" in df.columns:
        summary["sampling_points"] = df["Samplingpoint"].nunique()

    # Aggregation types
    if "AggType" in df.columns:
        summary["aggregation_types"] = df["AggType"].unique().tolist()

    # Units
    if "Unit" in df.columns:
        summary["units"] = df["Unit"].unique().tolist()

    # Validity distribution
    if "Validity" in df.columns:
        validity_counts = df["Validity"].value_counts().to_dict()
        summary["validity_distribution"] = {
            VALIDITY_FLAGS.get(k, f"Unknown_{k}"): v for k, v in validity_counts.items()
        }

    # Verification distribution
    if "Verification" in df.columns:
        verification_counts = df["Verification"].value_counts().to_dict()
        summary["verification_distribution"] = {
            VERIFICATION_FLAGS.get(k, f"Unknown_{k}"): v for k, v in verification_counts.items()
        }

    # Data quality metrics
    if "DataCapture" in df.columns:
        summary["data_quality"] = {
            "mean_data_capture": df["DataCapture"].mean(),
            "median_data_capture": df["DataCapture"].median(),
            "min_data_capture": df["DataCapture"].min(),
            "max_data_capture": df["DataCapture"].max(),
        }

    logger.info(f"Summary complete: {summary['total_records']:,} records analyzed")

    return summary


def print_summary(summary: Dict) -> None:
    """Print formatted summary to console."""
    print("\n" + "=" * 60)
    print("DATASET SUMMARY")
    print("=" * 60)

    print(f"\nTotal Records: {summary['total_records']:,}")

    if summary["date_range"]:
        dr = summary["date_range"]
        print(f"Date Range: {dr['start']} to {dr['end']} ({dr['days']} days)")

    print(f"Sampling Points: {summary['sampling_points']}")
    print(f"Pollutants: {', '.join(summary['pollutants'])}")
    print(f"Aggregation Types: {', '.join(summary['aggregation_types'])}")
    print(f"Units: {', '.join(summary['units'])}")

    if summary["validity_distribution"]:
        print("\nValidity Distribution:")
        for status, count in summary["validity_distribution"].items():
            pct = count / summary["total_records"] * 100
            print(f"  {status}: {count:,} ({pct:.1f}%)")

    if summary["verification_distribution"]:
        print("\nVerification Distribution:")
        for status, count in summary["verification_distribution"].items():
            pct = count / summary["total_records"] * 100
            print(f"  {status}: {count:,} ({pct:.1f}%)")

    if summary["data_quality"]:
        print("\nData Quality:")
        dq = summary["data_quality"]
        print(f"  Mean Data Capture: {dq['mean_data_capture']:.1f}%")
        print(f"  Median Data Capture: {dq['median_data_capture']:.1f}%")


def get_pollutant_statistics(
    df: pd.DataFrame, pollutant: Optional[str] = None, group_by: str = "Samplingpoint"
) -> pd.DataFrame:
    """
    Get statistics for a specific pollutant or all pollutants.

    Args:
        df: DataFrame with air quality data
        pollutant: Specific pollutant to analyze (None for all)
        group_by: Column to group by ('Samplingpoint', 'PollutantName', etc.)

    Returns:
        DataFrame with statistics
    """
    logger.info(f"Calculating pollutant statistics (pollutant={pollutant}, group_by={group_by})")

    data = df.copy()

    # Filter by pollutant if specified
    if pollutant:
        if "PollutantName" in data.columns:
            data = data[data["PollutantName"] == pollutant]
        elif "Pollutant" in data.columns:
            data = data[data["Pollutant"] == pollutant]

    # Calculate statistics
    stats = (
        data.groupby(group_by)
        .agg({"Value": ["count", "mean", "std", "min", "max", "median"], "Start": ["min", "max"]})
        .round(2)
    )

    stats.columns = ["_".join(col).strip() for col in stats.columns.values]
    stats = stats.sort_values("Value_mean", ascending=False)

    logger.info(f"Statistics calculated for {len(stats)} groups")

    return stats


def calculate_temporal_patterns(
    df: pd.DataFrame, pollutant: Optional[str] = None, frequency: str = "hour"
) -> pd.Series:
    """
    Calculate temporal patterns (hourly, daily, monthly averages).

    Args:
        df: DataFrame with air quality data
        pollutant: Specific pollutant to analyze (None for all)
        frequency: 'hour', 'day', 'month', 'weekday'

    Returns:
        Series with average values by time period
    """
    logger.info(f"Calculating temporal patterns (pollutant={pollutant}, frequency={frequency})")

    data = df.copy()

    # Filter by pollutant
    if pollutant:
        if "PollutantName" in data.columns:
            data = data[data["PollutantName"] == pollutant]
        elif "Pollutant" in data.columns:
            data = data[data["Pollutant"] == pollutant]

    if "Start" not in data.columns:
        raise ValueError("DataFrame must have 'Start' column")

    # Group by time period
    if frequency == "hour":
        grouped = data.groupby(data["Start"].dt.hour)["Value"].mean()
        grouped.index.name = "Hour"
    elif frequency == "day":
        grouped = data.groupby(data["Start"].dt.date)["Value"].mean()
        grouped.index.name = "Date"
    elif frequency == "month":
        grouped = data.groupby(data["Start"].dt.month)["Value"].mean()
        grouped.index.name = "Month"
    elif frequency == "weekday":
        grouped = data.groupby(data["Start"].dt.dayofweek)["Value"].mean()
        grouped.index.name = "Weekday"
        # Map to day names
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        grouped.index = grouped.index.map(lambda x: day_names[x])
    else:
        raise ValueError(f"Invalid frequency: {frequency}")

    return grouped


def identify_exceedances(
    df: pd.DataFrame, thresholds: Dict[str, float], pollutant_column: str = "PollutantName"
) -> pd.DataFrame:
    """
    Identify measurements exceeding pollution thresholds.

    Args:
        df: DataFrame with air quality data
        thresholds: Dict mapping pollutant names to threshold values
        pollutant_column: Column name for pollutants

    Returns:
        DataFrame with only exceedance records
    """
    logger.info(f"Identifying exceedances for {len(thresholds)} pollutants")

    if pollutant_column not in df.columns:
        raise ValueError(f"Column '{pollutant_column}' not found in DataFrame")

    exceedances = []

    for pollutant, threshold in thresholds.items():
        pollutant_data = df[df[pollutant_column] == pollutant]
        exceeded = pollutant_data[pollutant_data["Value"] > threshold].copy()
        exceeded["Threshold"] = threshold
        exceeded["ExceedanceRatio"] = exceeded["Value"] / threshold
        exceedances.append(exceeded)

    if not exceedances:
        logger.info("No exceedances found")
        return pd.DataFrame()

    result = pd.concat(exceedances, ignore_index=True)
    logger.info(f"Found {len(result):,} exceedances")

    return result


def calculate_trends(
    df: pd.DataFrame, pollutant: str, sampling_point: Optional[str] = None, period: str = "D"
) -> Tuple[pd.Series, Dict]:
    """
    Calculate trends over time for a pollutant.

    Args:
        df: DataFrame with air quality data
        pollutant: Pollutant to analyze
        sampling_point: Specific sampling point (None for all)
        period: Pandas period string ('D'=daily, 'W'=weekly, 'M'=monthly)

    Returns:
        Tuple of (time series, trend statistics)
    """
    logger.info(f"Calculating trends for {pollutant}")

    data = df.copy()

    # Filter by pollutant
    if "PollutantName" in data.columns:
        data = data[data["PollutantName"] == pollutant]
    elif "Pollutant" in data.columns:
        data = data[data["Pollutant"] == pollutant]

    # Filter by sampling point
    if sampling_point and "Samplingpoint" in data.columns:
        data = data[data["Samplingpoint"] == sampling_point]

    if len(data) == 0:
        raise ValueError(f"No data found for pollutant: {pollutant}")

    # Resample to desired period
    data = data.set_index("Start")
    resampled = data["Value"].resample(period).mean()

    # Calculate trend statistics
    x = np.arange(len(resampled))
    y = resampled.values

    # Remove NaN values
    mask = ~np.isnan(y)
    x_clean = x[mask]
    y_clean = y[mask]

    if len(x_clean) > 1:
        # Linear regression
        slope, intercept = np.polyfit(x_clean, y_clean, 1)

        trend_stats = {
            "slope": slope,
            "intercept": intercept,
            "mean": np.mean(y_clean),
            "std": np.std(y_clean),
            "min": np.min(y_clean),
            "max": np.max(y_clean),
            "trend": "increasing" if slope > 0 else "decreasing",
            "periods": len(resampled),
        }
    else:
        trend_stats = {"error": "Insufficient data for trend analysis"}

    return resampled, trend_stats


def compare_stations(
    df: pd.DataFrame, pollutant: str, top_n: int = 10, metric: str = "mean"
) -> pd.DataFrame:
    """
    Compare stations by pollutant concentrations.

    Args:
        df: DataFrame with air quality data
        pollutant: Pollutant to compare
        top_n: Number of top stations to return
        metric: Comparison metric ('mean', 'max', 'median')

    Returns:
        DataFrame with top stations
    """
    logger.info(f"Comparing stations for {pollutant} (top {top_n}, metric={metric})")

    data = df.copy()

    # Filter by pollutant
    if "PollutantName" in data.columns:
        data = data[data["PollutantName"] == pollutant]
    elif "Pollutant" in data.columns:
        data = data[data["Pollutant"] == pollutant]

    # Group by station
    comparison = (
        data.groupby("Samplingpoint")
        .agg({"Value": ["count", "mean", "median", "max", "std"], "Start": ["min", "max"]})
        .round(2)
    )

    comparison.columns = ["_".join(col).strip() for col in comparison.columns.values]

    # Sort by metric
    sort_col = f"Value_{metric}"
    if sort_col not in comparison.columns:
        raise ValueError(f"Invalid metric: {metric}")

    comparison = comparison.sort_values(sort_col, ascending=False).head(top_n)

    return comparison
