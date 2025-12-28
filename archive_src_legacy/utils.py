"""
Utility Functions

Helper functions for common operations.
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

from .logger import get_logger

logger = get_logger(__name__)


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2h 30m")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.2f}h"


def generate_date_range(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    format: str = "%Y-%m-%dT%H:%M:%SZ",
) -> tuple:
    """
    Generate properly formatted date range for API calls.

    Args:
        start_date: Start date (string or datetime)
        end_date: End date (string or datetime)
        format: Output format

    Returns:
        Tuple of (start_string, end_string)
    """
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)

    return (start_date.strftime(format), end_date.strftime(format))


def get_last_n_days_range(n_days: int, format: str = "%Y-%m-%dT%H:%M:%SZ") -> tuple:
    """
    Get date range for last N days.

    Args:
        n_days: Number of days
        format: Output format

    Returns:
        Tuple of (start_string, end_string)
    """
    end = datetime.now()
    start = end - timedelta(days=n_days)

    return (start.strftime(format), end.strftime(format))


def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """
    Split list into chunks of specified size.

    Args:
        lst: List to split
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]


def safe_divide(a: float, b: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if division by zero.

    Args:
        a: Numerator
        b: Denominator
        default: Default value if b is zero

    Returns:
        Result of division or default
    """
    return a / b if b != 0 else default


def get_file_age_days(file_path: Path) -> float:
    """
    Get age of file in days.

    Args:
        file_path: Path to file

    Returns:
        Age in days
    """
    if not file_path.exists():
        return -1

    modified_time = datetime.fromtimestamp(file_path.stat().st_mtime)
    age = datetime.now() - modified_time
    return age.total_seconds() / 86400  # Convert to days


def ensure_directory(path: Union[str, Path]) -> Path:
    """
    Ensure directory exists, create if it doesn't.

    Args:
        path: Directory path

    Returns:
        Path object
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def cleanup_old_files(directory: Path, days: int = 7, pattern: str = "*.zip") -> int:
    """
    Remove files older than specified days.

    Args:
        directory: Directory to clean
        days: Age threshold in days
        pattern: File pattern to match

    Returns:
        Number of files removed
    """
    if not directory.exists():
        return 0

    removed = 0
    for file in directory.glob(pattern):
        if get_file_age_days(file) > days:
            try:
                file.unlink()
                removed += 1
                logger.info(f"Removed old file: {file.name}")
            except Exception as e:
                logger.warning(f"Failed to remove {file.name}: {e}")

    if removed > 0:
        logger.info(f"Cleaned up {removed} old files from {directory}")

    return removed


def merge_dataframes_smart(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge multiple dataframes, handling duplicates intelligently.

    Args:
        dfs: List of DataFrames

    Returns:
        Merged DataFrame
    """
    if not dfs:
        return pd.DataFrame()

    if len(dfs) == 1:
        return dfs[0]

    # Concatenate
    merged = pd.concat(dfs, ignore_index=True)

    # Remove exact duplicates
    initial_count = len(merged)
    merged = merged.drop_duplicates()

    # Remove duplicates based on key columns if they exist
    key_columns = ["Samplingpoint", "Pollutant", "Start", "End"]
    existing_keys = [col for col in key_columns if col in merged.columns]

    if existing_keys:
        merged = merged.drop_duplicates(subset=existing_keys, keep="last")

    removed = initial_count - len(merged)
    if removed > 0:
        logger.info(f"Merged {len(dfs)} dataframes, removed {removed:,} duplicates")

    return merged


def print_dataframe_info(df: pd.DataFrame, name: str = "DataFrame") -> None:
    """
    Print useful information about a DataFrame.

    Args:
        df: DataFrame to analyze
        name: Name to display
    """
    print(f"\n{'='*60}")
    print(f"{name} Information")
    print(f"{'='*60}")
    print(f"Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")
    print(f"Memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    print(f"\nColumns: {', '.join(df.columns.tolist())}")
    print(f"\nData types:")
    print(df.dtypes)
    print(f"\nNull counts:")
    null_counts = df.isnull().sum()
    print(null_counts[null_counts > 0] if null_counts.any() else "None")
    print(f"{'='*60}\n")
