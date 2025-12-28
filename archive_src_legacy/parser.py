"""
Parquet File Parser Module

Production-ready functions for reading and processing EEA parquet files.
"""

import tempfile
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd

from .config import Config
from .logger import get_logger
from .pollutant_mapper import add_pollutant_names

logger = get_logger(__name__, Config.LOGS_DIR)


class ParserError(Exception):
    """Custom exception for parsing errors."""

    pass


def extract_zip(zip_path: Union[str, Path], extract_dir: Optional[Path] = None) -> Path:
    """
    Extract zip file containing parquet files.

    Args:
        zip_path: Path to zip file
        extract_dir: Directory to extract to (temp dir if None)

    Returns:
        Path to extraction directory

    Raises:
        ParserError: If extraction fails
    """
    zip_path = Path(zip_path)

    if not zip_path.exists():
        raise ParserError(f"Zip file not found: {zip_path}")

    if extract_dir is None:
        extract_dir = Config.get_temp_dir() / zip_path.stem

    extract_path = Path(extract_dir)
    extract_path.mkdir(parents=True, exist_ok=True)

    logger.info(f"Extracting {zip_path.name}...")

    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)

        parquet_files = list(extract_path.glob("**/*.parquet"))
        logger.info(f"Extracted {len(parquet_files)} parquet files to {extract_path}")

        return extract_path

    except zipfile.BadZipFile as e:
        raise ParserError(f"Invalid zip file: {e}") from e


def read_parquet(
    file_path: Union[str, Path], add_pollutant_name: bool = True, parse_dates: bool = True
) -> pd.DataFrame:
    """
    Read a single parquet file.

    Args:
        file_path: Path to parquet file
        add_pollutant_name: Add human-readable pollutant names
        parse_dates: Convert datetime columns

    Returns:
        DataFrame with air quality data

    Raises:
        ParserError: If reading fails
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise ParserError(f"Parquet file not found: {file_path}")

    logger.debug(f"Reading parquet file: {file_path.name}")

    try:
        df = pd.read_parquet(file_path)

        # Convert datetime columns
        if parse_dates:
            for col in ["Start", "End", "ResultTime"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

        # Add pollutant names
        if add_pollutant_name and "Pollutant" in df.columns:
            df = add_pollutant_names(df)

        logger.debug(f"Loaded {len(df):,} records from {file_path.name}")

        return df

    except Exception as e:
        raise ParserError(f"Failed to read parquet file: {e}") from e


def read_multiple_parquets(
    file_paths: List[Union[str, Path]], add_pollutant_name: bool = True, parse_dates: bool = True
) -> pd.DataFrame:
    """
    Read and combine multiple parquet files.

    Args:
        file_paths: List of parquet file paths
        add_pollutant_name: Add human-readable pollutant names
        parse_dates: Convert datetime columns

    Returns:
        Combined DataFrame

    Raises:
        ParserError: If reading fails
    """
    if not file_paths:
        raise ParserError("No parquet files provided")

    logger.info(f"Reading {len(file_paths)} parquet files...")

    dfs = []
    for file_path in file_paths:
        try:
            df = read_parquet(file_path, add_pollutant_name=False, parse_dates=parse_dates)
            dfs.append(df)
        except ParserError as e:
            logger.warning(f"Skipping file {file_path}: {e}")
            continue

    if not dfs:
        raise ParserError("No valid parquet files could be read")

    combined = pd.concat(dfs, ignore_index=True)

    if add_pollutant_name and "Pollutant" in combined.columns:
        combined = add_pollutant_names(combined)

    logger.info(f"Combined {len(combined):,} total records")

    return combined


def read_from_zip(
    zip_path: Union[str, Path],
    add_pollutant_name: bool = True,
    parse_dates: bool = True,
    cleanup: bool = True,
) -> pd.DataFrame:
    """
    Read all parquet files from a zip archive.

    Args:
        zip_path: Path to zip file
        add_pollutant_name: Add human-readable pollutant names
        parse_dates: Convert datetime columns
        cleanup: Remove extracted files after reading

    Returns:
        Combined DataFrame from all parquet files

    Raises:
        ParserError: If extraction or reading fails
    """
    zip_path = Path(zip_path)

    logger.info(f"Processing zip file: {zip_path.name}")

    # Extract
    extract_dir = extract_zip(zip_path)

    try:
        # Find all parquet files
        parquet_files = list(extract_dir.glob("**/*.parquet"))

        if not parquet_files:
            raise ParserError(f"No parquet files found in {zip_path.name}")

        # Read and combine
        df = read_multiple_parquets(parquet_files, add_pollutant_name, parse_dates)

        return df

    finally:
        # Cleanup if requested
        if cleanup:
            import shutil

            try:
                shutil.rmtree(extract_dir)
                logger.debug(f"Cleaned up temporary files: {extract_dir}")
            except Exception as e:
                logger.warning(f"Failed to cleanup temp files: {e}")


def filter_dataframe(
    df: pd.DataFrame,
    pollutants: Optional[List[str]] = None,
    sampling_points: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    validity: Optional[List[int]] = None,
    verification: Optional[List[int]] = None,
    min_data_capture: Optional[float] = None,
) -> pd.DataFrame:
    """
    Filter air quality data with multiple criteria.

    Args:
        df: Input DataFrame
        pollutants: List of pollutant codes or names
        sampling_points: List of sampling point IDs
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        validity: List of validity flags to include
        verification: List of verification flags to include
        min_data_capture: Minimum data capture percentage

    Returns:
        Filtered DataFrame
    """
    filtered = df.copy()
    initial_count = len(filtered)

    # Filter by pollutants (check both code and name)
    if pollutants:
        mask = pd.Series(False, index=filtered.index)

        if "Pollutant" in filtered.columns:
            # Try as numeric codes
            numeric_pollutants = [p for p in pollutants if str(p).isdigit()]
            if numeric_pollutants:
                mask |= filtered["Pollutant"].isin([int(p) for p in numeric_pollutants])

        if "PollutantName" in filtered.columns:
            # Try as names
            mask |= filtered["PollutantName"].isin(pollutants)

        filtered = filtered[mask]
        logger.debug(f"After pollutant filter: {len(filtered):,} records")

    # Filter by sampling points
    if sampling_points and "Samplingpoint" in filtered.columns:
        filtered = filtered[filtered["Samplingpoint"].isin(sampling_points)]
        logger.debug(f"After sampling point filter: {len(filtered):,} records")

    # Filter by date range
    if start_date and "Start" in filtered.columns:
        filtered = filtered[filtered["Start"] >= pd.to_datetime(start_date)]
        logger.debug(f"After start date filter: {len(filtered):,} records")

    if end_date and "End" in filtered.columns:
        filtered = filtered[filtered["End"] <= pd.to_datetime(end_date)]
        logger.debug(f"After end date filter: {len(filtered):,} records")

    # Filter by validity
    if validity and "Validity" in filtered.columns:
        filtered = filtered[filtered["Validity"].isin(validity)]
        logger.debug(f"After validity filter: {len(filtered):,} records")

    # Filter by verification
    if verification and "Verification" in filtered.columns:
        filtered = filtered[filtered["Verification"].isin(verification)]
        logger.debug(f"After verification filter: {len(filtered):,} records")

    # Filter by data capture
    if min_data_capture and "DataCapture" in filtered.columns:
        filtered = filtered[filtered["DataCapture"] >= min_data_capture]
        logger.debug(f"After data capture filter: {len(filtered):,} records")

    removed = initial_count - len(filtered)
    logger.info(f"Filtering complete: {len(filtered):,} records ({removed:,} removed)")

    return filtered


def export_to_csv(
    df: pd.DataFrame, output_path: Union[str, Path], include_index: bool = False
) -> Path:
    """
    Export DataFrame to CSV file.

    Args:
        df: DataFrame to export
        output_path: Output file path
        include_index: Include DataFrame index in output

    Returns:
        Path to exported file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Exporting {len(df):,} records to {output_path.name}")

    df.to_csv(output_path, index=include_index)

    size_mb = output_path.stat().st_size / 1024 / 1024
    logger.info(f"Export complete: {output_path} ({size_mb:.2f} MB)")

    return output_path


def export_to_parquet(
    df: pd.DataFrame, output_path: Union[str, Path], compression: str = "snappy"
) -> Path:
    """
    Export DataFrame to Parquet file.

    Args:
        df: DataFrame to export
        output_path: Output file path
        compression: Compression codec ('snappy', 'gzip', 'brotli', None)

    Returns:
        Path to exported file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Exporting {len(df):,} records to {output_path.name}")

    df.to_parquet(output_path, compression=compression, index=False)

    size_mb = output_path.stat().st_size / 1024 / 1024
    logger.info(f"Export complete: {output_path} ({size_mb:.2f} MB)")

    return output_path
