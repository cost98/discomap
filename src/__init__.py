"""
DiscoMap - EEA Air Quality Data Processing Package

A production-ready toolkit for downloading, processing, and analyzing
air quality data from the European Environment Agency (EEA).
"""

__version__ = "0.1.0"
__author__ = "Cosimo"

from .analyzer import (
    calculate_temporal_patterns,
    calculate_trends,
    compare_stations,
    get_pollutant_statistics,
    get_summary,
    identify_exceedances,
    print_summary,
)

# Core modules
from .config import Config
from .downloader import DownloadError, EEADownloader
from .logger import get_logger
from .parser import (
    ParserError,
    export_to_csv,
    export_to_parquet,
    filter_dataframe,
    read_from_zip,
    read_multiple_parquets,
    read_parquet,
)
from .pollutant_mapper import (
    POLLUTANT_CODES,
    POLLUTANT_NAMES,
    add_pollutant_names,
    get_pollutant_code,
    get_pollutant_name,
)
from .utils import (
    format_duration,
    format_file_size,
    generate_date_range,
    get_last_n_days_range,
    print_dataframe_info,
)
from .validators import ValidationResult, check_duplicates, clean_dataframe, validate_dataframe

# Public API
__all__ = [
    # Config
    "Config",
    # Downloader
    "EEADownloader",
    "DownloadError",
    # Parser
    "read_parquet",
    "read_multiple_parquets",
    "read_from_zip",
    "filter_dataframe",
    "export_to_csv",
    "export_to_parquet",
    "ParserError",
    # Analyzer
    "get_summary",
    "print_summary",
    "get_pollutant_statistics",
    "calculate_temporal_patterns",
    "identify_exceedances",
    "calculate_trends",
    "compare_stations",
    # Validators
    "validate_dataframe",
    "clean_dataframe",
    "check_duplicates",
    "ValidationResult",
    # Pollutant Mapper
    "POLLUTANT_CODES",
    "POLLUTANT_NAMES",
    "get_pollutant_name",
    "get_pollutant_code",
    "add_pollutant_names",
    # Utilities
    "get_logger",
    "format_file_size",
    "format_duration",
    "generate_date_range",
    "get_last_n_days_range",
    "print_dataframe_info",
]

# Initialize configuration
Config.ensure_directories()
