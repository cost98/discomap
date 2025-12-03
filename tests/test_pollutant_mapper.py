"""
Tests for Pollutant Mapper Module
"""

import pandas as pd
import pytest
from src.pollutant_mapper import (
    POLLUTANT_CODES,
    POLLUTANT_NAMES,
    add_pollutant_names,
    get_pollutant_code,
    get_pollutant_name,
)


class TestPollutantMapper:
    """Test pollutant code mapping functions."""

    def test_pollutant_codes_defined(self):
        """Test that common pollutants are defined."""
        assert 8 in POLLUTANT_CODES
        assert POLLUTANT_CODES[8] == "NO2"
        assert 5 in POLLUTANT_CODES
        assert POLLUTANT_CODES[5] == "PM10"
        assert 7 in POLLUTANT_CODES
        assert POLLUTANT_CODES[7] == "O3"

    def test_pollutant_names_reverse_mapping(self):
        """Test reverse mapping from names to codes."""
        assert "NO2" in POLLUTANT_NAMES
        assert POLLUTANT_NAMES["NO2"] == 8
        assert "PM10" in POLLUTANT_NAMES

    def test_get_pollutant_name_valid_code(self):
        """Test getting name from valid code."""
        assert get_pollutant_name(8) == "NO2"
        assert get_pollutant_name(5) == "PM10"
        assert get_pollutant_name(7) == "O3"

    def test_get_pollutant_name_invalid_code(self):
        """Test getting name from invalid code."""
        result = get_pollutant_name(9999)
        assert "Unknown" in result
        assert "9999" in result

    def test_get_pollutant_code_valid_name(self):
        """Test getting code from valid name."""
        assert get_pollutant_code("NO2") == 8
        # Note: PM10 has multiple codes (5 and 5012), reverse mapping returns the last one
        assert get_pollutant_code("PM10") in [5, 5012]
        assert get_pollutant_code("O3") == 7

    def test_get_pollutant_code_case_insensitive(self):
        """Test that name lookup is case insensitive."""
        assert get_pollutant_code("no2") == 8
        assert get_pollutant_code("No2") == 8
        assert get_pollutant_code("NO2") == 8

    def test_get_pollutant_code_invalid_name(self):
        """Test getting code from invalid name."""
        assert get_pollutant_code("INVALID") is None

    def test_add_pollutant_names_to_dataframe(self):
        """Test adding pollutant names to DataFrame."""
        df = pd.DataFrame({"Pollutant": [8, 5, 7, 8], "Value": [25, 30, 45, 28]})

        df_with_names = add_pollutant_names(df)

        assert "PollutantName" in df_with_names.columns
        assert df_with_names["PollutantName"].iloc[0] == "NO2"
        assert df_with_names["PollutantName"].iloc[1] == "PM10"
        assert df_with_names["PollutantName"].iloc[2] == "O3"

    def test_add_pollutant_names_preserves_original(self):
        """Test that original DataFrame is not modified."""
        df = pd.DataFrame({"Pollutant": [8, 5]})
        df_with_names = add_pollutant_names(df)

        assert "PollutantName" not in df.columns
        assert "PollutantName" in df_with_names.columns

    def test_add_pollutant_names_unknown_codes(self):
        """Test handling of unknown pollutant codes."""
        df = pd.DataFrame({"Pollutant": [9999, 8888]})
        df_with_names = add_pollutant_names(df)

        assert "Unknown" in df_with_names["PollutantName"].iloc[0]
