"""
Tests for Utils Module
"""

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from src.utils import (
    chunk_list,
    ensure_directory,
    format_duration,
    format_file_size,
    generate_date_range,
    get_last_n_days_range,
    safe_divide,
)


class TestFormatters:
    """Test formatting functions."""

    def test_format_file_size_bytes(self):
        """Test file size formatting in bytes."""
        assert "B" in format_file_size(500)

    def test_format_file_size_kilobytes(self):
        """Test file size formatting in kilobytes."""
        result = format_file_size(2048)
        assert "KB" in result

    def test_format_file_size_megabytes(self):
        """Test file size formatting in megabytes."""
        result = format_file_size(5 * 1024 * 1024)
        assert "MB" in result
        assert "5" in result

    def test_format_file_size_gigabytes(self):
        """Test file size formatting in gigabytes."""
        result = format_file_size(2 * 1024 * 1024 * 1024)
        assert "GB" in result

    def test_format_duration_seconds(self):
        """Test duration formatting in seconds."""
        result = format_duration(45)
        assert "s" in result

    def test_format_duration_minutes(self):
        """Test duration formatting in minutes."""
        result = format_duration(120)
        assert "m" in result

    def test_format_duration_hours(self):
        """Test duration formatting in hours."""
        result = format_duration(7200)
        assert "h" in result


class TestDateUtilities:
    """Test date-related utilities."""

    def test_generate_date_range_strings(self):
        """Test date range generation from strings."""
        start, end = generate_date_range("2025-11-16", "2025-11-20")

        assert isinstance(start, str)
        assert isinstance(end, str)
        assert "2025-11-16" in start
        assert "2025-11-20" in end

    def test_generate_date_range_datetime(self):
        """Test date range generation from datetime objects."""
        start_dt = datetime(2025, 11, 16)
        end_dt = datetime(2025, 11, 20)

        start, end = generate_date_range(start_dt, end_dt)

        assert isinstance(start, str)
        assert isinstance(end, str)

    def test_get_last_n_days_range(self):
        """Test last N days range generation."""
        start, end = get_last_n_days_range(7)

        assert isinstance(start, str)
        assert isinstance(end, str)

        # Parse dates
        start_dt = datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
        end_dt = datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ")

        # Should be approximately 7 days apart
        diff = end_dt - start_dt
        assert 6 <= diff.days <= 7


class TestListUtilities:
    """Test list utilities."""

    def test_chunk_list_even(self):
        """Test chunking list with even division."""
        lst = list(range(10))
        chunks = chunk_list(lst, 2)

        assert len(chunks) == 5
        assert all(len(chunk) == 2 for chunk in chunks)

    def test_chunk_list_uneven(self):
        """Test chunking list with uneven division."""
        lst = list(range(10))
        chunks = chunk_list(lst, 3)

        assert len(chunks) == 4
        assert len(chunks[-1]) == 1  # Last chunk has 1 item

    def test_chunk_list_larger_chunk(self):
        """Test chunking with chunk size larger than list."""
        lst = list(range(5))
        chunks = chunk_list(lst, 10)

        assert len(chunks) == 1
        assert chunks[0] == lst


class TestMathUtilities:
    """Test math utilities."""

    def test_safe_divide_normal(self):
        """Test safe division with normal numbers."""
        assert safe_divide(10, 2) == 5.0
        assert safe_divide(7, 2) == 3.5

    def test_safe_divide_by_zero(self):
        """Test safe division by zero returns default."""
        assert safe_divide(10, 0) == 0.0
        assert safe_divide(10, 0, default=99) == 99

    def test_safe_divide_negative(self):
        """Test safe division with negative numbers."""
        assert safe_divide(-10, 2) == -5.0
        assert safe_divide(10, -2) == -5.0


class TestFileUtilities:
    """Test file utilities."""

    def test_ensure_directory_creates(self, tmp_path):
        """Test directory creation."""
        test_dir = tmp_path / "test" / "nested" / "dir"
        result = ensure_directory(test_dir)

        assert result.exists()
        assert result.is_dir()

    def test_ensure_directory_existing(self, tmp_path):
        """Test with existing directory."""
        test_dir = tmp_path / "existing"
        test_dir.mkdir()

        result = ensure_directory(test_dir)
        assert result.exists()

    def test_ensure_directory_returns_path(self, tmp_path):
        """Test that function returns Path object."""
        test_dir = tmp_path / "test"
        result = ensure_directory(test_dir)

        assert isinstance(result, Path)
