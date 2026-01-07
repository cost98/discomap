"""
Unit tests for ParquetParser.

These tests also serve as usage examples.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.services.parquet_parser import ParquetParser


@pytest.fixture
def sample_eea_dataframe():
    """Sample EEA data structure (minimal)."""
    return pd.DataFrame({
        # Station fields
        "AirQualityStationEoICode": ["IT0001", "IT0001", "IT0002"],
        "Countrycode": ["IT", "IT", "IT"],
        "AirQualityStationName": ["Milano Centro", "Milano Centro", "Roma Eur"],
        "AirQualityStationType": ["traffic", "traffic", "background"],
        "AirQualityStationArea": ["urban", "urban", "suburban"],
        "Latitude": [45.4642, 45.4642, 41.8902],
        "Longitude": [9.1900, 9.1900, 12.4922],
        "Altitude": [122.0, 122.0, 20.0],
        "Municipality": ["Milano", "Milano", "Roma"],
        
        # Sampling point fields
        "SamplingPoint": ["IT/SPO.IT0001_8_100", "IT/SPO.IT0001_8_100", "IT/SPO.IT0002_10_100"],
        "AirPollutantCode": [8, 8, 10],
        
        # Measurement fields
        "DatetimeBegin": [
            datetime(2024, 1, 1, 0, 0),
            datetime(2024, 1, 1, 1, 0),
            datetime(2024, 1, 1, 0, 0),
        ],
        "Concentration": [25.5, 28.3, 42.1],
        "UnitOfMeasurement": ["µg/m³", "µg/m³", "µg/m³"],
        "AggregationType": ["hour", "hour", "hour"],
        "Validity": [1, 1, 1],
        "Verification": [2, 2, 2],
        "DataCapture": [95.5, 95.5, 98.2],
    })


class TestParquetParser:
    """Test ParquetParser functionality."""
    
    def test_init(self):
        """Test parser initialization."""
        parser = ParquetParser()
        assert parser is not None
        assert hasattr(parser, "COLUMN_MAPPING")
    
    def test_parse_stations(self, sample_eea_dataframe):
        """
        Test station extraction.
        
        Example usage:
            parser = ParquetParser()
            stations = parser.parse_stations(df)
        """
        parser = ParquetParser()
        stations = parser.parse_stations(sample_eea_dataframe)
        
        # Should extract 2 unique stations
        assert len(stations) == 2
        
        # Check first station
        station1 = next(s for s in stations if s["station_code"] == "IT0001")
        assert station1["country_code"] == "IT"
        assert station1["station_name"] == "Milano Centro"
        assert station1["station_type"] == "traffic"
        assert station1["latitude"] == 45.4642
        assert station1["longitude"] == 9.1900
        assert station1["altitude"] == 122.0
        assert station1["municipality"] == "Milano"
    
    def test_parse_sampling_points(self, sample_eea_dataframe):
        """
        Test sampling point extraction.
        
        Example usage:
            parser = ParquetParser()
            sampling_points = parser.parse_sampling_points(df)
        """
        parser = ParquetParser()
        sampling_points = parser.parse_sampling_points(sample_eea_dataframe)
        
        # Should extract 2 unique sampling points
        assert len(sampling_points) == 2
        
        # Check first sampling point
        sp1 = next(sp for sp in sampling_points if sp["sampling_point_id"] == "IT/SPO.IT0001_8_100")
        assert sp1["station_code"] == "IT0001"
        assert sp1["country_code"] == "IT"
        assert sp1["pollutant_code"] == 8
    
    def test_parse_measurements(self, sample_eea_dataframe):
        """
        Test measurement extraction.
        
        Example usage:
            parser = ParquetParser()
            measurements = parser.parse_measurements(df)
        """
        parser = ParquetParser()
        measurements = parser.parse_measurements(sample_eea_dataframe)
        
        # Should extract 3 measurements
        assert len(measurements) == 3
        
        # Check first measurement
        meas1 = measurements[0]
        assert meas1["time"] == datetime(2024, 1, 1, 0, 0)
        assert meas1["sampling_point_id"] == "IT/SPO.IT0001_8_100"
        assert meas1["pollutant_code"] == 8
        assert meas1["value"] == 25.5
        assert meas1["unit"] == "µg/m³"
        assert meas1["aggregation_type"] == "hour"
        assert meas1["validity"] == 1
        assert meas1["verification"] == 2
        assert meas1["data_capture"] == 95.5
    
    def test_parse_all(self, sample_eea_dataframe, tmp_path):
        """
        Test complete parsing workflow.
        
        Example usage:
            parser = ParquetParser()
            data = parser.parse_all(Path("file.parquet"))
            
            print(f"Stations: {len(data['stations'])}")
            print(f"Sampling Points: {len(data['sampling_points'])}")
            print(f"Measurements: {len(data['measurements'])}")
        """
        # Create temporary parquet file
        parquet_file = tmp_path / "test.parquet"
        sample_eea_dataframe.to_parquet(parquet_file)
        
        parser = ParquetParser()
        data = parser.parse_all(parquet_file)
        
        # Check structure
        assert "stations" in data
        assert "sampling_points" in data
        assert "measurements" in data
        
        # Check counts
        assert len(data["stations"]) == 2
        assert len(data["sampling_points"]) == 2
        assert len(data["measurements"]) == 3
    
    def test_parse_datetime(self):
        """Test datetime parsing helper."""
        parser = ParquetParser()
        
        # String datetime
        dt = parser._parse_datetime("2024-01-01 12:00:00")
        assert dt == datetime(2024, 1, 1, 12, 0, 0)
        
        # Already datetime
        dt_obj = datetime(2024, 1, 1, 12, 0, 0)
        assert parser._parse_datetime(dt_obj) == dt_obj
        
        # None/NaN
        assert parser._parse_datetime(None) is None
        assert parser._parse_datetime(pd.NA) is None
    
    def test_column_mapping(self):
        """Test column mapping completeness."""
        parser = ParquetParser()
        
        # Ensure mapping has essential fields
        assert "AirQualityStationEoICode" in parser.COLUMN_MAPPING
        assert "DatetimeBegin" in parser.COLUMN_MAPPING
        assert "Concentration" in parser.COLUMN_MAPPING
        assert "Validity" in parser.COLUMN_MAPPING
        
        # Check mapped values
        assert parser.COLUMN_MAPPING["AirQualityStationEoICode"] == "station_code"
        assert parser.COLUMN_MAPPING["DatetimeBegin"] == "time"
        assert parser.COLUMN_MAPPING["Concentration"] == "value"
    
    def test_missing_columns_handling(self):
        """Test handling of missing optional columns."""
        # Minimal dataframe with only required fields
        minimal_df = pd.DataFrame({
            "AirQualityStationEoICode": ["IT0001"],
            "Countrycode": ["IT"],
            "SamplingPoint": ["IT/SPO.IT0001_8_100"],
            "AirPollutantCode": [8],
            "DatetimeBegin": [datetime(2024, 1, 1)],
            "Concentration": [25.5],
        })
        
        parser = ParquetParser()
        
        # Should not raise errors
        stations = parser.parse_stations(minimal_df)
        assert len(stations) == 1
        assert "station_code" in stations[0]
        
        sampling_points = parser.parse_sampling_points(minimal_df)
        assert len(sampling_points) == 1
        
        measurements = parser.parse_measurements(minimal_df)
        assert len(measurements) == 1
    
    def test_duplicate_handling(self, sample_eea_dataframe):
        """Test deduplication of stations and sampling points."""
        parser = ParquetParser()
        
        # Stations should be deduplicated
        stations = parser.parse_stations(sample_eea_dataframe)
        station_codes = [s["station_code"] for s in stations]
        assert len(station_codes) == len(set(station_codes))  # No duplicates
        
        # Sampling points should be deduplicated
        sampling_points = parser.parse_sampling_points(sample_eea_dataframe)
        sp_ids = [sp["sampling_point_id"] for sp in sampling_points]
        assert len(sp_ids) == len(set(sp_ids))  # No duplicates


class TestParquetParserRealWorld:
    """Tests with more realistic scenarios."""
    
    def test_large_dataset_simulation(self):
        """Test parser with larger dataset (performance check)."""
        # Create 10K measurements
        n_rows = 10_000
        large_df = pd.DataFrame({
            "AirQualityStationEoICode": [f"IT{i % 100:04d}" for i in range(n_rows)],
            "Countrycode": ["IT"] * n_rows,
            "SamplingPoint": [f"IT/SPO.IT{i % 100:04d}_8_100" for i in range(n_rows)],
            "AirPollutantCode": [8] * n_rows,
            "DatetimeBegin": [datetime(2024, 1, 1, i % 24) for i in range(n_rows)],
            "Concentration": [20.0 + (i % 50) for i in range(n_rows)],
        })
        
        parser = ParquetParser()
        
        # Should handle without errors
        measurements = parser.parse_measurements(large_df)
        assert len(measurements) == n_rows
        
        # Stations should be deduplicated to 100
        stations = parser.parse_stations(large_df)
        assert len(stations) == 100
    
    def test_null_values_handling(self):
        """Test handling of null/NaN values."""
        df_with_nulls = pd.DataFrame({
            "AirQualityStationEoICode": ["IT0001", "IT0002"],
            "Countrycode": ["IT", None],  # Null country
            "SamplingPoint": ["IT/SPO.IT0001_8_100", "IT/SPO.IT0002_8_100"],
            "AirPollutantCode": [8, 8],
            "DatetimeBegin": [datetime(2024, 1, 1), datetime(2024, 1, 1)],
            "Concentration": [25.5, None],  # Null concentration
            "Latitude": [45.0, None],  # Null latitude
        })
        
        parser = ParquetParser()
        
        # Parse should handle nulls gracefully
        stations = parser.parse_stations(df_with_nulls)
        assert len(stations) == 2
        
        measurements = parser.parse_measurements(df_with_nulls)
        # Second measurement might be excluded or have None value
        assert len(measurements) >= 1
