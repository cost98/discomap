"""Parquet Parser for EEA Air Quality Data.

Parses Parquet files downloaded from EEA and converts them to database-ready format.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow.parquet as pq

from src.logger import get_logger

logger = get_logger(__name__)


class ParquetParser:
    """Parse EEA Parquet files and prepare data for database insertion."""

    # Mapping colonne Parquet EEA → Database fields
    COLUMN_MAPPING = {
        # Station fields
        "AirQualityStationEoICode": "station_code",
        "Countrycode": "country_code",
        "AirQualityStationName": "station_name",
        "AirQualityStationType": "station_type",
        "AirQualityStationArea": "area_type",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Altitude": "altitude",
        "Municipality": "municipality",
        "AirQualityStationStartDate": "start_date",
        "AirQualityStationEndDate": "end_date",
        
        # Sampling Point fields
        "SamplingPoint": "sampling_point_id",
        "AirPollutantCode": "pollutant_code",
        "SamplingPointStartDate": "sp_start_date",
        "SamplingPointEndDate": "sp_end_date",
        
        # Measurement fields
        "DatetimeBegin": "time",
        "DatetimeEnd": "datetime_end",
        "Concentration": "value",
        "UnitOfMeasurement": "unit",
        "AggregationType": "aggregation_type",
        "Validity": "validity",
        "Verification": "verification",
        "DataCapture": "data_capture",
        "ResultTime": "result_time",
        "ObservationId": "observation_id",
    }

    def __init__(self):
        """Initialize parser."""
        logger.info("ParquetParser initialized")

    def read_parquet(self, filepath: Path) -> pd.DataFrame:
        """
        Read Parquet file into DataFrame.
        
        Args:
            filepath: Path to Parquet file
            
        Returns:
            DataFrame with raw data
        """
        logger.info(f"Reading Parquet file: {filepath}")
        
        # Leggi con PyArrow per performance
        table = pq.read_table(filepath)
        df = table.to_pandas()
        
        logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        logger.debug(f"Columns: {df.columns.tolist()}")
        
        return df

    def parse_stations(self, df: pd.DataFrame) -> List[Dict]:
        """
        Extract unique stations from DataFrame.
        
        Args:
            df: DataFrame with EEA data
            
        Returns:
            List of station dictionaries ready for StationRepository
        """
        logger.info("Extracting stations...")
        
        # Colonne necessarie per Station
        station_cols = [
            "AirQualityStationEoICode",
            "Countrycode", 
            "AirQualityStationName",
            "AirQualityStationType",
            "AirQualityStationArea",
            "Latitude",
            "Longitude",
            "Altitude",
            "Municipality",
        ]
        
        # Seleziona e rinomina
        available_cols = [col for col in station_cols if col in df.columns]
        df_stations = df[available_cols].drop_duplicates(subset=["AirQualityStationEoICode"])
        
        # Converti in dict
        stations = []
        for _, row in df_stations.iterrows():
            station = {}
            for eea_col, db_col in self.COLUMN_MAPPING.items():
                if eea_col in row and pd.notna(row[eea_col]):
                    value = row[eea_col]
                    
                    # Conversioni tipo
                    if db_col in ["latitude", "longitude", "altitude"]:
                        station[db_col] = float(value)
                    elif db_col in ["start_date", "end_date"]:
                        station[db_col] = self._parse_date(value)
                    else:
                        station[db_col] = str(value) if not pd.isna(value) else None
            
            if "station_code" in station:
                stations.append(station)
        
        logger.info(f"Extracted {len(stations)} unique stations")
        return stations

    def parse_sampling_points(self, df: pd.DataFrame) -> List[Dict]:
        """
        Extract unique sampling points from DataFrame.
        
        Args:
            df: DataFrame with EEA data
            
        Returns:
            List of sampling point dictionaries
        """
        logger.info("Extracting sampling points...")
        
        sp_cols = [
            "SamplingPoint",
            "AirQualityStationEoICode",
            "Countrycode",
            "AirPollutantCode",
        ]
        
        available_cols = [col for col in sp_cols if col in df.columns]
        df_sp = df[available_cols].drop_duplicates(subset=["SamplingPoint"])
        
        sampling_points = []
        for _, row in df_sp.iterrows():
            sp = {}
            
            if pd.notna(row.get("SamplingPoint")):
                sp["sampling_point_id"] = str(row["SamplingPoint"])
            if pd.notna(row.get("AirQualityStationEoICode")):
                sp["station_code"] = str(row["AirQualityStationEoICode"])
            if pd.notna(row.get("Countrycode")):
                sp["country_code"] = str(row["Countrycode"])
            if pd.notna(row.get("AirPollutantCode")):
                sp["pollutant_code"] = int(row["AirPollutantCode"])
            
            if "sampling_point_id" in sp:
                sampling_points.append(sp)
        
        logger.info(f"Extracted {len(sampling_points)} unique sampling points")
        return sampling_points

    def parse_measurements(self, df: pd.DataFrame) -> List[Dict]:
        """
        Extract measurements from DataFrame.
        
        Args:
            df: DataFrame with EEA data
            
        Returns:
            List of measurement dictionaries
        """
        logger.info("Extracting measurements...")
        
        measurements = []
        
        for _, row in df.iterrows():
            meas = {}
            
            # Campi obbligatori
            if pd.notna(row.get("DatetimeBegin")):
                meas["time"] = self._parse_datetime(row["DatetimeBegin"])
            if pd.notna(row.get("SamplingPoint")):
                meas["sampling_point_id"] = str(row["SamplingPoint"])
            if pd.notna(row.get("AirPollutantCode")):
                meas["pollutant_code"] = int(row["AirPollutantCode"])
            
            # Campi opzionali
            if pd.notna(row.get("Concentration")):
                meas["value"] = float(row["Concentration"])
            if pd.notna(row.get("UnitOfMeasurement")):
                meas["unit"] = str(row["UnitOfMeasurement"])
            if pd.notna(row.get("AggregationType")):
                meas["aggregation_type"] = str(row["AggregationType"])
            if pd.notna(row.get("Validity")):
                meas["validity"] = int(row["Validity"])
            if pd.notna(row.get("Verification")):
                meas["verification"] = int(row["Verification"])
            if pd.notna(row.get("DataCapture")):
                meas["data_capture"] = float(row["DataCapture"])
            if pd.notna(row.get("ResultTime")):
                meas["result_time"] = self._parse_datetime(row["ResultTime"])
            if pd.notna(row.get("ObservationId")):
                meas["observation_id"] = str(row["ObservationId"])
            
            # Aggiungi solo se ha campi obbligatori
            if all(k in meas for k in ["time", "sampling_point_id", "pollutant_code"]):
                measurements.append(meas)
        
        logger.info(f"Extracted {len(measurements)} measurements")
        return measurements

    def parse_all(self, filepath: Path) -> Dict[str, List[Dict]]:
        """
        Parse entire Parquet file and extract all entities.
        
        Args:
            filepath: Path to Parquet file
            
        Returns:
            Dictionary with 'stations', 'sampling_points', 'measurements'
            
        Example:
            >>> parser = ParquetParser()
            >>> data = parser.parse_all(Path("data.parquet"))
            >>> print(f"Stations: {len(data['stations'])}")
            >>> print(f"Measurements: {len(data['measurements'])}")
        """
        logger.info(f"Starting full parse of {filepath.name}")
        
        df = self.read_parquet(filepath)
        
        result = {
            "stations": self.parse_stations(df),
            "sampling_points": self.parse_sampling_points(df),
            "measurements": self.parse_measurements(df),
        }
        
        logger.info(
            f"Parse complete - Stations: {len(result['stations'])}, "
            f"Sampling Points: {len(result['sampling_points'])}, "
            f"Measurements: {len(result['measurements'])}"
        )
        
        return result

    @staticmethod
    def _parse_datetime(value) -> Optional[datetime]:
        """Parse datetime from various formats."""
        if pd.isna(value):
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return pd.to_datetime(value).to_pydatetime()
            except Exception:
                return None
        return None

    @staticmethod
    def _parse_date(value) -> Optional[datetime]:
        """Parse date from various formats."""
        if pd.isna(value):
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            try:
                return pd.to_datetime(value).date()
            except Exception:
                return None
        return None
