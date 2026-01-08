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
        "Samplingpoint": "sampling_point_id",  # Alternative format
        "AirPollutantCode": "pollutant_code",
        "Pollutant": "pollutant_code",  # Alternative format
        "SamplingPointStartDate": "sp_start_date",
        "SamplingPointEndDate": "sp_end_date",
        
        # Measurement fields
        "DatetimeBegin": "time",
        "Start": "time",  # Alternative format
        "DatetimeEnd": "datetime_end",
        "End": "datetime_end",  # Alternative format
        "Concentration": "value",
        "Value": "value",  # Alternative format
        "UnitOfMeasurement": "unit",
        "Unit": "unit",  # Alternative format
        "AggregationType": "aggregation_type",
        "AggType": "aggregation_type",  # Alternative format
        "Validity": "validity",
        "Verification": "verification",
        "DataCapture": "data_capture",
        "ResultTime": "result_time",
        "ObservationId": "observation_id",
        "FkObservationLog": "observation_id",  # Alternative format
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
        
        # Check if we have direct station info or need to extract from Samplingpoint
        has_station_col = "AirQualityStationEoICode" in df.columns
        has_samplingpoint = "Samplingpoint" in df.columns or "SamplingPoint" in df.columns
        
        stations = []
        
        if has_station_col:
            # Old format: station info is directly available
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
            
            available_cols = [col for col in station_cols if col in df.columns]
            df_stations = df[available_cols].drop_duplicates(subset=["AirQualityStationEoICode"])
            
            for _, row in df_stations.iterrows():
                station = {}
                for eea_col, db_col in self.COLUMN_MAPPING.items():
                    if eea_col in row and pd.notna(row[eea_col]):
                        value = row[eea_col]
                        
                        if db_col in ["latitude", "longitude", "altitude"]:
                            station[db_col] = float(value)
                        elif db_col in ["start_date", "end_date"]:
                            station[db_col] = self._parse_date(value)
                        else:
                            station[db_col] = str(value) if not pd.isna(value) else None
                
                if "station_code" in station:
                    stations.append(station)
        
        elif has_samplingpoint:
            # New format: extract station code from Samplingpoint field
            # Format: "PT/SPO-PT02022_00008_100" → station_code = "PT02022"
            sp_col = "Samplingpoint" if "Samplingpoint" in df.columns else "SamplingPoint"
            
            station_codes = set()
            for sp_id in df[sp_col].dropna().unique():
                # Extract country and station code from sampling point
                # Format: CC/SPO-SSSSSS_XXXXX_YYY where CC=country, SSSSSS=station
                if "/" in sp_id:
                    parts = sp_id.split("/")
                    if len(parts) >= 2:
                        country_code = parts[0]
                        # Extract station code from "SPO-PT02022_00008_100"
                        station_part = parts[1].replace("SPO-", "").split("_")[0]
                        
                        station_code = f"{country_code}/{station_part}"
                        if station_code not in station_codes:
                            station_codes.add(station_code)
                            stations.append({
                                "station_code": station_code,
                                "country_code": country_code,
                            })
        
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
        
        # Determine available column names (support both formats)
        sp_col = None
        if "SamplingPoint" in df.columns:
            sp_col = "SamplingPoint"
        elif "Samplingpoint" in df.columns:
            sp_col = "Samplingpoint"
        
        pollutant_col = None
        if "AirPollutantCode" in df.columns:
            pollutant_col = "AirPollutantCode"
        elif "Pollutant" in df.columns:
            pollutant_col = "Pollutant"
        
        if not sp_col or not pollutant_col:
            logger.warning(f"Missing required columns for sampling points")
            return []
        
        # Get unique combinations of sampling point + pollutant
        df_sp = df[[sp_col, pollutant_col]].drop_duplicates()
        
        sampling_points = []
        for _, row in df_sp.iterrows():
            sp_id = str(row[sp_col]) if pd.notna(row[sp_col]) else None
            pollutant_code = int(row[pollutant_col]) if pd.notna(row[pollutant_col]) else None
            
            if not sp_id or not pollutant_code:
                continue
            
            # Extract station code from sampling point ID
            # Format: "PT/SPO-PT02022_00008_100" → station_code = "PT/PT02022"
            station_code = None
            country_code = None
            if "/" in sp_id:
                parts = sp_id.split("/")
                if len(parts) >= 2:
                    country_code = parts[0]
                    station_part = parts[1].replace("SPO-", "").split("_")[0]
                    station_code = f"{country_code}/{station_part}"
            
            sp = {
                "sampling_point_id": sp_id,
                "pollutant_code": pollutant_code,
            }
            
            if station_code:
                sp["station_code"] = station_code
            if country_code:
                sp["country_code"] = country_code
            
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
        
        # Determine column names (support both formats)
        time_col = None
        if "DatetimeBegin" in df.columns:
            time_col = "DatetimeBegin"
        elif "Start" in df.columns:
            time_col = "Start"
        
        sp_col = None
        if "SamplingPoint" in df.columns:
            sp_col = "SamplingPoint"
        elif "Samplingpoint" in df.columns:
            sp_col = "Samplingpoint"
        
        pollutant_col = None
        if "AirPollutantCode" in df.columns:
            pollutant_col = "AirPollutantCode"
        elif "Pollutant" in df.columns:
            pollutant_col = "Pollutant"
        
        value_col = None
        if "Concentration" in df.columns:
            value_col = "Concentration"
        elif "Value" in df.columns:
            value_col = "Value"
        
        if not all([time_col, sp_col, pollutant_col]):
            logger.error(f"Missing required columns. Found: time={time_col}, sp={sp_col}, pollutant={pollutant_col}")
            return []
        
        measurements = []
        
        for _, row in df.iterrows():
            meas = {}
            
            # Campi obbligatori
            if pd.notna(row.get(time_col)):
                meas["time"] = self._parse_datetime(row[time_col])
            if pd.notna(row.get(sp_col)):
                meas["sampling_point_id"] = str(row[sp_col])
            if pd.notna(row.get(pollutant_col)):
                meas["pollutant_code"] = int(row[pollutant_col])
            
            # Campi opzionali con nomi flessibili
            if value_col and pd.notna(row.get(value_col)):
                meas["value"] = float(row[value_col])
            
            # Unit
            unit_col = "UnitOfMeasurement" if "UnitOfMeasurement" in df.columns else "Unit"
            if unit_col in df.columns and pd.notna(row.get(unit_col)):
                meas["unit"] = str(row[unit_col])
            
            # Aggregation type
            agg_col = "AggregationType" if "AggregationType" in df.columns else "AggType"
            if agg_col in df.columns and pd.notna(row.get(agg_col)):
                meas["aggregation_type"] = str(row[agg_col])
            
            # Altri campi opzionali
            if pd.notna(row.get("Validity")):
                meas["validity"] = int(row["Validity"])
            if pd.notna(row.get("Verification")):
                meas["verification"] = int(row["Verification"])
            if pd.notna(row.get("DataCapture")):
                meas["data_capture"] = float(row["DataCapture"])
            if pd.notna(row.get("ResultTime")):
                meas["result_time"] = self._parse_datetime(row["ResultTime"])
            
            # Observation ID
            obs_col = "ObservationId" if "ObservationId" in df.columns else "FkObservationLog"
            if obs_col in df.columns and pd.notna(row.get(obs_col)):
                meas["observation_id"] = str(row[obs_col])
            
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
        """Parse datetime from various formats, ensuring UTC timezone."""
        if pd.isna(value):
            return None
        
        # If already datetime, ensure it has timezone
        if isinstance(value, datetime):
            if value.tzinfo is None:
                # Add UTC timezone to naive datetime
                import pytz
                return pytz.UTC.localize(value)
            return value
        
        # If pandas Timestamp, convert to datetime with UTC
        if hasattr(value, 'tz_localize'):
            # Pandas Timestamp
            if value.tz is None:
                # Naive timestamp - localize to UTC
                value = value.tz_localize('UTC')
            return value.to_pydatetime()
        
        # If string, parse and add UTC
        if isinstance(value, str):
            try:
                dt = pd.to_datetime(value)
                if dt.tz is None:
                    dt = dt.tz_localize('UTC')
                return dt.to_pydatetime()
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
