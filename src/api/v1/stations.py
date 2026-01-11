"""CSV upload endpoints for stations metadata."""

import csv
import io
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

from src.logger import get_logger
from src.database.engine import get_db_session
from src.database.repositories import CountryRepository, PollutantRepository, StationRepository, SamplingPointRepository

logger = get_logger(__name__)
router = APIRouter(prefix="/stations", tags=["Stations Metadata"])


class CSVUploadResponse(BaseModel):
    """Response for CSV upload."""
    message: str
    stations_processed: int
    stations_created: int
    stations_updated: int
    pollutants_processed: int
    pollutants_created: int
    sampling_points_processed: int
    sampling_points_created: int
    sampling_points_updated: int
    errors: list[str]


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date from CSV format (DD/MM/YYYY HH:MM:SS)."""
    if not date_str or date_str.strip() == "":
        return None
    try:
        return datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    """Parse float, handling -999 as NULL."""
    if not value or value.strip() == "" or value == "-999.0" or value == "-999":
        return None
    try:
        return float(value)
    except ValueError:
        return None


@router.post("/upload-csv", response_model=CSVUploadResponse)
async def upload_stations_csv(file: UploadFile = File(...)):
    """
    Upload stations metadata from EEA DataExtract CSV.
    
    Expected CSV columns:
    - Country: Country name
    - Air Quality Station EoI Code: Station code (ES1047A)
    - Air Quality Station Nat Code: National station code
    - Air Quality Station Name: Station name
    - Longitude, Latitude, Altitude: Coordinates
    - Air Quality Station Area: urban/suburban/rural
    - Air Quality Station Type: traffic/background/industrial
    - Municipality: Municipality name
    - Operational Activity Begin/End: Station operation dates
    
    Example:
        ```bash
        curl -X POST http://localhost:8000/api/v1/stations/upload-csv \\
             -F "file=@DataExtract.csv"
        ```
    
    Returns:
        Statistics about processed stations
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    # Read CSV content
    content = await file.read()
    text = content.decode('utf-8')
    csv_reader = csv.DictReader(io.StringIO(text))
    
    stations_data = {}
    sampling_points_data = {}
    errors = []
    
    # Carica countries e pollutants esistenti dal DB
    async with get_db_session() as session:
        country_repo = CountryRepository(session)
        pollutant_repo = PollutantRepository(session)
        
        countries = await country_repo.get_all()
        valid_countries = {c.country_code for c in countries}
        
        pollutants = await pollutant_repo.get_all()
        pollutant_map = {p.pollutant_name: p.pollutant_code for p in pollutants}
    
    logger.info(f"📋 Loaded {len(valid_countries)} countries and {len(pollutant_map)} pollutants from DB")
    
    # Parse CSV rows
    rows_processed = 0
    for row_num, row in enumerate(csv_reader, start=2):  # Start at 2 (header is row 1)
        rows_processed += 1
        try:
            station_code = row.get("Air Quality Station EoI Code", "").strip()
            sampling_point_id = row.get("Sampling Point Id", "").strip()
            
            if not station_code or not sampling_point_id:
                continue  # Skip rows without station or sampling point
            
            # Extract country code from station_code (e.g., ES1047A -> ES)
            country_code = station_code[:2] if len(station_code) >= 2 else None
            
            # Verifica che country esista nel DB
            if country_code not in valid_countries:
                errors.append(f"Row {row_num}: Country '{country_code}' not in DB - skipping station {station_code}")
                continue
            
            # Build station data (use first occurrence of each station)
            if station_code not in stations_data:
                stations_data[station_code] = {
                    "station_code": station_code,
                    "country_code": country_code,
                    "station_name": row.get("Air Quality Station Name", "").strip() or None,
                    "station_type": row.get("Air Quality Station Type", "").strip() or None,
                    "area_type": row.get("Air Quality Station Area", "").strip() or None,
                    "latitude": parse_float(row.get("Latitude", "")),
                    "longitude": parse_float(row.get("Longitude", "")),
                    "altitude": parse_float(row.get("Altitude", "")),
                    "municipality": row.get("Municipality", "").strip() or None,
                    "region": None,  # Not in this CSV
                    "start_date": parse_date(row.get("Operational Activity Begin", "")),
                    "end_date": parse_date(row.get("Operational Activity End", "")),
                    "extra_metadata": {
                        "nat_code": row.get("Air Quality Station Nat Code", "").strip(),
                        "network": row.get("Air Quality Network", "").strip(),
                        "network_name": row.get("Air Quality Network Name", "").strip(),
                        "timezone": row.get("Timezone", "").strip(),
                    }
                }
            
            # Build sampling point data
            # Add country prefix to match Parquet format: ES/SP_35006001_14_6
            prefixed_sp_id = f"{country_code}/{sampling_point_id}"
            
            if prefixed_sp_id not in sampling_points_data:
                # Extract pollutant code from Air Pollutant column
                pollutant_name = row.get("Air Pollutant", "").strip()
                pollutant_code = pollutant_map.get(pollutant_name)
                
                if pollutant_code:
                    # Pollutant esiste nel DB, crea sampling point
                    sampling_points_data[prefixed_sp_id] = {
                        "sampling_point_id": prefixed_sp_id,
                        "station_code": station_code,
                        "country_code": country_code,
                        "pollutant_code": pollutant_code,
                        "start_date": parse_date(row.get("Operational Activity Begin", "")),
                        "end_date": parse_date(row.get("Operational Activity End", "")),
                        "extra_metadata": {
                            "sample_id": row.get("Sample Id", "").strip(),
                            "process_id": row.get("Process Id", "").strip(),
                            "measurement_type": row.get("Measurement Type", "").strip(),
                            "measurement_method": row.get("Measurement Method", "").strip(),
                        }
                    }
                else:
                    # Pollutant non esiste nel DB, skippa sampling point
                    errors.append(f"Row {row_num}: Pollutant '{pollutant_name}' not in DB - skipping {prefixed_sp_id}")
        
        except Exception as e:
            errors.append(f"Row {row_num}: {str(e)}")
            logger.warning(f"Error parsing row {row_num}: {e}")
    
    logger.info(f"📊 CSV parsing complete: {rows_processed} rows read, {len(stations_data)} stations, {len(sampling_points_data)} sampling points")
    
    # Insert/update stations and sampling points in database
    stations_created = 0
    stations_updated = 0
    sp_created = 0
    sp_updated = 0
    
    async with get_db_session() as session:
        station_repo = StationRepository(session)
        sp_repo = SamplingPointRepository(session)
        
        # 1. First insert/update stations
        for station_code, data in stations_data.items():
            try:
                existing = await station_repo.get_by_code(station_code)
                
                if existing:
                    await station_repo.create_or_update(data)
                    stations_updated += 1
                else:
                    await station_repo.create_or_update(data)
                    stations_created += 1
            
            except Exception as e:
                errors.append(f"Station {station_code}: {str(e)}")
                logger.error(f"Error inserting station {station_code}: {e}")
        
        await session.flush()  # Ensure stations exist
        
        # 2. Insert/update sampling points (pollutants già esistono nel DB)
        for sp_id, data in sampling_points_data.items():
            try:
                existing = await sp_repo.get_by_id(sp_id)
                
                if existing:
                    await sp_repo.create_or_update(data)
                    sp_updated += 1
                else:
                    await sp_repo.create_or_update(data)
                    sp_created += 1
            
            except Exception as e:
                errors.append(f"Sampling Point {sp_id}: {str(e)}")
                logger.error(f"Error inserting sampling point {sp_id}: {e}")
        
        await session.commit()
    
    logger.info(
        f"📊 CSV upload complete: {len(stations_data)} stations ({stations_created} created, {stations_updated} updated), "
        f"{len(sampling_points_data)} sampling points ({sp_created} created, {sp_updated} updated), {len(errors)} errors"
    )
    
    return CSVUploadResponse(
        message=f"Successfully processed {len(stations_data)} stations and {len(sampling_points_data)} sampling points from CSV",
        stations_processed=len(stations_data),
        stations_created=stations_created,
        stations_updated=stations_updated,
        pollutants_processed=0,  # Non creati qui, già nel DB
        pollutants_created=0,
        sampling_points_processed=len(sampling_points_data),
        sampling_points_created=sp_created,
        sampling_points_updated=sp_updated,
        errors=errors[:100],  # Limit to first 100 errors
    )