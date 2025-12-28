"""Service for importing EEA data using official API."""

import logging
import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional

import requests
from sqlalchemy.ext.asyncio import AsyncSession

from src.services.parquet_copy_importer import ParquetCopyImporter

logger = logging.getLogger(__name__)

EEA_API_BASE = "https://eeadmz1-downloads-api-appservice.azurewebsites.net/"


async def import_from_eea_api(
    session: AsyncSession,
    countries: List[str],
    pollutants: Optional[List[str]] = None,
    dataset: int = 1,
    datetime_start: Optional[str] = None,
    datetime_end: Optional[str] = None,
    aggregation_type: Optional[str] = None,
) -> int:
    """Import data from EEA API using COPY for high performance.
    
    Args:
        session: Database session
        countries: Country codes (e.g., ["IT", "FR"])
        pollutants: Pollutant codes (e.g., ["NO2", "PM10"])
        dataset: 1=E2a (UTD), 2=E1a (verified), 3=Airbase
        datetime_start: Start datetime (YYYY-MM-DDTHH:MM:SSZ)
        datetime_end: End datetime (YYYY-MM-DDTHH:MM:SSZ)
        aggregation_type: "hour", "day", or "var"
        
    Returns:
        Total records imported
    """
    logger.info(f"Downloading from EEA API: countries={countries}, pollutants={pollutants}")
    
    # Build request
    request_body = {
        "countries": countries,
        "cities": [],
        "pollutants": pollutants or [],
        "dataset": dataset,
        "source": "DiscoMap Python",
    }
    
    if datetime_start:
        request_body["dateTimeStart"] = datetime_start
    if datetime_end:
        request_body["dateTimeEnd"] = datetime_end
    if aggregation_type:
        request_body["aggregationType"] = aggregation_type
    
    # Download from API
    endpoint = f"{EEA_API_BASE}ParquetFile"
    logger.info(f"Requesting data from {endpoint}")
    
    response = requests.post(endpoint, json=request_body, timeout=600)
    response.raise_for_status()
    
    if response.status_code == 206:
        logger.warning("Download exceeds 600MB - consider using smaller time ranges")
    
    # Save to temp zip file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip', mode='wb') as tmp:
        tmp.write(response.content)
        temp_zip = tmp.name
    
    logger.info(f"Downloaded {len(response.content) / 1024 / 1024:.2f} MB")
    
    try:
        # Extract parquet files from zip
        temp_dir = Path(tempfile.mkdtemp())
        
        with zipfile.ZipFile(temp_zip, 'r') as zf:
            zf.extractall(temp_dir)
        
        # Find all parquet files
        parquet_files = list(temp_dir.glob("**/*.parquet"))
        logger.info(f"Found {len(parquet_files)} Parquet files in archive")
        
        if not parquet_files:
            raise ValueError("No Parquet files found in downloaded archive")
        
        # Import using COPY
        importer = ParquetCopyImporter(session)
        total_records = 0
        
        for pfile in parquet_files:
            logger.info(f"Importing {pfile.name}...")
            count = await importer.import_parquet_file(str(pfile))
            total_records += count
        
        logger.info(f"Total imported: {total_records:,} records")
        return total_records
        
    finally:
        # Cleanup
        try:
            Path(temp_zip).unlink(missing_ok=True)
            if 'temp_dir' in locals():
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
        except:
            pass
