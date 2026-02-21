"""Synchronous ETL endpoints."""

import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.logger import get_logger
from src.services import ETLPipeline

logger = get_logger(__name__)
router = APIRouter(prefix="/sync", tags=["\U0001F4E5 ETL - Sync"])


class ETLRequest(BaseModel):
    """ETL run request (sync - single URL)."""
    url: str


class ETLResponse(BaseModel):
    """ETL run response (sync)."""
    success: bool
    message: str
    stats: dict | None = None


@router.post("/run", response_model=ETLResponse)
async def run_etl(
    request: ETLRequest,
    upsert: bool = False,
):
    """
    Run ETL pipeline from single Parquet URL (SYNCHRONOUS).
    
    Processes air quality data from EEA Parquet files:
    1. Downloads data from provided URL
    2. Parses and validates measurements
    3. Stores in TimescaleDB
    
    Returns results immediately. Recommended for single files.
    For multiple URLs, use POST /api/v1/etl/async/batch.
    
    Args:
        request: ETL configuration with URL and options
        
    Returns:
        ETL execution statistics
        
    Example:
        ```json
        POST /api/v1/etl/sync/run
        {
            "url": "https://eeadmz1batchservice02.blob.core.windows.net/..."
        }
        ```
    """
    try:
        logger.info(f"📥 ETL request: {request.url} (upsert={upsert})")
        
        pipeline = ETLPipeline(upsert_mode=upsert)
        stats = await pipeline.run_from_url(request.url)
        
        logger.info(f"✅ ETL completed: {stats}")
        
        return ETLResponse(
            success=True,
            message="ETL pipeline completed successfully",
            stats=stats,
        )
    
    except Exception as e:
        logger.error(f"❌ ETL failed: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"ETL pipeline failed: {str(e)}"
        )
