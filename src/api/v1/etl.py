"""ETL endpoints."""

import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from src.logger import get_logger
from src.services import ETLPipeline

logger = get_logger(__name__)
router = APIRouter()


# Request/Response models
class ETLRequest(BaseModel):
    """ETL run request."""
    url: str
    skip_download: bool = False


class ETLResponse(BaseModel):
    """ETL run response."""
    success: bool
    message: str
    stats: dict | None = None


@router.post("/run", response_model=ETLResponse)
async def run_etl(request: ETLRequest):
    """
    Run ETL pipeline from Parquet URL.
    
    Processes air quality data from EEA Parquet files:
    1. Downloads data from provided URL
    2. Parses and validates measurements
    3. Stores in TimescaleDB
    
    Args:
        request: ETL configuration with URL and options
        
    Returns:
        ETL execution statistics
        
    Example:
        ```json
        POST /api/v1/etl/run
        {
            "url": "https://eeadmz1batchservice02.blob.core.windows.net/...",
            "skip_download": false
        }
        ```
    """
    try:
        logger.info(f"📥 ETL request: {request.url}")
        
        pipeline = ETLPipeline()
        stats = await pipeline.run_from_url(
            request.url,
            skip_download=request.skip_download,
        )
        
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
