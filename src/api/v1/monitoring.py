"""Monitoring and health check endpoints."""

import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from src.database.engine import get_engine
from src.logger import get_logger

logger = get_logger(__name__)
router = APIRouter()


# Response models
class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    database: str


class StatsResponse(BaseModel):
    """Database statistics response."""
    stations: int
    sampling_points: int
    measurements: int


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Check system health and database connectivity.
    
    Returns:
        Health status with database connection state
    """
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        
        return HealthResponse(
            status="healthy",
            database="connected",
        )
    
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            database=f"error: {str(e)}",
        )


@router.get("/stats", response_model=StatsResponse)
async def get_stats():
    """
    Get database statistics.
    
    Returns:
        Current counts for stations, sampling points, and measurements
    """
    try:
        engine = get_engine()
        
        async with engine.begin() as conn:
            # Count stations
            result = await conn.execute(
                text("SELECT COUNT(*) FROM airquality.stations")
            )
            stations_count = result.scalar() or 0
            
            # Count sampling points
            result = await conn.execute(
                text("SELECT COUNT(*) FROM airquality.sampling_points")
            )
            sampling_points_count = result.scalar() or 0
            
            # Count measurements
            result = await conn.execute(
                text("SELECT COUNT(*) FROM airquality.measurements")
            )
            measurements_count = result.scalar() or 0
        
        return StatsResponse(
            stations=stations_count,
            sampling_points=sampling_points_count,
            measurements=measurements_count,
        )
    
    except Exception as e:
        logger.error(f"Stats query failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve statistics: {str(e)}"
        )
