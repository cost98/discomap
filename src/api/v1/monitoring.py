"""Monitoring and health check endpoints."""

import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from src.database.engine import get_engine
from src.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(tags=["\U00002699\U0000FE0F System"])


# Response models
class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    database: str
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "status": "healthy",
                "database": "connected"
            }
        }
    }


class StatsResponse(BaseModel):
    """Database statistics response."""
    stations: int
    sampling_points: int
    measurements: int
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "stations": 5837,
                "sampling_points": 7025,
                "measurements": 427742283
            }
        }
    }


class DataQualityResponse(BaseModel):
    """Data quality statistics response."""
    total_measurements: int
    valid_measurements: int
    invalid_measurements: int
    below_detection_measurements: int
    maintenance_measurements: int
    valid_percentage: float
    invalid_percentage: float
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "total_measurements": 427742283,
                "valid_measurements": 388419262,
                "invalid_measurements": 27635388,
                "below_detection_measurements": 11069612,
                "maintenance_measurements": 618021,
                "valid_percentage": 90.82,
                "invalid_percentage": 6.46
            }
        }
    }


class ValidityFlagResponse(BaseModel):
    """Validity flag details."""
    validity_code: int
    validity_name: str
    description: str
    count: int
    percentage: float
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "validity_code": 1,
                "validity_name": "Valid",
                "description": "Valid measurement",
                "count": 388419262,
                "percentage": 90.82
            }
        }
    }


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


@router.get(
    "/data-quality",
    response_model=DataQualityResponse,
    summary="Get data quality statistics",
    description="Returns statistics about measurement validity across the entire dataset"
)
async def get_data_quality() -> DataQualityResponse:
    """Get data quality statistics based on validity flags."""
    try:
        engine = get_engine()
        
        async with engine.connect() as conn:
            # Get overall quality statistics
            result = await conn.execute(text("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(*) FILTER (WHERE validity = 1) as valid,
                    COUNT(*) FILTER (WHERE validity = -1) as invalid,
                    COUNT(*) FILTER (WHERE validity IN (2, 3)) as below_detection,
                    COUNT(*) FILTER (WHERE validity = -99) as maintenance
                FROM airquality.measurements
            """))
            row = result.fetchone()
            
            total = row[0]
            valid = row[1]
            invalid = row[2]
            below_detection = row[3]
            maintenance = row[4]
            
            return DataQualityResponse(
                total_measurements=total,
                valid_measurements=valid,
                invalid_measurements=invalid + maintenance,
                below_detection_measurements=below_detection,
                maintenance_measurements=maintenance,
                valid_percentage=round((valid / total * 100), 2) if total > 0 else 0,
                invalid_percentage=round(((invalid + maintenance) / total * 100), 2) if total > 0 else 0
            )
            
    except Exception as e:
        logger.error(f"Error getting data quality statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/data-quality/by-validity",
    response_model=list[ValidityFlagResponse],
    summary="Get data quality breakdown by validity flag",
    description="Returns detailed statistics for each validity flag with descriptions"
)
async def get_data_quality_by_validity() -> list[ValidityFlagResponse]:
    """Get data quality breakdown by validity flag."""
    try:
        engine = get_engine()
        
        async with engine.connect() as conn:
            # Get total count for percentage calculation
            total_result = await conn.execute(text("SELECT COUNT(*) FROM airquality.measurements"))
            total_count = total_result.scalar()
            
            # Get counts by validity with flag details
            result = await conn.execute(text("""
                SELECT 
                    vf.validity_code,
                    vf.validity_name,
                    vf.description,
                    COUNT(m.validity) as count
                FROM airquality.validity_flags vf
                LEFT JOIN airquality.measurements m ON m.validity = vf.validity_code
                GROUP BY vf.validity_code, vf.validity_name, vf.description
                ORDER BY count DESC
            """))
            
            return [
                ValidityFlagResponse(
                    validity_code=row[0],
                    validity_name=row[1],
                    description=row[2],
                    count=row[3],
                    percentage=round((row[3] / total_count * 100), 2) if total_count > 0 else 0
                )
                for row in result.fetchall()
            ]
            
    except Exception as e:
        logger.error(f"Error getting validity breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))
