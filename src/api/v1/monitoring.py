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


class DatabaseSizeResponse(BaseModel):
    """Database size and compression statistics."""
    total_database_size: str
    total_database_size_bytes: int
    measurements_table_size: str
    measurements_table_size_bytes: int
    measurements_indexes_size: str
    measurements_total_size: str
    compression_enabled: bool
    uncompressed_size: str
    compressed_size: str
    compression_ratio: float
    space_saved: str
    space_saved_percentage: float
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "total_database_size": "2.5 GB",
                "total_database_size_bytes": 2684354560,
                "measurements_table_size": "1.8 GB",
                "measurements_table_size_bytes": 1932735283,
                "measurements_indexes_size": "450 MB",
                "measurements_total_size": "2.2 GB",
                "compression_enabled": True,
                "uncompressed_size": "5.4 GB",
                "compressed_size": "1.8 GB",
                "compression_ratio": 3.0,
                "space_saved": "3.6 GB",
                "space_saved_percentage": 66.67
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
    "/stats/database",
    response_model=DatabaseSizeResponse,
    summary="Get database storage statistics",
    description="Returns database size, compression ratio, and space savings from TimescaleDB compression"
)
async def get_database_stats() -> DatabaseSizeResponse:
    """Get database size and compression statistics."""
    try:
        engine = get_engine()
        
        async with engine.connect() as conn:
            # Get total database size
            db_size_result = await conn.execute(text("""
                SELECT pg_database_size(current_database())
            """))
            total_size_bytes = db_size_result.scalar()
            
            # Get measurements table size (before compression)
            table_size_result = await conn.execute(text("""
                SELECT 
                    pg_total_relation_size('airquality.measurements') as total_size,
                    pg_relation_size('airquality.measurements') as table_size,
                    pg_total_relation_size('airquality.measurements') - pg_relation_size('airquality.measurements') as indexes_size
            """))
            table_stats = table_size_result.fetchone()
            
            # Try to get compression stats from TimescaleDB (if compression is enabled)
            # Default to no compression
            uncompressed_bytes = table_stats[1]
            compressed_bytes = table_stats[1]
            compression_ratio = 1.0
            
            try:
                compression_result = await conn.execute(text("""
                    SELECT 
                        COALESCE(SUM(before_compression_total_bytes), 0) as uncompressed_bytes,
                        COALESCE(SUM(after_compression_total_bytes), 0) as compressed_bytes,
                        CASE 
                            WHEN SUM(after_compression_total_bytes) > 0 
                            THEN SUM(before_compression_total_bytes)::float / SUM(after_compression_total_bytes)::float
                            ELSE 0
                        END as compression_ratio
                    FROM timescaledb_information.compressed_chunk_stats
                    WHERE hypertable_name = 'measurements'
                """))
                compression_stats = compression_result.fetchone()
                
                if compression_stats and compression_stats[0] and compression_stats[1]:
                    uncompressed_bytes = compression_stats[0]
                    compressed_bytes = compression_stats[1]
                    compression_ratio = compression_stats[2] if compression_stats[2] else 1.0
            except Exception:
                # Compression not enabled or stats not available - use defaults
                pass
            
            space_saved_bytes = uncompressed_bytes - compressed_bytes
            space_saved_pct = (space_saved_bytes / uncompressed_bytes * 100) if uncompressed_bytes > 0 else 0
            
            return DatabaseSizeResponse(
                total_database_size=_format_bytes(total_size_bytes),
                total_database_size_bytes=total_size_bytes,
                measurements_table_size=_format_bytes(table_stats[1]),
                measurements_table_size_bytes=table_stats[1],
                measurements_indexes_size=_format_bytes(table_stats[2]),
                measurements_total_size=_format_bytes(table_stats[0]),
                compression_enabled=compression_ratio > 1.0,
                uncompressed_size=_format_bytes(uncompressed_bytes),
                compressed_size=_format_bytes(compressed_bytes),
                compression_ratio=round(compression_ratio, 2),
                space_saved=_format_bytes(space_saved_bytes),
                space_saved_percentage=round(space_saved_pct, 2)
            )
            
    except Exception as e:
        logger.error(f"Error getting database statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _format_bytes(bytes_value: int) -> str:
    """Format bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.1f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.1f} PB"
