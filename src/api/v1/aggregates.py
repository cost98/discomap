"""Continuous aggregate management endpoints."""

from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import text

from src.database.engine import get_engine
from src.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(tags=["\U0001F527 Database Optimization"])


# Response models
class AggregateStatusResponse(BaseModel):
    """Continuous aggregate status response."""
    view_name: str
    total_rows: int
    first_day: Optional[datetime]
    last_day: Optional[datetime]
    sampling_points: int
    pollutants: int
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "view_name": "daily_measurements",
                "total_rows": 21309368,
                "first_day": "2009-01-01T00:00:00Z",
                "last_day": "2025-01-01T00:00:00Z",
                "sampling_points": 7025,
                "pollutants": 7
            }
        }
    }


class RefreshRequest(BaseModel):
    """Refresh operation request."""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "description": "Full refresh (all data)",
                    "value": {}
                },
                {
                    "description": "Partial refresh (last week)",
                    "value": {
                        "start_date": "2025-01-01T00:00:00Z",
                        "end_date": "2025-01-07T23:59:59Z"
                    }
                }
            ]
        }
    }


class RefreshResponse(BaseModel):
    """Refresh operation response."""
    message: str
    view_name: str
    status: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "message": "Full continuous aggregate refresh task queued",
                "view_name": "daily_measurements",
                "status": "queued",
                "start_date": None,
                "end_date": None
            }
        }
    }


@router.get("/status", response_model=AggregateStatusResponse)
async def get_aggregate_status():
    """
    Get continuous aggregate status and statistics.
    
    Returns:
        Statistics about the daily_measurements continuous aggregate
    """
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            # Get count and date range
            result = await conn.execute(text("""
                SELECT 
                    COUNT(*) as total_rows,
                    MIN(day) as first_day,
                    MAX(day) as last_day,
                    COUNT(DISTINCT sampling_point_id) as sampling_points,
                    COUNT(DISTINCT pollutant_code) as pollutants
                FROM airquality.daily_measurements
            """))
            
            row = result.fetchone()
            
            return AggregateStatusResponse(
                view_name="daily_measurements",
                total_rows=row[0] or 0,
                first_day=row[1],
                last_day=row[2],
                sampling_points=row[3] or 0,
                pollutants=row[4] or 0
            )
            
    except Exception as e:
        logger.error(f"Error getting aggregate status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def refresh_aggregate_background(start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """Background task to refresh continuous aggregate."""
    try:
        if start_date and end_date:
            logger.info(f"🔄 Starting continuous aggregate refresh from {start_date} to {end_date}...")
        else:
            logger.info("🔄 Starting full continuous aggregate refresh...")
        
        engine = get_engine()
        # Use connect() without transaction for CALL statements
        async with engine.connect() as conn:
            # Set autocommit mode
            await conn.execution_options(isolation_level="AUTOCOMMIT")
            
            if start_date and end_date:
                await conn.execute(text("""
                    CALL refresh_continuous_aggregate(
                        'airquality.daily_measurements', 
                        :start_date, 
                        :end_date
                    )
                """), {"start_date": start_date, "end_date": end_date})
            else:
                await conn.execute(text("""
                    CALL refresh_continuous_aggregate('airquality.daily_measurements', NULL, NULL)
                """))
        
        logger.info("✅ Continuous aggregate refresh completed successfully")
    except Exception as e:
        logger.error(f"❌ Error refreshing continuous aggregate: {e}")


@router.post("/refresh", response_model=RefreshResponse)
async def refresh_aggregate(background_tasks: BackgroundTasks, request: RefreshRequest = RefreshRequest()):
    """
    Trigger a manual refresh of the daily_measurements continuous aggregate.
    
    Args:
        start_date: Optional start date for partial refresh (ISO format: 2024-01-01T00:00:00)
        end_date: Optional end date for partial refresh (ISO format: 2024-12-31T23:59:59)
    
    If dates are not provided, refreshes all historical data.
    The operation may take 10-30 minutes for full refresh, 1-5 minutes for partial.
    
    Examples:
        Full refresh: POST /aggregates/refresh
        Partial refresh: POST /aggregates/refresh with body:
            {"start_date": "2024-01-01T00:00:00", "end_date": "2024-12-31T23:59:59"}
    
    Returns:
        Confirmation message
    """
    try:
        # Add refresh task to background
        background_tasks.add_task(
            refresh_aggregate_background, 
            request.start_date, 
            request.end_date
        )
        
        if request.start_date and request.end_date:
            logger.info(f"📊 Continuous aggregate refresh task queued (from {request.start_date} to {request.end_date})")
            message = f"Refresh started for period {request.start_date.date()} to {request.end_date.date()}. Use /aggregates/status to monitor progress."
        else:
            logger.info("📊 Full continuous aggregate refresh task queued")
            message = "Full refresh started in background. Use /aggregates/status to monitor progress."
        
        return RefreshResponse(
            message=message,
            view_name="daily_measurements",
            status="queued",
            start_date=request.start_date,
            end_date=request.end_date
        )
        
    except Exception as e:
        logger.error(f"Error queuing aggregate refresh: {e}")
        raise HTTPException(status_code=500, detail=str(e))
