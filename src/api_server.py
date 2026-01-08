"""
FastAPI server for DiscoMap.

Minimal API server for ETL operations and monitoring.
"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.config import settings
from src.database.engine import close_db, get_engine
from src.logger import get_logger
from src.services import ETLPipeline

# Setup logging
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events for FastAPI app."""
    # Startup
    logger.info("🚀 Starting DiscoMap API...")
    logger.info(f"Database: {settings.database_url.split('@')[1]}")
    
    # Test DB connection
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(__import__('sqlalchemy').text("SELECT 1"))
        logger.info("✅ Database connection OK")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
    
    yield
    
    # Shutdown
    logger.info("👋 Shutting down DiscoMap API...")
    await close_db()


app = FastAPI(
    title="DiscoMap API",
    description="EEA Air Quality Data ETL Pipeline",
    version="1.0.0",
    lifespan=lifespan,
)


# Models
class ETLRequest(BaseModel):
    url: str
    skip_download: bool = False


class ETLResponse(BaseModel):
    success: bool
    message: str
    stats: dict | None = None


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "DiscoMap API",
        "version": "1.0.0",
        "status": "running",
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(__import__('sqlalchemy').text("SELECT 1"))
        
        return {
            "status": "healthy",
            "database": "connected",
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": f"error: {str(e)}",
        }


@app.post("/etl/run", response_model=ETLResponse)
async def run_etl(request: ETLRequest):
    """
    Run ETL pipeline from Parquet URL.
    
    Example:
        POST /etl/run
        {
            "url": "https://eeadmz1batchservice02.blob.core.windows.net/...",
            "skip_download": false
        }
    """
    try:
        logger.info(f"ETL request received: {request.url}")
        
        pipeline = ETLPipeline()
        stats = await pipeline.run_from_url(
            request.url,
            skip_download=request.skip_download,
        )
        
        logger.info(f"ETL completed: {stats}")
        
        return ETLResponse(
            success=True,
            message="ETL completed successfully",
            stats=stats,
        )
    
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get database statistics."""
    try:
        from sqlalchemy import text
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
        
        return {
            "stations": stations_count,
            "sampling_points": sampling_points_count,
            "measurements": measurements_count,
        }
    
    except Exception as e:
        logger.error(f"Stats query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "api_server:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="info",
    )
