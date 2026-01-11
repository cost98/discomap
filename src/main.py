"""
FastAPI server for DiscoMap.

API server for EEA Air Quality ETL operations and monitoring.
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import text

from src.api.v1 import router as v1_router
from src.config import settings
from src.database.engine import close_db, get_engine
from src.logger import get_logger

# Setup logging
logger = get_logger(__name__)

# OpenAPI tags metadata for Swagger organization
tags_metadata = [
    {
        "name": "\U0001F4CD Metadata",
        "description": "Reference data and metadata - stations, sampling points, countries, pollutants",
    },
    {
        "name": "\U0001F4E5 ETL - Sync",
        "description": "Synchronous ETL operations - immediate response, best for small datasets",
    },
    {
        "name": "\U0001F4E5 ETL - Batch",
        "description": "Asynchronous batch ETL from Parquet URLs - background processing for large datasets",
    },
    {
        "name": "\U0001F4E5 ETL - Files",
        "description": "Asynchronous ETL from uploaded Parquet files",
    },
    {
        "name": "\U0001F527 Database Optimization",
        "description": "Continuous aggregates management and chunk compression for performance and storage optimization",
    },
    {
        "name": "\U00002699\U0000FE0F System",
        "description": "API health checks, database statistics, and system monitoring",
    },
]


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
            await conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection OK")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
    
    yield
    
    # Shutdown
    logger.info("👋 Shutting down DiscoMap API...")
    await close_db()


app = FastAPI(
    title="DiscoMap API",
    description="🌍 **EEA Air Quality Data Platform**\n\n"
                "Time-series database and ETL pipeline for European air quality measurements.\n\n"
                "**Features:**\n"
                "- 📥 Batch ETL from Parquet files (sync/async)\n"
                "- 📊 Pre-aggregated daily statistics\n"
                "- 🗄️ TimescaleDB hypertable storage\n"
                "- 🔍 Station metadata and sampling points\n"
                "- ⚡ Continuous aggregates for fast queries",
    version="1.0.0",
    openapi_tags=tags_metadata,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    contact={
        "name": "DiscoMap Team",
        "url": "https://github.com/cost98/discomap",
    },
    license_info={
        "name": "MIT",
    },
)

# Include API v1 router
app.include_router(v1_router)


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "src.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
        log_level="info",
    )
