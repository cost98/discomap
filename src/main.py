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
    description="EEA Air Quality Data ETL Pipeline & Monitoring",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
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
