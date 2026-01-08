"""API v1 routers."""

from fastapi import APIRouter

from .etl import router as etl_router
from .monitoring import router as monitoring_router

# Main v1 router
router = APIRouter(prefix="/api/v1")

# Include sub-routers
router.include_router(etl_router, prefix="/etl", tags=["ETL"])
router.include_router(monitoring_router, prefix="/monitoring", tags=["Monitoring"])

__all__ = ["router"]
