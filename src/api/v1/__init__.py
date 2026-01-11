"""API v1 routers."""

from fastapi import APIRouter

from .etl_sync import router as etl_sync_router
from .etl_async import router as etl_async_router
from .etl_async_file import router as etl_async_file_router
from .monitoring import router as monitoring_router
from .stations import router as stations_router
from .aggregates import router as aggregates_router
from .compression import router as compression_router

# Main v1 router
router = APIRouter(prefix="/api/v1")

# Include sub-routers
router.include_router(etl_sync_router, prefix="/etl")
router.include_router(etl_async_router, prefix="/etl")  # /api/v1/etl/async/batch, /status
router.include_router(etl_async_file_router, prefix="/etl")  # /api/v1/etl/async/file/*
router.include_router(stations_router)  # /api/v1/stations/*
router.include_router(monitoring_router, prefix="/monitoring")
router.include_router(aggregates_router, prefix="/aggregates")
router.include_router(compression_router, prefix="/compression")

__all__ = ["router"]
