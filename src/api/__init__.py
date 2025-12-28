"""
DiscoMap API Routes

Unified import API for EEA air quality data.
"""

from fastapi import APIRouter

from src.api.routes.import_routes import router as import_router

# Main API router
api_router = APIRouter()

# Include import routes
api_router.include_router(import_router)

__all__ = ["api_router", "import_router"]
