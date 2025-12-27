"""
Package database - SQLAlchemy ORM per DiscoMap.

Uso:
    from src.database import get_db_session, StationRepository
    
    async with get_db_session() as session:
        repo = StationRepository(session)
        station = await repo.get_by_code("IT0508A")
"""

from .engine import close_db, get_db_session, get_engine, get_session_factory
from .models import (
    Base,
    Country,
    Measurement,
    Pollutant,
    SamplingPoint,
    Station,
    ValidityFlag,
    VerificationStatus,
)
from .repositories import MeasurementRepository, SamplingPointRepository, StationRepository

__all__ = [
    # Models
    "Base",
    "Country",
    "Pollutant",
    "ValidityFlag",
    "VerificationStatus",
    "Station",
    "SamplingPoint",
    "Measurement",
    # Engine
    "get_engine",
    "get_session_factory",
    "get_db_session",
    "close_db",
    # Repositories
    "StationRepository",
    "SamplingPointRepository",
    "MeasurementRepository",
]
