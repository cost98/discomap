"""
Package database - SQLAlchemy ORM per DiscoMap.

Uso semplice:
    from src.database import get_db_session, StationRepository
    
    async with get_db_session() as session:
        station_repo = StationRepository(session)
        station = await station_repo.get_by_code("IT0508A")
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
    "Station",
    "SamplingPoint",
    "Pollutant",
    "Measurement",
    "ValidityFlag",
    "VerificationStatus",
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
