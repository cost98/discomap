"""
Database package for DiscoMap.

Provides SQLAlchemy models, engine configuration, and database utilities.
"""

from .engine import get_engine, get_session, get_db_session, init_db, close_db
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
from .repositories import (
    CountryRepository,
    MeasurementRepository,
    PollutantRepository,
    SamplingPointRepository,
    StationRepository,
)

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
    "get_session",
    "get_db_session",
    "init_db",
    "close_db",
    # Repositories
    "CountryRepository",
    "StationRepository",
    "SamplingPointRepository",
    "PollutantRepository",
    "MeasurementRepository",
]
