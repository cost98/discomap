"""
Pytest configuration and fixtures for DiscoMap tests.
"""

import asyncio
from typing import AsyncGenerator

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.database.models import Base


# Test database URL (use in-memory SQLite or separate test database)
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
async def test_engine():
    """Create test database engine."""
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    yield engine
    
    await engine.dispose()


@pytest.fixture
async def db_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create test database session."""
    async_session = sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    
    async with async_session() as session:
        yield session
        await session.rollback()


@pytest.fixture
def sample_station_data():
    """Sample station data for testing."""
    return {
        "station_code": "TEST001",
        "country_code": "IT",
        "station_name": "Test Station",
        "station_type": "background",
        "area_type": "urban",
        "latitude": 45.4642,
        "longitude": 9.1900,
        "altitude": 122.0,
        "municipality": "Milano",
        "region": "Lombardia",
    }


@pytest.fixture
def sample_sampling_point_data():
    """Sample sampling point data for testing."""
    return {
        "sampling_point_id": "IT/SPO.TEST001_8_chemi_2023-01-01",
        "station_code": "TEST001",
        "country_code": "IT",
        "instrument_type": "8_chemi",
        "pollutant_code": 8,  # NO2
    }
