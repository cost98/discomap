"""
Pytest configuration and fixtures for DiscoMap tests.
"""

import asyncio
from pathlib import Path
from typing import AsyncGenerator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from src.database.models import Base


# Test database URL (use in-memory SQLite for fast unit tests)
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(scope="function")
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


# PostgreSQL container fixture for integration tests
@pytest.fixture(scope="function")
def postgres_container():
    """
    Tira su container PostgreSQL+TimescaleDB per integration tests.
    Usa l'immagine timescale/timescaledb:latest-pg16.
    """
    postgres = PostgresContainer(
        image="timescale/timescaledb:latest-pg16",
        username="testuser",
        password="testpass",
        dbname="testdb",
    )
    postgres.start()
    yield postgres
    postgres.stop()


@pytest.fixture(scope="function")
async def postgres_engine(postgres_container):
    """Engine per PostgreSQL reale con TimescaleDB."""
    # Costruisci URL da container
    connection_url = postgres_container.get_connection_url().replace(
        "psycopg2", "asyncpg"
    )
    
    engine = create_async_engine(connection_url, echo=False)
    
    # Crea schema e tabelle
    async with engine.begin() as conn:
        # Crea schema airquality
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS airquality"))
        
        # Crea estensione TimescaleDB
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
        
        # Crea tabelle dai modelli
        await conn.run_sync(Base.metadata.create_all)
        
        # Converti measurements in hypertable
        await conn.execute(text("""
            SELECT create_hypertable(
                'airquality.measurements',
                'time',
                if_not_exists => TRUE
            )
        """))
    
    yield engine
    
    await engine.dispose()


@pytest.fixture
async def postgres_session(postgres_engine) -> AsyncGenerator[AsyncSession, None]:
    """Sessione PostgreSQL per integration tests."""
    async_session = sessionmaker(
        postgres_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autocommit=False,
        autoflush=False,
    )
    
    async with async_session() as session:
        yield session
        await session.rollback()


@pytest.fixture
async def postgres_session_with_data(postgres_session) -> AsyncGenerator[AsyncSession, None]:
    """Sessione PostgreSQL con dati di lookup precaricati."""
    from src.database.models import Country, Pollutant, ValidityFlag, VerificationStatus
    
    # Crea countries
    countries = [
        Country(country_code="IT", country_name="Italia", region="Europe"),
        Country(country_code="FR", country_name="Francia", region="Europe"),
    ]
    postgres_session.add_all(countries)
    
    # Crea pollutants
    pollutants = [
        Pollutant(pollutant_code=5, pollutant_name="PM10", pollutant_label="Particulate Matter < 10 µm", unit="µg/m³"),
        Pollutant(pollutant_code=8, pollutant_name="NO2", pollutant_label="Nitrogen Dioxide", unit="µg/m³"),
        Pollutant(pollutant_code=1, pollutant_name="SO2", pollutant_label="Sulphur Dioxide", unit="µg/m³"),
    ]
    postgres_session.add_all(pollutants)
    
    # Crea validity flags
    validity_flags = [
        ValidityFlag(validity_code=1, validity_name="Valid", description="Valid data"),
        ValidityFlag(validity_code=2, validity_name="Invalid", description="Invalid data"),
        ValidityFlag(validity_code=3, validity_name="Unverified", description="Not yet verified"),
    ]
    postgres_session.add_all(validity_flags)
    
    # Crea verification status
    verification_statuses = [
        VerificationStatus(verification_code=1, verification_name="Verified", description="Data verified"),
        VerificationStatus(verification_code=2, verification_name="Preliminary", description="Preliminary data"),
    ]
    postgres_session.add_all(verification_statuses)
    
    await postgres_session.commit()
    
    yield postgres_session


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
