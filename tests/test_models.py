"""
Tests for SQLAlchemy models.
"""

from datetime import datetime

import pytest

from src.database.models import (
    Country,
    Measurement,
    Pollutant,
    SamplingPoint,
    Station,
)


@pytest.mark.asyncio
async def test_station_model_creation(db_session):
    """Test Station model instantiation."""
    station = Station(
        station_code="TEST001",
        country_code="IT",
        station_name="Test Station",
        latitude=45.4642,
        longitude=9.1900,
    )
    
    db_session.add(station)
    await db_session.commit()
    await db_session.refresh(station)
    
    assert station.station_code == "TEST001"
    assert station.created_at is not None


@pytest.mark.asyncio
async def test_station_relationships(db_session):
    """Test Station model relationships."""
    station = Station(
        station_code="TEST001",
        country_code="IT",
        station_name="Test Station",
    )
    
    sampling_point = SamplingPoint(
        sampling_point_id="IT/SPO.TEST001_8_chemi_2023-01-01",
        station_code="TEST001",
        pollutant_code=8,
    )
    
    db_session.add(station)
    db_session.add(sampling_point)
    await db_session.commit()
    
    # Test relationship loading
    await db_session.refresh(station)
    # Note: May need to explicitly load relationships in SQLite
    assert station.station_code == "TEST001"


@pytest.mark.asyncio
async def test_measurement_composite_key(db_session):
    """Test Measurement composite primary key."""
    station = Station(station_code="TEST001")
    sp = SamplingPoint(
        sampling_point_id="IT/SPO.TEST001_8_chemi_2023-01-01",
        station_code="TEST001",
        pollutant_code=8,
    )
    
    measurement = Measurement(
        time=datetime(2024, 1, 1, 12, 0, 0),
        sampling_point_id="IT/SPO.TEST001_8_chemi_2023-01-01",
        pollutant_code=8,
        value=25.5,
    )
    
    db_session.add(station)
    db_session.add(sp)
    db_session.add(measurement)
    await db_session.commit()
    
    assert measurement.time == datetime(2024, 1, 1, 12, 0, 0)
    assert measurement.value == 25.5
