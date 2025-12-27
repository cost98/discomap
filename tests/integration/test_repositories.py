"""
Tests for repository pattern.
"""

from datetime import datetime

import pytest

from src.database.repositories import (
    MeasurementRepository,
    SamplingPointRepository,
    StationRepository,
)


@pytest.mark.asyncio
async def test_station_create(db_session, sample_station_data):
    """Test station creation."""
    repo = StationRepository(db_session)
    
    station = await repo.create_or_update(sample_station_data)
    
    assert station.station_code == "TEST001"
    assert station.country_code == "IT"
    assert station.station_name == "Test Station"
    assert station.latitude == 45.4642


@pytest.mark.asyncio
async def test_station_update(db_session, sample_station_data):
    """Test station update."""
    repo = StationRepository(db_session)
    
    # Create
    await repo.create_or_update(sample_station_data)
    
    # Update
    updated_data = sample_station_data.copy()
    updated_data["station_name"] = "Updated Station"
    
    station = await repo.create_or_update(updated_data)
    
    assert station.station_name == "Updated Station"


@pytest.mark.asyncio
async def test_station_get_by_code(db_session, sample_station_data):
    """Test station retrieval by code."""
    repo = StationRepository(db_session)
    
    # Create
    await repo.create_or_update(sample_station_data)
    
    # Get
    station = await repo.get_by_code("TEST001")
    
    assert station is not None
    assert station.station_code == "TEST001"


@pytest.mark.asyncio
async def test_sampling_point_create(
    db_session, sample_station_data, sample_sampling_point_data
):
    """Test sampling point creation."""
    # Create station first
    station_repo = StationRepository(db_session)
    await station_repo.create_or_update(sample_station_data)
    
    # Create sampling point
    sp_repo = SamplingPointRepository(db_session)
    sp = await sp_repo.create_or_update(sample_sampling_point_data)
    
    assert sp.sampling_point_id == "IT/SPO.TEST001_8_chemi_2023-01-01"
    assert sp.station_code == "TEST001"
    assert sp.pollutant_code == 8


@pytest.mark.asyncio
async def test_measurement_bulk_insert(
    db_session, sample_station_data, sample_sampling_point_data
):
    """Test bulk measurement insertion."""
    # Setup station and sampling point
    station_repo = StationRepository(db_session)
    await station_repo.create_or_update(sample_station_data)
    
    sp_repo = SamplingPointRepository(db_session)
    sp = await sp_repo.create_or_update(sample_sampling_point_data)
    
    # Create measurements
    meas_repo = MeasurementRepository(db_session)
    
    measurements = [
        {
            "time": datetime(2024, 1, 1, i),
            "sampling_point_id": sp.sampling_point_id,
            "pollutant_code": 8,
            "value": 25.5 + i,
            "unit": "µg/m³",
            "aggregation_type": "hour",
            "validity": 1,
            "verification": 1,
        }
        for i in range(24)
    ]
    
    count = await meas_repo.create_many(measurements)
    
    assert count == 24


@pytest.mark.asyncio
async def test_measurement_get_latest(
    db_session, sample_station_data, sample_sampling_point_data
):
    """Test retrieving latest measurements."""
    # Setup
    station_repo = StationRepository(db_session)
    await station_repo.create_or_update(sample_station_data)
    
    sp_repo = SamplingPointRepository(db_session)
    sp = await sp_repo.create_or_update(sample_sampling_point_data)
    
    meas_repo = MeasurementRepository(db_session)
    
    measurements = [
        {
            "time": datetime(2024, 1, 1, i),
            "sampling_point_id": sp.sampling_point_id,
            "pollutant_code": 8,
            "value": 25.5 + i,
            "unit": "µg/m³",
        }
        for i in range(10)
    ]
    
    await meas_repo.create_many(measurements)
    
    # Get latest
    latest = await meas_repo.get_latest(sp.sampling_point_id, limit=5)
    
    assert len(latest) == 5
    # Should be in descending order
    assert latest[0].time > latest[1].time
