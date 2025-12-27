"""
Integration tests con PostgreSQL reale + TimescaleDB.

Usano testcontainers per tirare su PostgreSQL automaticamente.
"""

import pytest
from datetime import datetime, timezone

from src.database.repositories import MeasurementRepository, StationRepository


@pytest.mark.asyncio
async def test_station_crud_postgres(postgres_session_with_data, sample_station_data):
    """Test CRUD su Station con PostgreSQL reale."""
    repo = StationRepository(postgres_session_with_data)
    
    # Create
    station = await repo.create_or_update(sample_station_data)
    assert station.station_code == "TEST001"
    assert station.country_code == "IT"
    
    # Read
    found = await repo.get_by_code("TEST001")
    assert found is not None
    assert found.station_name == "Test Station"
    
    # Update
    sample_station_data["station_name"] = "Updated Station"
    updated = await repo.create_or_update(sample_station_data)
    assert updated.station_name == "Updated Station"


@pytest.mark.asyncio
async def test_bulk_insert_timescaledb(postgres_session_with_data):
    """Test bulk insert su hypertable TimescaleDB."""
    from src.database.models import SamplingPoint
    
    repo = MeasurementRepository(postgres_session_with_data)
    
    # Crea sampling points necessari
    sampling_points = [
        SamplingPoint(
            sampling_point_id=f"IT/SPO.TEST{i:03d}_8_chemi_2023-01-01",
            country_code="IT",
            pollutant_code=8
        )
        for i in range(10)
    ]
    postgres_session_with_data.add_all(sampling_points)
    await postgres_session_with_data.commit()
    
    # Crea 1000 measurements
    measurements = [
        {
            "time": datetime(2023, 1, 1, i // 60, i % 60, tzinfo=timezone.utc),
            "sampling_point_id": f"IT/SPO.TEST{i % 10:03d}_8_chemi_2023-01-01",
            "country_code": "IT",
            "pollutant_code": 8,
            "value": 25.5 + i * 0.1,
            "validity_code": 1,
            "verification_code": 1,
        }
        for i in range(1000)
    ]
    
    # Bulk insert
    count = await repo.bulk_insert(measurements)
    assert count == 1000
    
    # Verifica inserimento
    await postgres_session_with_data.commit()
    latest = await repo.get_latest("IT/SPO.TEST000_8_chemi_2023-01-01", limit=10)
    assert len(latest) > 0


@pytest.mark.asyncio
async def test_time_range_delete(postgres_session_with_data):
    """Test delete di range temporali."""
    from src.database.models import SamplingPoint
    
    repo = MeasurementRepository(postgres_session_with_data)
    
    # Crea sampling point necessario
    sp = SamplingPoint(
        sampling_point_id="IT/SPO.TEST_DELETE",
        country_code="IT",
        pollutant_code=5
    )
    postgres_session_with_data.add(sp)
    await postgres_session_with_data.commit()
    
    # Inserisci dati
    measurements = [
        {
            "time": datetime(2023, 1, 1, hour, 0, tzinfo=timezone.utc),
            "sampling_point_id": "IT/SPO.TEST_DELETE",
            "country_code": "IT",
            "pollutant_code": 5,
            "value": 10.0,
            "validity_code": 1,
            "verification_code": 1,
        }
        for hour in range(24)
    ]
    
    await repo.bulk_insert(measurements)
    await postgres_session_with_data.commit()
    
    # Delete ore 10-15
    deleted = await repo.delete_time_range(
        "IT/SPO.TEST_DELETE",
        datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc),
        datetime(2023, 1, 1, 15, 0, tzinfo=timezone.utc),
    )
    await postgres_session_with_data.commit()
    
    # Verifica che siano state cancellate 6 ore (10,11,12,13,14,15)
    assert deleted == 6
    
    # Verifica che rimangano le altre
    remaining = await repo.get_latest("IT/SPO.TEST_DELETE", limit=100)
    assert len(remaining) == 18  # 24 - 6
