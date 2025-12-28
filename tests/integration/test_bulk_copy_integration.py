"""Integration tests for COPY bulk import with real database."""

import pytest
from datetime import datetime, timezone
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.bulk_import import (
    bulk_copy_measurements,
    bulk_copy_sampling_points,
    bulk_copy_stations,
)
from src.database.models import Measurement, SamplingPoint, Station


@pytest.mark.asyncio
async def test_bulk_copy_stations_integration(postgres_session_with_data: AsyncSession):
    """Test bulk_copy_stations with real database."""
    
    # Prepare test data as tuples
    stations = [
        ('COPY_TEST_ST_001', 'Copy Test Station 1', 'IT', 45.0, 9.0, 100.0),
        ('COPY_TEST_ST_002', 'Copy Test Station 2', 'FR', 48.0, 2.0, 50.0),
        ('COPY_TEST_ST_003', 'Copy Test Station 3', 'IT', 41.0, 12.0, 200.0),
    ]
    
    # Execute COPY
    total = await bulk_copy_stations(postgres_session_with_data, stations)
    
    assert total == 3
    
    # Verify data was inserted
    result = await postgres_session_with_data.execute(
        select(func.count()).select_from(Station).where(
            Station.station_code.like('COPY_TEST_ST_%')
        )
    )
    count = result.scalar()
    assert count == 3
    
    # Verify specific station
    result = await postgres_session_with_data.execute(
        select(Station).where(Station.station_code == 'COPY_TEST_ST_001')
    )
    station = result.scalar_one()
    assert station.station_name == 'Copy Test Station 1'
    assert station.country_code == 'IT'
    assert station.latitude == 45.0


@pytest.mark.asyncio
async def test_bulk_copy_sampling_points_integration(postgres_session_with_data: AsyncSession):
    """Test bulk_copy_sampling_points with real database."""
    
    # First create a station
    station = Station(
        station_code='COPY_SP_TEST_ST',
        station_name='Station for SP Test',
        country_code='IT',
        latitude=45.0,
        longitude=9.0,
        altitude=100.0,
    )
    postgres_session_with_data.add(station)
    await postgres_session_with_data.commit()
    
    # Prepare sampling points as tuples
    sampling_points = [
        ('COPY_SP_001', 'COPY_SP_TEST_ST', 'IT', 5),
        ('COPY_SP_002', 'COPY_SP_TEST_ST', 'IT', 8),
        ('COPY_SP_003', 'COPY_SP_TEST_ST', 'IT', 1),
    ]
    
    # Execute COPY
    total = await bulk_copy_sampling_points(postgres_session_with_data, sampling_points)
    
    assert total == 3
    
    # Verify data
    result = await postgres_session_with_data.execute(
        select(func.count()).select_from(SamplingPoint).where(
            SamplingPoint.sampling_point_id.like('COPY_SP_%')
        )
    )
    count = result.scalar()
    assert count == 3


@pytest.mark.asyncio
async def test_bulk_copy_measurements_integration(postgres_session_with_data: AsyncSession):
    """Test bulk_copy_measurements with real database."""
    
    # Create station and sampling point
    station = Station(
        station_code='COPY_MEAS_ST',
        station_name='Station for Measurement Test',
        country_code='IT',
        latitude=45.0,
        longitude=9.0,
        altitude=100.0,
    )
    postgres_session_with_data.add(station)
    await postgres_session_with_data.commit()
    
    sp = SamplingPoint(
        sampling_point_id='COPY_MEAS_SP',
        station_code='COPY_MEAS_ST',
        country_code='IT',
        pollutant_code=5,
    )
    postgres_session_with_data.add(sp)
    await postgres_session_with_data.commit()
    
    # Prepare measurements as tuples
    base_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    
    def generate_measurements():
        from datetime import timedelta
        for i in range(1000):
            yield (
                base_time + timedelta(hours=i),
                'COPY_MEAS_SP',
                5,  # pollutant_code
                40.0 + (i % 10),
                'µg/m³',
                'hour',
                1,  # validity
                1,  # verification
                95.0,
                None,
                f'COPY_OBS_{i:05d}',
            )
    
    # Execute COPY
    total = await bulk_copy_measurements(
        postgres_session_with_data,
        generate_measurements(),
        batch_size=500,
    )
    
    assert total == 1000
    
    # Verify data
    result = await postgres_session_with_data.execute(
        select(func.count()).select_from(Measurement).where(
            Measurement.sampling_point_id == 'COPY_MEAS_SP'
        )
    )
    count = result.scalar()
    assert count == 1000
    
    # Verify a specific measurement
    result = await postgres_session_with_data.execute(
        select(Measurement).where(
            Measurement.observation_id == 'COPY_OBS_00000'
        )
    )
    measurement = result.scalar_one()
    assert measurement.value == 40.0
    assert measurement.pollutant_code == 5
    assert measurement.validity == 1


@pytest.mark.asyncio
async def test_bulk_copy_large_dataset_performance(postgres_session_with_data: AsyncSession):
    """Test COPY performance with 100k measurements."""
    
    # Setup
    station = Station(
        station_code='PERF_COPY_ST',
        station_name='Performance Test Station',
        country_code='IT',
        latitude=45.0,
        longitude=9.0,
        altitude=100.0,
    )
    postgres_session_with_data.add(station)
    await postgres_session_with_data.commit()
    
    # Create 10 sampling points
    sampling_points = [
        (f'PERF_COPY_SP_{i:03d}', 'PERF_COPY_ST', 'IT', 5)
        for i in range(10)
    ]
    await bulk_copy_sampling_points(postgres_session_with_data, sampling_points)
    
    # Generate 100k measurements (10 sp × 10,000 hours)
    base_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    
    def generate_measurements():
        from datetime import timedelta
        for sp_idx in range(10):
            sp_id = f'PERF_COPY_SP_{sp_idx:03d}'
            for hour in range(10_000):
                yield (
                    base_time + timedelta(hours=hour),
                    sp_id,
                    5,
                    40.0 + (hour % 50),
                    None, None,
                    1, 1,
                    None, None,
                    f'PERF_OBS_{sp_idx}_{hour}',
                )
    
    import time
    start = time.time()
    
    total = await bulk_copy_measurements(
        postgres_session_with_data,
        generate_measurements(),
        batch_size=50_000,
    )
    
    elapsed = time.time() - start
    rate = total / elapsed if elapsed > 0 else 0
    
    assert total == 100_000
    
    # Verify performance: should be > 5,000 rec/sec (testcontainers are slower)
    assert rate > 5_000, f"Rate too slow: {rate:.0f} rec/s"
    
    print(f"\n  COPY Performance: {total:,} records in {elapsed:.2f}s ({rate:.0f} rec/s)")
    
    # Verify count
    result = await postgres_session_with_data.execute(
        select(func.count()).select_from(Measurement).where(
            Measurement.sampling_point_id.like('PERF_COPY_SP_%')
        )
    )
    count = result.scalar()
    assert count == 100_000


@pytest.mark.asyncio
async def test_bulk_copy_measurements_with_duplicates_fails(postgres_session_with_data: AsyncSession):
    """Test that COPY fails properly on duplicate primary keys."""
    
    # Setup
    station = Station(
        station_code='DUP_TEST_ST',
        station_name='Duplicate Test Station',
        country_code='IT',
        latitude=45.0,
        longitude=9.0,
        altitude=100.0,
    )
    postgres_session_with_data.add(station)
    await postgres_session_with_data.commit()
    
    sp = SamplingPoint(
        sampling_point_id='DUP_TEST_SP',
        station_code='DUP_TEST_ST',
        country_code='IT',
        pollutant_code=5,
    )
    postgres_session_with_data.add(sp)
    await postgres_session_with_data.commit()
    
    # Insert same measurement twice (duplicate PK)
    duplicate_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    
    measurements = [
        (duplicate_time, 'DUP_TEST_SP', 5, 40.0, None, None, 1, 1, None, None, 'DUP_OBS_1'),
        (duplicate_time, 'DUP_TEST_SP', 5, 41.0, None, None, 1, 1, None, None, 'DUP_OBS_2'),
    ]
    
    # Should raise exception due to duplicate PK
    with pytest.raises(Exception):  # asyncpg will raise an exception
        await bulk_copy_measurements(postgres_session_with_data, measurements)
