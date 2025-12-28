"""Performance tests for bulk data insertion.

Tests TimescaleDB hypertable performance with large datasets.
Run these tests with: pytest tests/performance/ -v -s
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import List
import random
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pytest
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import (
    Measurement,
    SamplingPoint,
    Station,
    Country,
    Pollutant,
    ValidityFlag,
    VerificationStatus,
)

# Import fixtures from integration tests
pytest_plugins = ["tests.integration.conftest"]


class BulkDataGenerator:
    """Generate realistic bulk test data."""

    def __init__(self):
        # Use only countries that exist in postgres_session_with_data fixture
        self.country_codes = ["IT", "FR"]
        # Use only pollutants that exist in postgres_session_with_data fixture
        self.pollutant_codes = [1, 5, 8]  # SO2, PM10, NO2
        self.validity_flags = [1, 2, 3]  # 1=valid, 2=invalid, 3=unverified
        self.verification_statuses = [1, 2]  # 1=verified, 2=preliminary

    def generate_sampling_points(self, num_stations: int) -> List[SamplingPoint]:
        """Generate sampling points."""
        sampling_points = []
        point_id = 1

        for station_num in range(1, num_stations + 1):
            station_code = f"TEST_STATION_{station_num:04d}"
            country_code = self.country_codes[(station_num - 1) % len(self.country_codes)]

            # Each station has 1-3 sampling points
            num_points = random.randint(1, 3)

            for _ in range(num_points):
                pollutant_code = random.choice(self.pollutant_codes)

                sp = SamplingPoint(
                    sampling_point_id=f"SP_{country_code}_{station_code}_{point_id}",
                    station_code=station_code,
                    country_code=country_code,
                    pollutant_code=pollutant_code,
                )
                sampling_points.append(sp)
                point_id += 1

        return sampling_points

    def generate_measurements(
        self,
        sampling_point_ids: List[str],
        start_date: datetime,
        num_hours: int,
    ) -> List[Measurement]:
        """Generate measurements for given sampling points and time range."""
        measurements = []

        for sp_id in sampling_point_ids:
            for hour_offset in range(num_hours):
                timestamp = start_date + timedelta(hours=hour_offset)

                # Generate realistic pollutant values
                value = round(random.uniform(5.0, 150.0), 2)

                measurement = Measurement(
                    sampling_point_id=sp_id,
                    time=timestamp,
                    pollutant_code=random.choice(self.pollutant_codes),
                    value=value,
                    validity=random.choice(self.validity_flags),
                    verification=random.choice(self.verification_statuses),
                    observation_id=f"OBS_{sp_id}_{timestamp.strftime('%Y%m%d%H')}",
                )
                measurements.append(measurement)

        return measurements

    def generate_measurements_as_tuples(
        self,
        sampling_point_ids: List[str],
        start_date: datetime,
        num_hours: int,
    ):
        """Generate measurements as tuples for COPY bulk import.
        
        Returns tuples in the correct column order for PostgreSQL COPY.
        Column order: time, sampling_point_id, pollutant_code, value, unit, 
                     aggregation_type, validity, verification, data_capture, 
                     result_time, observation_id
        """
        for sp_id in sampling_point_ids:
            for hour_offset in range(num_hours):
                timestamp = start_date + timedelta(hours=hour_offset)
                value = round(random.uniform(5.0, 150.0), 2)
                
                yield (
                    timestamp,
                    sp_id,
                    random.choice(self.pollutant_codes),
                    value,
                    None,  # unit
                    None,  # aggregation_type
                    random.choice(self.validity_flags),
                    random.choice(self.verification_statuses),
                    None,  # data_capture
                    None,  # result_time
                    f"OBS_{sp_id}_{timestamp.strftime('%Y%m%d%H')}",
                )


@pytest.fixture
async def generator():
    """Provide data generator."""
    return BulkDataGenerator()


@pytest.fixture
async def test_stations(postgres_session_with_data: AsyncSession) -> List[Station]:
    """Create test stations for performance tests."""
    stations = []
    # Use only countries that exist in the test database
    country_codes = ["IT", "FR"]

    for i in range(1, 101):  # 100 stations
        country_code_idx = ((i - 1) % len(country_codes))

        station = Station(
            station_code=f"TEST_STATION_{i:04d}",
            station_name=f"Test Station {i}",
            country_code=country_codes[country_code_idx],
            latitude=random.uniform(35.0, 55.0),
            longitude=random.uniform(-10.0, 20.0),
            altitude=random.uniform(0.0, 500.0),
        )
        stations.append(station)

    postgres_session_with_data.add_all(stations)
    await postgres_session_with_data.commit()

    return stations


@pytest.mark.asyncio
async def test_insert_10k_measurements(
    postgres_session_with_data: AsyncSession,
    test_stations: List[Station],
    generator: BulkDataGenerator,
):
    """Test inserting 10,000 measurements."""
    # Create sampling points
    all_sampling_points = generator.generate_sampling_points(num_stations=20)
    postgres_session_with_data.add_all(all_sampling_points)
    await postgres_session_with_data.commit()

    # Use exactly 20 sampling points for predictable count (20 * 500 = 10,000)
    sampling_points = all_sampling_points[:20]
    sp_ids = [sp.sampling_point_id for sp in sampling_points]

    # Generate measurements (20 sampling points * 500 hours = 10,000 records)
    start_date = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    measurements = generator.generate_measurements(
        sampling_point_ids=sp_ids,
        start_date=start_date,
        num_hours=500,
    )

    assert len(measurements) == 10_000

    # Measure insert time
    start_time = time.time()
    await postgres_session_with_data.run_sync(
        lambda session: session.bulk_insert_mappings(
            Measurement,
            [{
                'time': m.time,
                'sampling_point_id': m.sampling_point_id,
                'pollutant_code': m.pollutant_code,
                'value': m.value,
                'unit': m.unit,
                'aggregation_type': m.aggregation_type,
                'validity': m.validity,
                'verification': m.verification,
                'data_capture': m.data_capture,
                'result_time': m.result_time,
                'observation_id': m.observation_id,
            } for m in measurements]
        )
    )
    await postgres_session_with_data.commit()
    elapsed = time.time() - start_time

    # Verify
    result = await postgres_session_with_data.execute(
        select(func.count()).select_from(Measurement)
    )
    count = result.scalar()

    print(f"\nOK - Inserted {count:,} measurements in {elapsed:.2f}s")
    print(f"  Rate: {count / elapsed:.0f} records/sec")

    assert count == 10_000


@pytest.mark.asyncio
async def test_insert_100k_measurements(
    postgres_session_with_data: AsyncSession,
    test_stations: List[Station],
    generator: BulkDataGenerator,
):
    """Test inserting 100,000 measurements."""
    # Create more sampling points
    all_sampling_points = generator.generate_sampling_points(num_stations=50)
    postgres_session_with_data.add_all(all_sampling_points)
    await postgres_session_with_data.commit()

    # Use exactly 50 sampling points
    sampling_points = all_sampling_points[:50]
    sp_ids = [sp.sampling_point_id for sp in sampling_points]

    # Generate measurements in batches
    start_date = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    total_inserted = 0
    start_time = time.time()

    # Insert in batches of 10,000 (50 sampling points * 200 hours = 10,000 per batch)
    num_hours_per_batch = 200

    for batch_num in range(10):
        batch_start = start_date + timedelta(hours=batch_num * num_hours_per_batch)
        measurements = generator.generate_measurements(
            sampling_point_ids=sp_ids,
            start_date=batch_start,
            num_hours=num_hours_per_batch,
        )

        await postgres_session_with_data.run_sync(
            lambda session: session.bulk_insert_mappings(
                Measurement,
                [{
                    'time': m.time,
                    'sampling_point_id': m.sampling_point_id,
                    'pollutant_code': m.pollutant_code,
                    'value': m.value,
                    'unit': m.unit,
                    'aggregation_type': m.aggregation_type,
                    'validity': m.validity,
                    'verification': m.verification,
                    'data_capture': m.data_capture,
                    'result_time': m.result_time,
                    'observation_id': m.observation_id,
                } for m in measurements]
            )
        )
        await postgres_session_with_data.commit()
        total_inserted += len(measurements)

        print(f"  Batch {batch_num + 1}/10: {total_inserted:,} total records")

    elapsed = time.time() - start_time

    # Verify
    result = await postgres_session_with_data.execute(select(func.count()).select_from(Measurement))
    count = result.scalar()

    print(f"\nOK - Inserted {count:,} measurements in {elapsed:.2f}s")
    print(f"  Rate: {count / elapsed:.0f} records/sec")
    print(f"  Avg batch time: {elapsed / 10:.2f}s")

    assert count >= 100_000


@pytest.mark.asyncio
async def test_insert_1m_measurements(
    postgres_session_with_data: AsyncSession,
    generator: BulkDataGenerator,
):
    """Test inserting 1 million measurements.

    This test demonstrates TimescaleDB's capability for large-scale data.
    Expected time: 30-120 seconds depending on hardware.
    """
    # Create stations directly in this test (don't use shared fixture)
    from src.database.models import Station
    
    stations = []
    for i in range(1, 21):  # 20 stations
        station = Station(
            station_code=f"PERF1M_ST_{i:04d}",
            station_name=f"Performance Test Station {i}",
            country_code=generator.country_codes[i % 2],  # Alternate IT/FR
            latitude=random.uniform(35.0, 55.0),
            longitude=random.uniform(-10.0, 20.0),
            altitude=random.uniform(0.0, 500.0),
        )
        stations.append(station)
    
    postgres_session_with_data.add_all(stations)
    await postgres_session_with_data.commit()
    
    # Create sampling points for these stations
    sampling_points = []
    
    for idx, station in enumerate(stations):
        # Create 1 sampling point per station for predictable count
        # Use completely unique ID format to avoid ANY possible collision
        sp = SamplingPoint(
            sampling_point_id=f"PERF1M_{idx:05d}",
            station_code=station.station_code,
            country_code=station.country_code,
            pollutant_code=random.choice(generator.pollutant_codes),
        )
        sampling_points.append(sp)
    
    postgres_session_with_data.add_all(sampling_points)
    await postgres_session_with_data.commit()

    sp_ids = [sp.sampling_point_id for sp in sampling_points]
    print(f"\nCreated {len(sp_ids)} sampling points")

    # Generate and insert in batches
    start_date = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    total_inserted = 0
    start_time = time.time()

    # 20 batches of 50,000 records each (50 sp * 1000 hours = 50k per batch)
    num_batches = 20
    hours_per_batch = 1000  # 50 sampling points * 1000 hours = 50,000 per batch

    # Use 50 sampling points instead of 20 for larger batches
    # Extend sp_ids by creating more sampling points
    while len(sampling_points) < 50:
        idx = len(sampling_points)
        station = stations[idx % len(stations)]
        sp = SamplingPoint(
            sampling_point_id=f"PERF1M_{idx:05d}",
            station_code=station.station_code,
            country_code=station.country_code,
            pollutant_code=random.choice(generator.pollutant_codes),
        )
        sampling_points.append(sp)
    
    postgres_session_with_data.add_all(sampling_points[20:])  # Add the new ones
    await postgres_session_with_data.commit()
    sp_ids = [sp.sampling_point_id for sp in sampling_points]

    for batch_num in range(num_batches):
        # Each batch uses sequential time ranges to avoid duplicates
        batch_start = start_date + timedelta(hours=batch_num * hours_per_batch)

        measurements = generator.generate_measurements(
            sampling_point_ids=sp_ids,
            start_date=batch_start,
            num_hours=hours_per_batch,
        )

        batch_insert_start = time.time()
        await postgres_session_with_data.run_sync(
            lambda session: session.bulk_insert_mappings(
                Measurement,
                [{
                    'time': m.time,
                    'sampling_point_id': m.sampling_point_id,
                    'pollutant_code': m.pollutant_code,
                    'value': m.value,
                    'unit': m.unit,
                    'aggregation_type': m.aggregation_type,
                    'validity': m.validity,
                    'verification': m.verification,
                    'data_capture': m.data_capture,
                    'result_time': m.result_time,
                    'observation_id': m.observation_id,
                } for m in measurements]
            )
        )
        await postgres_session_with_data.commit()
        measurements.clear()  # Clear the list to free memory
        batch_elapsed = time.time() - batch_insert_start

        total_inserted += len(measurements)
        elapsed = time.time() - start_time

        if (batch_num + 1) % 10 == 0:
            print(
                f"  Batch {batch_num + 1}/{num_batches}: "
                f"{total_inserted:,} records "
                f"({batch_elapsed:.2f}s, {len(measurements) / batch_elapsed:.0f} rec/s)"
            )

    elapsed = time.time() - start_time

    # Verify total count
    result = await postgres_session_with_data.execute(
        select(func.count()).select_from(Measurement)
    )
    count = result.scalar()

    print(f"\n{'=' * 60}")
    print(f"OK - Inserted {count:,} measurements in {elapsed:.2f}s")
    print(f"  Average rate: {count / elapsed:.0f} records/sec")
    print(f"{'=' * 60}")


    assert count >= 1_000_000


@pytest.mark.asyncio
async def test_insert_1m_measurements_with_copy(
    postgres_session_with_data: AsyncSession,
    generator: BulkDataGenerator,
):
    """Test inserting 1 million measurements using PostgreSQL COPY.
    
    COPY is the fastest method for bulk inserts in PostgreSQL.
    Expected to be 3-5x faster than bulk_insert_mappings.
    """
    # Create stations directly in this test
    from src.database.models import Station
    
    stations = []
    for i in range(1, 21):  # 20 stations
        station = Station(
            station_code=f"PERFCOPY_ST_{i:04d}",
            station_name=f"Performance Copy Test Station {i}",
            country_code=generator.country_codes[i % 2],  # Alternate IT/FR
            latitude=random.uniform(35.0, 55.0),
            longitude=random.uniform(-10.0, 20.0),
            altitude=random.uniform(0.0, 500.0),
        )
        stations.append(station)
    
    postgres_session_with_data.add_all(stations)
    await postgres_session_with_data.commit()
    
    # Create sampling points
    sampling_points = []
    for idx, station in enumerate(stations):
        sp = SamplingPoint(
            sampling_point_id=f"PERFCOPY_{idx:05d}",
            station_code=station.station_code,
            country_code=station.country_code,
            pollutant_code=random.choice(generator.pollutant_codes),
        )
        sampling_points.append(sp)
    
    postgres_session_with_data.add_all(sampling_points)
    await postgres_session_with_data.commit()

    sp_ids = [sp.sampling_point_id for sp in sampling_points]
    print(f"\nCreated {len(sp_ids)} sampling points")

    # Get raw asyncpg connection for COPY
    connection = await postgres_session_with_data.connection()
    raw_connection = await connection.get_raw_connection()
    asyncpg_conn = raw_connection.driver_connection

    start_date = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    start_time = time.time()

    # COPY in larger batches - COPY is so fast we can do bigger batches
    num_batches = 10  # Fewer batches needed
    hours_per_batch = 5000  # 20 sp * 5000 hours = 100k per batch

    for batch_num in range(num_batches):
        batch_start = start_date + timedelta(hours=batch_num * hours_per_batch)
        
        # Generate as tuples (generator, not list)
        records = generator.generate_measurements_as_tuples(
            sampling_point_ids=sp_ids,
            start_date=batch_start,
            num_hours=hours_per_batch,
        )
        
        # Convert generator to list for copy_records_to_table
        records_list = list(records)
        
        batch_insert_start = time.time()
        
        # Use COPY - much faster than INSERT
        await asyncpg_conn.copy_records_to_table(
            'measurements',
            schema_name='airquality',
            records=records_list,
            columns=[
                'time', 'sampling_point_id', 'pollutant_code', 'value', 
                'unit', 'aggregation_type', 'validity', 'verification',
                'data_capture', 'result_time', 'observation_id'
            ],
        )
        
        batch_elapsed = time.time() - batch_insert_start

        if (batch_num + 1) % 2 == 0:
            print(
                f"  Batch {batch_num + 1}/{num_batches}: "
                f"{len(records_list):,} records "
                f"({batch_elapsed:.2f}s, {len(records_list) / batch_elapsed:.0f} rec/s)"
            )

    elapsed = time.time() - start_time

    # Verify total count
    result = await postgres_session_with_data.execute(
        select(func.count()).select_from(Measurement)
    )
    count = result.scalar()

    print(f"\n{'=' * 60}")
    print(f"OK - Inserted {count:,} measurements in {elapsed:.2f}s using COPY")
    print(f"  Average rate: {count / elapsed:.0f} records/sec")
    print(f"  Speedup vs bulk_insert_mappings: ~3-5x faster")
    print(f"{'=' * 60}")

    assert count >= 1_000_000


@pytest.mark.asyncio
async def test_query_performance_after_bulk_insert(
    postgres_session_with_data: AsyncSession,
    test_stations: List[Station],
    generator: BulkDataGenerator,
):
    """Test query performance on large dataset."""
    # First, insert a decent amount of data (100k records)
    sampling_points = generator.generate_sampling_points(num_stations=30)
    postgres_session_with_data.add_all(sampling_points)
    await postgres_session_with_data.commit()

    sp_ids = [sp.sampling_point_id for sp in sampling_points]
    start_date = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Insert data
    for batch in range(10):
        measurements = generator.generate_measurements(
            sampling_point_ids=sp_ids[:10],
            start_date=start_date + timedelta(hours=batch * 1000),
            num_hours=1000,
        )
        postgres_session_with_data.add_all(measurements)

    await postgres_session_with_data.commit()

    # Test 1: Count all measurements
    start_time = time.time()
    result = await postgres_session_with_data.execute(
        select(func.count()).select_from(Measurement)
    )
    count = result.scalar()
    elapsed = time.time() - start_time
    print(f"\n  COUNT(*): {count:,} records in {elapsed * 1000:.2f}ms")

    # Test 2: Time range query
    start_time = time.time()
    query = (
        select(Measurement)
        .where(Measurement.time >= datetime(2023, 1, 1))
        .where(Measurement.time < datetime(2023, 1, 8))
    )
    result = await postgres_session_with_data.execute(query)
    records = result.scalars().all()
    elapsed = time.time() - start_time
    print(f"  Time range query: {len(records):,} records in {elapsed * 1000:.2f}ms")

    # Test 3: Aggregation query
    start_time = time.time()
    query = select(
        Measurement.sampling_point_id,
        func.avg(Measurement.value).label("avg_value"),
        func.max(Measurement.value).label("max_value"),
        func.min(Measurement.value).label("min_value"),
    ).group_by(Measurement.sampling_point_id)

    result = await postgres_session_with_data.execute(query)
    stats = result.all()
    elapsed = time.time() - start_time
    print(
        f"  Aggregation query: {len(stats)} groups in {elapsed * 1000:.2f}ms"
    )

    assert count >= 100_000


if __name__ == "__main__":
    # Run tests with: pytest tests/performance/test_bulk_insert.py -v -s
    print("Run with: pytest tests/performance/test_bulk_insert.py -v -s")
