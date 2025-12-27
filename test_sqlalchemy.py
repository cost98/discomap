"""
Test script for new SQLAlchemy database layer.

Demonstrates async database operations with repositories.
"""

import asyncio
from datetime import datetime

from src.database import (
    StationRepository,
    SamplingPointRepository,
    MeasurementRepository,
    get_db_session,
)


async def test_repositories():
    """Test repository pattern."""
    print("🧪 Testing SQLAlchemy repositories...")

    async with get_db_session() as session:
        # Test StationRepository
        station_repo = StationRepository(session)

        # Create a test station
        station_data = {
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

        station = await station_repo.create_or_update(station_data)
        print(f"✅ Created/updated station: {station.station_code}")

        # Test SamplingPointRepository
        sp_repo = SamplingPointRepository(session)

        sp_data = {
            "sampling_point_id": "IT/SPO.TEST001_8_chemi_2023-01-01",
            "station_code": "TEST001",
            "country_code": "IT",
            "instrument_type": "8_chemi",
            "pollutant_code": 8,  # NO2
            "start_date": datetime(2023, 1, 1),
        }

        sp = await sp_repo.create_or_update(sp_data)
        print(f"✅ Created/updated sampling point: {sp.sampling_point_id}")

        # Test MeasurementRepository
        meas_repo = MeasurementRepository(session)

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
            for i in range(24)  # 24 hours
        ]

        count = await meas_repo.create_many(measurements)
        print(f"✅ Inserted {count} measurements")

        # Query measurements
        latest = await meas_repo.get_latest(sp.sampling_point_id, limit=10)
        print(f"✅ Retrieved {len(latest)} latest measurements")

        for m in latest[:3]:
            print(f"   {m.time}: {m.value} {m.unit}")

    print("\n🎉 All tests passed!")


if __name__ == "__main__":
    asyncio.run(test_repositories())
