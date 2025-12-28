"""Unit tests for bulk_import utilities."""

import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.database.bulk_import import (
    bulk_copy_measurements,
    bulk_copy_sampling_points,
    bulk_copy_stations,
)


@pytest.mark.asyncio
async def test_bulk_copy_measurements_with_list():
    """Test bulk_copy_measurements with list of records."""
    # Mock session and connections
    mock_session = AsyncMock()
    mock_connection = AsyncMock()
    mock_raw_connection = MagicMock()
    mock_asyncpg_conn = AsyncMock()
    
    mock_session.connection.return_value = mock_connection
    mock_connection.get_raw_connection.return_value = mock_raw_connection
    mock_raw_connection.driver_connection = mock_asyncpg_conn
    
    # Test data
    records = [
        (
            datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            'SP_001',
            5,  # pollutant_code
            42.5,  # value
            None, None,  # unit, aggregation_type
            1, 1,  # validity, verification
            None, None,  # data_capture, result_time
            'OBS_001'
        ),
        (
            datetime(2023, 1, 1, 1, 0, 0, tzinfo=timezone.utc),
            'SP_001',
            5,
            43.2,
            None, None,
            1, 1,
            None, None,
            'OBS_002'
        ),
    ]
    
    # Execute
    total = await bulk_copy_measurements(mock_session, records, batch_size=100)
    
    # Verify
    assert total == 2
    mock_asyncpg_conn.copy_records_to_table.assert_called_once()
    call_args = mock_asyncpg_conn.copy_records_to_table.call_args
    assert call_args[1]['schema_name'] == 'airquality'
    assert call_args[1]['records'] == records
    assert len(call_args[1]['columns']) == 11


@pytest.mark.asyncio
async def test_bulk_copy_measurements_with_generator():
    """Test bulk_copy_measurements with generator."""
    mock_session = AsyncMock()
    mock_connection = AsyncMock()
    mock_raw_connection = MagicMock()
    mock_asyncpg_conn = AsyncMock()
    
    mock_session.connection.return_value = mock_connection
    mock_connection.get_raw_connection.return_value = mock_raw_connection
    mock_raw_connection.driver_connection = mock_asyncpg_conn
    
    # Generator function
    def record_generator():
        for i in range(5):
            yield (
                datetime(2023, 1, 1, i, 0, 0, tzinfo=timezone.utc),
                f'SP_{i:03d}',
                5,
                40.0 + i,
                None, None,
                1, 1,
                None, None,
                f'OBS_{i:03d}'
            )
    
    # Execute with small batch size to test batching
    total = await bulk_copy_measurements(
        mock_session, 
        record_generator(), 
        batch_size=2
    )
    
    # Verify
    assert total == 5
    # Should be called 3 times: 2 + 2 + 1
    assert mock_asyncpg_conn.copy_records_to_table.call_count == 3


@pytest.mark.asyncio
async def test_bulk_copy_sampling_points():
    """Test bulk_copy_sampling_points."""
    mock_session = AsyncMock()
    mock_connection = AsyncMock()
    mock_raw_connection = MagicMock()
    mock_asyncpg_conn = AsyncMock()
    
    mock_session.connection.return_value = mock_connection
    mock_connection.get_raw_connection.return_value = mock_raw_connection
    mock_raw_connection.driver_connection = mock_asyncpg_conn
    
    records = [
        ('SP_001', 'ST_001', 'IT', 5),
        ('SP_002', 'ST_001', 'IT', 8),
        ('SP_003', 'ST_002', 'FR', 5),
    ]
    
    total = await bulk_copy_sampling_points(mock_session, records)
    
    assert total == 3
    call_args = mock_asyncpg_conn.copy_records_to_table.call_args
    assert call_args[0][0] == 'sampling_points'
    assert call_args[1]['schema_name'] == 'airquality'
    assert call_args[1]['records'] == records
    assert call_args[1]['columns'] == [
        'sampling_point_id', 'station_code', 'country_code', 'pollutant_code'
    ]


@pytest.mark.asyncio
async def test_bulk_copy_stations():
    """Test bulk_copy_stations."""
    mock_session = AsyncMock()
    mock_connection = AsyncMock()
    mock_raw_connection = MagicMock()
    mock_asyncpg_conn = AsyncMock()
    
    mock_session.connection.return_value = mock_connection
    mock_connection.get_raw_connection.return_value = mock_raw_connection
    mock_raw_connection.driver_connection = mock_asyncpg_conn
    
    records = [
        ('ST_001', 'Station 1', 'IT', 45.0, 9.0, 100.0),
        ('ST_002', 'Station 2', 'FR', 48.0, 2.0, 50.0),
    ]
    
    total = await bulk_copy_stations(mock_session, records)
    
    assert total == 2
    call_args = mock_asyncpg_conn.copy_records_to_table.call_args
    assert call_args[0][0] == 'stations'
    assert call_args[1]['schema_name'] == 'airquality'
    assert call_args[1]['records'] == records
    assert call_args[1]['columns'] == [
        'station_code', 'station_name', 'country_code',
        'latitude', 'longitude', 'altitude'
    ]


@pytest.mark.asyncio
async def test_bulk_copy_measurements_empty_list():
    """Test bulk_copy_measurements with empty list."""
    mock_session = AsyncMock()
    mock_connection = AsyncMock()
    mock_raw_connection = MagicMock()
    mock_asyncpg_conn = AsyncMock()
    
    mock_session.connection.return_value = mock_connection
    mock_connection.get_raw_connection.return_value = mock_raw_connection
    mock_raw_connection.driver_connection = mock_asyncpg_conn
    
    total = await bulk_copy_measurements(mock_session, [], batch_size=100)
    
    assert total == 0
    mock_asyncpg_conn.copy_records_to_table.assert_not_called()


@pytest.mark.asyncio
async def test_bulk_copy_measurements_large_batch():
    """Test bulk_copy_measurements with large dataset."""
    mock_session = AsyncMock()
    mock_connection = AsyncMock()
    mock_raw_connection = MagicMock()
    mock_asyncpg_conn = AsyncMock()
    
    mock_session.connection.return_value = mock_connection
    mock_connection.get_raw_connection.return_value = mock_raw_connection
    mock_raw_connection.driver_connection = mock_asyncpg_conn
    
    # Create 250k records
    def large_generator():
        for i in range(250_000):
            yield (
                datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                f'SP_{i % 100:03d}',
                5,
                40.0,
                None, None,
                1, 1,
                None, None,
                f'OBS_{i}'
            )
    
    total = await bulk_copy_measurements(
        mock_session,
        large_generator(),
        batch_size=100_000
    )
    
    assert total == 250_000
    # Should be called 3 times: 100k + 100k + 50k
    assert mock_asyncpg_conn.copy_records_to_table.call_count == 3
