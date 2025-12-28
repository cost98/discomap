"""Bulk import utilities using PostgreSQL COPY for maximum performance.

COPY is 3-10x faster than bulk INSERT for large datasets.
Use these utilities for high-performance data loading.
"""

from typing import AsyncGenerator, List, Tuple, Any
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime


async def bulk_copy_measurements(
    session: AsyncSession,
    records: AsyncGenerator[Tuple, None] | List[Tuple],
    batch_size: int = 100_000,
) -> int:
    """Bulk insert measurements using PostgreSQL COPY.
    
    Args:
        session: AsyncSession connected to database
        records: Generator or list of tuples with measurement data
                 Format: (time, sampling_point_id, pollutant_code, value, 
                         unit, aggregation_type, validity, verification,
                         data_capture, result_time, observation_id)
        batch_size: Records per COPY batch (default 100k)
        
    Returns:
        Total number of records inserted
        
    Example:
        ```python
        def generate_records():
            for i in range(1000000):
                yield (
                    datetime.now(),
                    'SP_001',
                    5,  # pollutant_code
                    42.5,  # value
                    None, None,  # unit, aggregation_type
                    1, 1,  # validity, verification
                    None, None,  # data_capture, result_time
                    f'OBS_{i}'
                )
        
        total = await bulk_copy_measurements(session, generate_records())
        print(f"Inserted {total:,} records")
        ```
    """
    # Get raw asyncpg connection
    connection = await session.connection()
    raw_connection = await connection.get_raw_connection()
    asyncpg_conn = raw_connection.driver_connection
    
    total_inserted = 0
    batch = []
    
    # Column order must match table definition
    columns = [
        'time', 'sampling_point_id', 'pollutant_code', 'value',
        'unit', 'aggregation_type', 'validity', 'verification',
        'data_capture', 'result_time', 'observation_id'
    ]
    
    # Process records in batches
    if hasattr(records, '__iter__') and not isinstance(records, list):
        # Generator - process in batches
        for record in records:
            batch.append(record)
            
            if len(batch) >= batch_size:
                await asyncpg_conn.copy_records_to_table(
                    'measurements',
                    schema_name='airquality',
                    records=batch,
                    columns=columns,
                )
                total_inserted += len(batch)
                batch = []
        
        # Insert remaining records
        if batch:
            await asyncpg_conn.copy_records_to_table(
                'measurements',
                schema_name='airquality',
                records=batch,
                columns=columns,
            )
            total_inserted += len(batch)
    else:
        # List - insert all at once or in batches if large
        records_list = list(records) if not isinstance(records, list) else records
        
        for i in range(0, len(records_list), batch_size):
            batch = records_list[i:i + batch_size]
            await asyncpg_conn.copy_records_to_table(
                'measurements',
                schema_name='airquality',
                records=batch,
                columns=columns,
            )
            total_inserted += len(batch)
    
    return total_inserted


async def bulk_copy_sampling_points(
    session: AsyncSession,
    records: List[Tuple[str, str, str, int]],
) -> int:
    """Bulk insert sampling points using PostgreSQL COPY.
    
    Args:
        session: AsyncSession connected to database
        records: List of tuples (sampling_point_id, station_code, 
                                country_code, pollutant_code)
        
    Returns:
        Number of records inserted
    """
    connection = await session.connection()
    raw_connection = await connection.get_raw_connection()
    asyncpg_conn = raw_connection.driver_connection
    
    await asyncpg_conn.copy_records_to_table(
        'sampling_points',
        schema_name='airquality',
        records=records,
        columns=['sampling_point_id', 'station_code', 'country_code', 'pollutant_code'],
    )
    
    return len(records)


async def bulk_copy_stations(
    session: AsyncSession,
    records: List[Tuple[str, str, str, float, float, float]],
) -> int:
    """Bulk insert stations using PostgreSQL COPY.
    
    Args:
        session: AsyncSession connected to database
        records: List of tuples (station_code, station_name, country_code,
                                latitude, longitude, altitude)
        
    Returns:
        Number of records inserted
    """
    connection = await session.connection()
    raw_connection = await connection.get_raw_connection()
    asyncpg_conn = raw_connection.driver_connection
    
    await asyncpg_conn.copy_records_to_table(
        'stations',
        schema_name='airquality',
        records=records,
        columns=['station_code', 'station_name', 'country_code', 
                'latitude', 'longitude', 'altitude'],
    )
    
    return len(records)
