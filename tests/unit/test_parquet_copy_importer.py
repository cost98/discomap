"""Unit tests for ParquetCopyImporter."""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, Mock
import pandas as pd

from src.services.parquet_copy_importer import (
    ParquetCopyImporter,
    import_eea_data_with_copy,
)


@pytest.fixture
def mock_session():
    """Create mock AsyncSession."""
    session = AsyncMock()
    mock_connection = AsyncMock()
    mock_raw_connection = MagicMock()
    mock_asyncpg_conn = AsyncMock()
    
    session.connection.return_value = mock_connection
    mock_connection.get_raw_connection.return_value = mock_raw_connection
    mock_raw_connection.driver_connection = mock_asyncpg_conn
    
    return session


@pytest.fixture
def sample_parquet_data():
    """Create sample Parquet data as DataFrame."""
    return pd.DataFrame({
        'DatetimeBegin': [
            datetime(2023, 1, 1, 0, 0, 0),
            datetime(2023, 1, 1, 1, 0, 0),
            datetime(2023, 1, 1, 2, 0, 0),
        ],
        'SamplingPoint': ['SP_IT_001', 'SP_IT_001', 'SP_IT_002'],
        'Pollutant': [5, 5, 8],
        'Concentration': [42.5, 43.2, 15.3],
        'UnitOfMeasurement': ['µg/m³', 'µg/m³', 'µg/m³'],
        'AggregationType': ['hour', 'hour', 'hour'],
        'Validity': [1, 1, 2],
        'Verification': [1, 1, 1],
        'DataCapture': [95.0, 95.0, 98.0],
        'ResultTime': [
            datetime(2023, 1, 1, 1, 0, 0),
            datetime(2023, 1, 1, 2, 0, 0),
            datetime(2023, 1, 1, 3, 0, 0),
        ],
    })


@pytest.mark.asyncio
async def test_parquet_copy_importer_init(mock_session):
    """Test ParquetCopyImporter initialization."""
    importer = ParquetCopyImporter(mock_session)
    assert importer.session == mock_session


@pytest.mark.asyncio
async def test_import_parquet_file_success(mock_session, sample_parquet_data):
    """Test successful Parquet file import."""
    importer = ParquetCopyImporter(mock_session)
    
    # Mock pyarrow.parquet.read_table
    with patch('pyarrow.parquet.read_table') as mock_read:
        mock_table = Mock()
        mock_table.to_pandas.return_value = sample_parquet_data
        mock_read.return_value = mock_table
        
        # Mock bulk_copy_measurements
        with patch('src.services.parquet_copy_importer.bulk_copy_measurements') as mock_copy:
            mock_copy.return_value = len(sample_parquet_data)
            
            total = await importer.import_parquet_file('test.parquet')
            
            assert total == 3
            mock_read.assert_called_once_with('test.parquet')
            mock_copy.assert_called_once()


@pytest.mark.asyncio
async def test_import_parquet_file_missing_columns(mock_session):
    """Test import fails with missing required columns."""
    importer = ParquetCopyImporter(mock_session)
    
    # DataFrame without required columns
    bad_df = pd.DataFrame({
        'DatetimeBegin': [datetime(2023, 1, 1, 0, 0, 0)],
        'SamplingPoint': ['SP_001'],
        # Missing Pollutant and Concentration
    })
    
    with patch('pyarrow.parquet.read_table') as mock_read:
        mock_table = Mock()
        mock_table.to_pandas.return_value = bad_df
        mock_read.return_value = mock_table
        
        with pytest.raises(ValueError, match="Missing required columns"):
            await importer.import_parquet_file('test.parquet')


@pytest.mark.asyncio
async def test_dataframe_to_tuples(mock_session, sample_parquet_data):
    """Test conversion from DataFrame to tuples."""
    importer = ParquetCopyImporter(mock_session)
    
    tuples = list(importer._dataframe_to_tuples(sample_parquet_data))
    
    assert len(tuples) == 3
    
    # Check first tuple structure
    first = tuples[0]
    assert len(first) == 11  # All columns
    assert first[0] == datetime(2023, 1, 1, 0, 0, 0)  # time
    assert first[1] == 'SP_IT_001'  # sampling_point_id
    assert first[2] == 5  # pollutant_code
    assert first[3] == 42.5  # value
    assert first[4] == 'µg/m³'  # unit
    assert first[5] == 'hour'  # aggregation_type
    assert first[6] == 1  # validity
    assert first[7] == 1  # verification
    assert 'OBS_SP_IT_001' in first[10]  # observation_id


@pytest.mark.asyncio
async def test_import_parquet_files_batch(mock_session, sample_parquet_data):
    """Test batch import of multiple files."""
    importer = ParquetCopyImporter(mock_session)
    
    files = ['file1.parquet', 'file2.parquet', 'file3.parquet']
    
    with patch('pyarrow.parquet.read_table') as mock_read:
        mock_table = Mock()
        mock_table.to_pandas.return_value = sample_parquet_data
        mock_read.return_value = mock_table
        
        with patch('src.services.parquet_copy_importer.bulk_copy_measurements') as mock_copy:
            mock_copy.return_value = len(sample_parquet_data)
            
            total = await importer.import_parquet_files_batch(files, max_workers=2)
            
            assert total == 9  # 3 files × 3 records
            assert mock_read.call_count == 3
            assert mock_copy.call_count == 3


@pytest.mark.asyncio
async def test_import_parquet_files_batch_with_errors(mock_session, sample_parquet_data):
    """Test batch import handles errors gracefully."""
    importer = ParquetCopyImporter(mock_session)
    
    files = ['file1.parquet', 'file2.parquet', 'file3.parquet']
    
    with patch('pyarrow.parquet.read_table') as mock_read:
        # First file succeeds, second fails, third succeeds
        def side_effect(path):
            if path == 'file2.parquet':
                raise Exception("File read error")
            mock_table = Mock()
            mock_table.to_pandas.return_value = sample_parquet_data
            return mock_table
        
        mock_read.side_effect = side_effect
        
        with patch('src.services.parquet_copy_importer.bulk_copy_measurements') as mock_copy:
            mock_copy.return_value = len(sample_parquet_data)
            
            total = await importer.import_parquet_files_batch(files, max_workers=1)
            
            # Should process 2 successful files
            assert total == 6  # 2 files × 3 records


@pytest.mark.asyncio
async def test_import_eea_data_with_copy(mock_session, sample_parquet_data):
    """Test high-level import function."""
    urls = ['url1.parquet', 'url2.parquet']
    
    with patch('pyarrow.parquet.read_table') as mock_read:
        mock_table = Mock()
        mock_table.to_pandas.return_value = sample_parquet_data
        mock_read.return_value = mock_table
        
        with patch('src.services.parquet_copy_importer.bulk_copy_measurements') as mock_copy:
            mock_copy.return_value = len(sample_parquet_data)
            
            result = await import_eea_data_with_copy(
                session=mock_session,
                parquet_urls=urls,
                max_workers=2
            )
            
            assert result['total_records'] == 6
            assert result['files_processed'] == 2
            assert result['elapsed'] > 0
            assert result['rate'] > 0


@pytest.mark.asyncio
async def test_dataframe_to_tuples_with_defaults(mock_session):
    """Test DataFrame conversion uses defaults for missing optional columns."""
    importer = ParquetCopyImporter(mock_session)
    
    # Minimal DataFrame with only required columns
    minimal_df = pd.DataFrame({
        'DatetimeBegin': [datetime(2023, 1, 1, 0, 0, 0)],
        'SamplingPoint': ['SP_001'],
        'Pollutant': [5],
        'Concentration': [42.5],
    })
    
    tuples = list(importer._dataframe_to_tuples(minimal_df))
    
    assert len(tuples) == 1
    first = tuples[0]
    
    # Check defaults are applied
    assert first[4] is None  # unit (None if not in DataFrame)
    assert first[5] is None  # aggregation_type
    assert first[6] == 1  # validity (default)
    assert first[7] == 2  # verification (default to preliminary)
    assert first[8] is None  # data_capture
    assert first[9] is None  # result_time


@pytest.mark.asyncio
async def test_import_empty_parquet_file(mock_session):
    """Test importing empty Parquet file."""
    importer = ParquetCopyImporter(mock_session)
    
    empty_df = pd.DataFrame({
        'DatetimeBegin': [],
        'SamplingPoint': [],
        'Pollutant': [],
        'Concentration': [],
    })
    
    with patch('pyarrow.parquet.read_table') as mock_read:
        mock_table = Mock()
        mock_table.to_pandas.return_value = empty_df
        mock_read.return_value = mock_table
        
        with patch('src.services.parquet_copy_importer.bulk_copy_measurements') as mock_copy:
            mock_copy.return_value = 0
            
            total = await importer.import_parquet_file('empty.parquet')
            
            assert total == 0
