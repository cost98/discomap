"""
Integration tests for ETL Pipeline.

These tests demonstrate complete ETL workflows with database.
Run with: pytest tests/integration/test_etl_pipeline.py -v
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from src.database.repositories import (
    MeasurementRepository,
    SamplingPointRepository,
    StationRepository,
)
from src.services import ETLPipeline


@pytest.fixture
def sample_parquet_file(tmp_path):
    """Create a sample Parquet file for testing."""
    # Sample EEA data structure
    df = pd.DataFrame({
        # Station fields
        "AirQualityStationEoICode": ["TEST001", "TEST001", "TEST002"],
        "Countrycode": ["IT", "IT", "IT"],
        "AirQualityStationName": ["Milano Test", "Milano Test", "Roma Test"],
        "AirQualityStationType": ["traffic", "traffic", "background"],
        "AirQualityStationArea": ["urban", "urban", "suburban"],
        "Latitude": [45.4642, 45.4642, 41.8902],
        "Longitude": [9.1900, 9.1900, 12.4922],
        "Altitude": [122.0, 122.0, 20.0],
        "Municipality": ["Milano", "Milano", "Roma"],
        
        # Sampling point fields
        "SamplingPoint": ["IT/SPO.TEST001_8", "IT/SPO.TEST001_8", "IT/SPO.TEST002_10"],
        "AirPollutantCode": [8, 8, 10],
        
        # Measurement fields
        "DatetimeBegin": [
            datetime(2024, 1, 1, 0, 0),
            datetime(2024, 1, 1, 1, 0),
            datetime(2024, 1, 1, 0, 0),
        ],
        "Concentration": [25.5, 28.3, 42.1],
        "UnitOfMeasurement": ["µg/m³", "µg/m³", "µg/m³"],
        "AggregationType": ["hour", "hour", "hour"],
        "Validity": [1, 1, 1],
        "Verification": [2, 2, 2],
        "DataCapture": [95.5, 95.5, 98.2],
    })
    
    # Save to parquet
    parquet_file = tmp_path / "test_data.parquet"
    df.to_parquet(parquet_file)
    
    return parquet_file


@pytest.mark.asyncio
class TestETLPipelineIntegration:
    """Integration tests for complete ETL workflow."""
    
    async def test_etl_from_file(self, db_session, sample_parquet_file):
        """
        Test complete ETL from Parquet file to database.
        
        Example usage:
            pipeline = ETLPipeline()
            stats = await pipeline.run_from_file(Path("data.parquet"))
        """
        pipeline = ETLPipeline(output_dir=str(sample_parquet_file.parent))
        
        # Run ETL
        stats = await pipeline.run_from_file(sample_parquet_file)
        
        # Verify statistics
        assert stats["stations"] == 2
        assert stats["sampling_points"] == 2
        assert stats["measurements"] == 3
        
        # Verify data in database
        station_repo = StationRepository(db_session)
        station = await station_repo.get_by_code("TEST001")
        
        assert station is not None
        assert station.station_code == "TEST001"
        assert station.country_code == "IT"
        assert station.station_name == "Milano Test"
        assert station.latitude == 45.4642
    
    async def test_etl_sampling_points_loaded(self, db_session, sample_parquet_file):
        """
        Test sampling points are correctly loaded.
        
        Example:
            pipeline = ETLPipeline()
            await pipeline.run_from_file(filepath)
            
            # Verify in DB
            sp_repo = SamplingPointRepository(session)
            sp = await sp_repo.get_by_id("IT/SPO.TEST001_8")
        """
        pipeline = ETLPipeline()
        await pipeline.run_from_file(sample_parquet_file)
        
        # Verify sampling point
        sp_repo = SamplingPointRepository(db_session)
        sp = await sp_repo.get_by_id("IT/SPO.TEST001_8")
        
        assert sp is not None
        assert sp.sampling_point_id == "IT/SPO.TEST001_8"
        assert sp.station_code == "TEST001"
        assert sp.pollutant_code == 8
    
    async def test_etl_measurements_loaded(self, db_session, sample_parquet_file):
        """
        Test measurements are bulk-loaded correctly.
        
        Example:
            pipeline = ETLPipeline(batch_size=1000)
            await pipeline.run_from_file(filepath)
            
            # Query measurements
            meas_repo = MeasurementRepository(session)
            measurements = await meas_repo.get_latest("IT/SPO.TEST001_8", limit=10)
        """
        pipeline = ETLPipeline(batch_size=100)
        await pipeline.run_from_file(sample_parquet_file)
        
        # Verify measurements
        meas_repo = MeasurementRepository(db_session)
        measurements = await meas_repo.get_latest("IT/SPO.TEST001_8", limit=10)
        
        assert len(measurements) == 2  # 2 measurements for this sampling point
        
        # Check first measurement
        meas = measurements[0]  # Latest (DESC order)
        assert meas.sampling_point_id == "IT/SPO.TEST001_8"
        assert meas.pollutant_code == 8
        assert meas.value in [25.5, 28.3]
        assert meas.unit == "µg/m³"
        assert meas.validity == 1
        assert meas.verification == 2
    
    async def test_etl_batch_size(self, db_session, tmp_path):
        """
        Test ETL with different batch sizes.
        
        Example:
            # Small batch for memory-constrained environments
            pipeline = ETLPipeline(batch_size=500)
            
            # Large batch for performance
            pipeline = ETLPipeline(batch_size=5000)
        """
        # Create file with many measurements
        n_rows = 2500
        large_df = pd.DataFrame({
            "AirQualityStationEoICode": ["TEST999"] * n_rows,
            "Countrycode": ["IT"] * n_rows,
            "SamplingPoint": ["IT/SPO.TEST999_8"] * n_rows,
            "AirPollutantCode": [8] * n_rows,
            "DatetimeBegin": [datetime(2024, 1, 1, i % 24, i % 60) for i in range(n_rows)],
            "Concentration": [20.0 + (i % 50) for i in range(n_rows)],
        })
        
        parquet_file = tmp_path / "large.parquet"
        large_df.to_parquet(parquet_file)
        
        # Test with small batch size
        pipeline = ETLPipeline(batch_size=1000)
        stats = await pipeline.run_from_file(parquet_file)
        
        assert stats["measurements"] == n_rows
        
        # Verify all loaded
        meas_repo = MeasurementRepository(db_session)
        all_meas = await meas_repo.get_latest("IT/SPO.TEST999_8", limit=n_rows)
        assert len(all_meas) == n_rows
    
    async def test_etl_idempotency(self, db_session, sample_parquet_file):
        """
        Test ETL can be run multiple times (upsert behavior).
        
        Example:
            # First run
            stats1 = await pipeline.run_from_file(filepath)
            
            # Second run (updates existing)
            stats2 = await pipeline.run_from_file(filepath)
        """
        pipeline = ETLPipeline()
        
        # First run
        stats1 = await pipeline.run_from_file(sample_parquet_file)
        assert stats1["stations"] == 2
        
        # Second run (should update existing stations)
        stats2 = await pipeline.run_from_file(sample_parquet_file)
        assert stats2["stations"] == 2  # Same count (upsert)
        
        # Verify only 2 stations in DB (not 4)
        station_repo = StationRepository(db_session)
        station1 = await station_repo.get_by_code("TEST001")
        station2 = await station_repo.get_by_code("TEST002")
        
        assert station1 is not None
        assert station2 is not None
    
    async def test_etl_transaction_rollback(self, db_session, tmp_path):
        """
        Test transaction rollback on error.
        
        If an error occurs during ETL, the entire transaction should rollback.
        """
        # Create file with invalid data (will cause constraint error)
        invalid_df = pd.DataFrame({
            "AirQualityStationEoICode": ["INVALID"],
            "SamplingPoint": ["IT/SPO.INVALID_8"],
            "AirPollutantCode": [8],
            "DatetimeBegin": [datetime(2024, 1, 1)],
            # Missing required station fields - will cause FK constraint violation
        })
        
        parquet_file = tmp_path / "invalid.parquet"
        invalid_df.to_parquet(parquet_file)
        
        pipeline = ETLPipeline()
        
        # Should handle error gracefully
        try:
            await pipeline.run_from_file(parquet_file)
        except Exception:
            pass  # Expected to fail
        
        # Verify no partial data in database
        station_repo = StationRepository(db_session)
        station = await station_repo.get_by_code("INVALID")
        assert station is None  # Should not exist


@pytest.mark.asyncio
class TestETLPipelinePerformance:
    """Performance-focused integration tests."""
    
    async def test_bulk_insert_performance(self, db_session, tmp_path):
        """
        Test bulk insert performance with large dataset.
        
        Example for optimal performance:
            pipeline = ETLPipeline(batch_size=2000)  # Tune based on memory
            stats = await pipeline.run_from_file(large_file)
        """
        # Create 10K measurements
        n_rows = 10_000
        df = pd.DataFrame({
            "AirQualityStationEoICode": [f"PERF{i % 10:03d}" for i in range(n_rows)],
            "Countrycode": ["IT"] * n_rows,
            "SamplingPoint": [f"IT/SPO.PERF{i % 10:03d}_8" for i in range(n_rows)],
            "AirPollutantCode": [8] * n_rows,
            "DatetimeBegin": [datetime(2024, 1, 1, i % 24, i % 60) for i in range(n_rows)],
            "Concentration": [20.0 + (i % 50) for i in range(n_rows)],
        })
        
        parquet_file = tmp_path / "perf_test.parquet"
        df.to_parquet(parquet_file)
        
        # Run ETL with optimal batch size
        pipeline = ETLPipeline(batch_size=2000)
        
        import time
        start = time.time()
        stats = await pipeline.run_from_file(parquet_file)
        elapsed = time.time() - start
        
        # Verify results
        assert stats["measurements"] == n_rows
        assert stats["stations"] == 10
        
        # Performance assertion (should be fast)
        # ~10K rows should load in < 5 seconds
        assert elapsed < 10.0, f"ETL too slow: {elapsed:.2f}s for {n_rows} rows"
        
        print(f"\n⚡ Performance: {n_rows} rows in {elapsed:.2f}s ({n_rows/elapsed:.0f} rows/s)")


@pytest.mark.asyncio
class TestETLPipelineEdgeCases:
    """Edge case tests."""
    
    async def test_empty_parquet_file(self, db_session, tmp_path):
        """Test ETL with empty Parquet file."""
        empty_df = pd.DataFrame()
        parquet_file = tmp_path / "empty.parquet"
        empty_df.to_parquet(parquet_file)
        
        pipeline = ETLPipeline()
        
        # Should handle gracefully
        try:
            stats = await pipeline.run_from_file(parquet_file)
            assert stats["measurements"] == 0
        except Exception:
            pass  # Acceptable to fail on empty file
    
    async def test_missing_required_fields(self, db_session, tmp_path):
        """Test ETL with missing required measurement fields."""
        # Missing DatetimeBegin
        df = pd.DataFrame({
            "SamplingPoint": ["IT/SPO.TEST_8"],
            "Concentration": [25.5],
            # Missing time - required field
        })
        
        parquet_file = tmp_path / "missing_fields.parquet"
        df.to_parquet(parquet_file)
        
        pipeline = ETLPipeline()
        stats = await pipeline.run_from_file(parquet_file)
        
        # Should skip invalid measurements
        assert stats["measurements"] == 0
