"""ETL Pipeline for EEA Air Quality Data.

Orchestrates the complete ETL process: Download → Parse → Load into database.
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.database.engine import get_db_session
from src.database.repositories import (
    MeasurementRepository,
    SamplingPointRepository,
    StationRepository,
)
from src.logger import get_logger
from src.services.parquet_downloader import ParquetDownloader
from src.services.parquet_parser import ParquetParser

logger = get_logger(__name__)


class ETLPipeline:
    """
    Complete ETL pipeline for EEA air quality data.
    
    Workflow:
        1. Download Parquet files from URLs
        2. Parse Parquet files
        3. Load data into database (stations → sampling points → measurements)
    """

    def __init__(
        self,
        output_dir: str = "data/raw/parquet",
        batch_size: int = 1000,
    ):
        """
        Initialize ETL pipeline.
        
        Args:
            output_dir: Directory for downloaded Parquet files
            batch_size: Batch size for measurement inserts
        """
        self.downloader = ParquetDownloader(output_dir=output_dir)
        self.parser = ParquetParser()
        self.batch_size = batch_size
        
        logger.info(f"ETL Pipeline initialized - batch_size={batch_size}")

    async def run_from_url(
        self,
        url: str,
        skip_download: bool = False,
    ) -> Dict[str, int]:
        """
        Run ETL from a single Parquet URL.
        
        Args:
            url: Parquet file URL
            skip_download: If True, use existing local file
            
        Returns:
            Statistics dictionary with counts
            
        Example:
            >>> pipeline = ETLPipeline()
            >>> stats = await pipeline.run_from_url(
            ...     "https://eeadmz1batchservice02.blob.core.windows.net/..."
            ... )
            >>> print(f"Inserted {stats['measurements']} measurements")
        """
        import time
        start_time = time.time()
        
        logger.info(f"🚀 Starting ETL for URL: {url}")
        
        # 1. Download (se necessario)
        download_start = time.time()
        if skip_download:
            filename = url.split("/")[-1]
            filepath = Path(self.downloader.output_dir) / filename
            logger.info(f"⏩ Skipping download, using: {filepath}")
        else:
            filepath = self.downloader.download(url)
            download_time = time.time() - download_start
            logger.info(f"📥 Download completed in {download_time:.2f}s")
        
        # 2. Parse
        parse_start = time.time()
        data = self.parser.parse_all(filepath)
        parse_time = time.time() - parse_start
        logger.info(f"📊 Parsing completed in {parse_time:.2f}s - {len(data['measurements'])} measurements")
        
        # 3. Load
        load_start = time.time()
        stats = await self._load_to_database(data)
        load_time = time.time() - load_start
        
        total_time = time.time() - start_time
        throughput = stats['measurements'] / total_time if total_time > 0 else 0
        
        logger.info(f"💾 Database load completed in {load_time:.2f}s")
        logger.info(f"✅ ETL complete - Total: {total_time:.2f}s | Throughput: {throughput:.0f} meas/sec")
        logger.info(f"📈 Stats - Stations: {stats['stations']}, Sampling Points: {stats['sampling_points']}, Measurements: {stats['measurements']}")
        
        return stats

    async def run_from_file(self, filepath: Path) -> Dict[str, int]:
        """
        Run ETL from existing Parquet file.
        
        Args:
            filepath: Path to Parquet file
            
        Returns:
            Statistics dictionary
        """
        logger.info(f"Starting ETL for file: {filepath}")
        
        # Parse
        data = self.parser.parse_all(filepath)
        
        # Load
        stats = await self._load_to_database(data)
        
        logger.info(f"ETL complete - {stats}")
        return stats

    async def run_batch_from_urls(
        self,
        urls: List[str],
        max_files: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Run ETL for multiple URLs in batch.
        
        Args:
            urls: List of Parquet URLs
            max_files: Max files to process (None = all)
            
        Returns:
            Aggregated statistics
        """
        logger.info(f"Starting batch ETL for {len(urls)} URLs")
        
        if max_files:
            urls = urls[:max_files]
        
        total_stats = {
            "files_processed": 0,
            "stations": 0,
            "sampling_points": 0,
            "measurements": 0,
            "errors": 0,
        }
        
        for i, url in enumerate(urls, 1):
            try:
                logger.info(f"Processing file {i}/{len(urls)}: {url}")
                stats = await self.run_from_url(url)
                
                total_stats["files_processed"] += 1
                total_stats["stations"] += stats["stations"]
                total_stats["sampling_points"] += stats["sampling_points"]
                total_stats["measurements"] += stats["measurements"]
                
            except Exception as e:
                logger.error(f"Error processing {url}: {e}", exc_info=True)
                total_stats["errors"] += 1
        
        logger.info(f"Batch ETL complete - {total_stats}")
        return total_stats

    async def _load_to_database(self, data: Dict[str, List[Dict]]) -> Dict[str, int]:
        """
        Load parsed data into database.
        
        Order: stations → sampling_points → measurements (rispetta FK constraints)
        
        Args:
            data: Parsed data from ParquetParser
            
        Returns:
            Statistics dictionary
        """
        logger.info("💾 Starting database load...")
        
        stats = {
            "stations": 0,
            "sampling_points": 0,
            "measurements": 0,
        }
        
        async with get_db_session() as session:
            # 1. Stations
            station_repo = StationRepository(session)
            for station_data in data["stations"]:
                try:
                    await station_repo.create_or_update(station_data)
                    stats["stations"] += 1
                except Exception as e:
                    logger.error(f"Station insert error: {e}", exc_info=True)
            
            logger.info(f"🏢 Loaded {stats['stations']} stations")
            
            # 2. Sampling Points
            sp_repo = SamplingPointRepository(session)
            for sp_data in data["sampling_points"]:
                try:
                    await sp_repo.create_or_update(sp_data)
                    stats["sampling_points"] += 1
                except Exception as e:
                    logger.error(f"Sampling point insert error: {e}", exc_info=True)
            
            logger.info(f"📍 Loaded {stats['sampling_points']} sampling points")
            
            # 3. Measurements (bulk insert in batches)
            meas_repo = MeasurementRepository(session)
            measurements = data["measurements"]
            total_batches = (len(measurements) + self.batch_size - 1) // self.batch_size
            
            logger.info(f"📦 Inserting {len(measurements)} measurements in {total_batches} batches...")
            
            for i in range(0, len(measurements), self.batch_size):
                batch = measurements[i : i + self.batch_size]
                batch_num = i // self.batch_size + 1
                try:
                    count = await meas_repo.bulk_insert(batch)
                    stats["measurements"] += count
                    progress = (stats["measurements"] / len(measurements)) * 100
                    logger.debug(f"  Batch {batch_num}/{total_batches}: +{count} measurements ({progress:.1f}%)")
                except Exception as e:
                    logger.error(f"Measurement batch insert error: {e}", exc_info=True)
            
            logger.info(f"✅ Loaded {stats['measurements']:,} measurements")
            
            # Commit transaction
            await session.commit()
        
        return stats


# Helper per esecuzione sincrona
def run_etl_sync(url: str, **kwargs) -> Dict[str, int]:
    """
    Run ETL synchronously (per script/testing).
    
    Args:
        url: Parquet URL
        **kwargs: Additional args for ETLPipeline
        
    Returns:
        Statistics
        
    Example:
        >>> stats = run_etl_sync("https://...")
        >>> print(f"Done: {stats}")
    """
    pipeline = ETLPipeline(**kwargs)
    return asyncio.run(pipeline.run_from_url(url))
