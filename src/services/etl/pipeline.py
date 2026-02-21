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
from src.services.downloaders import URLDownloader
from src.services.parsers import ParquetParser

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
        batch_size: int = 50000,
        cleanup_after_processing: bool = True,
        max_concurrent_files: int = 3,
        upsert_mode: bool = False,
    ):
        """
        Initialize ETL pipeline.
        
        Args:
            output_dir: Directory for downloaded Parquet files
            batch_size: Batch size for measurement inserts (default 50000 - COPY scala bene)
            cleanup_after_processing: Delete files after successful processing
            max_concurrent_files: Max files to process in parallel (default 3)
            upsert_mode: Use bulk_upsert instead of bulk_copy (default False = veloce, True = gestisce duplicati)
        """
        self.downloader = URLDownloader(output_dir=output_dir)
        self.parser = ParquetParser()
        self.batch_size = batch_size
        self.cleanup_after_processing = cleanup_after_processing
        self.max_concurrent_files = max_concurrent_files
        self.upsert_mode = upsert_mode
        
        logger.info(f"ETL Pipeline initialized - batch_size={batch_size}, cleanup={cleanup_after_processing}, upsert={upsert_mode}")

    async def process_parquet_file(
        self,
        filepath: Path | str,
        cleanup: bool | None = None,
    ) -> Dict[str, int]:
        """
        Process a Parquet file: Parse + Load to database.
        
        Core ETL logic separated from download mechanism.
        
        Args:
            filepath: Path to local Parquet file
            cleanup: Delete file after processing (None = use instance default)
            
        Returns:
            Statistics dictionary with counts
            
        Example:
            >>> pipeline = ETLPipeline()
            >>> stats = await pipeline.process_parquet_file("data/file.parquet")
            >>> print(f"Inserted {stats['measurements']} measurements")
        """
        import time
        
        filepath = Path(filepath)
        logger.info(f"📄 Processing parquet file: {filepath.name}")
        
        start_time = time.time()
        
        # 1. Parse
        parse_start = time.time()
        data = self.parser.parse_all(filepath)
        parse_time = time.time() - parse_start
        logger.info(f"📊 Parsing completed in {parse_time:.2f}s - {len(data['measurements'])} measurements")
        
        # 2. Load
        load_start = time.time()
        stats = await self._load_to_database(data)
        load_time = time.time() - load_start
        
        total_time = time.time() - start_time
        throughput = stats['measurements'] / total_time if total_time > 0 else 0
        
        logger.info(f"💾 Database load completed in {load_time:.2f}s")
        logger.info(f"✅ ETL complete - Total: {total_time:.2f}s | Throughput: {throughput:.0f} meas/sec")
        logger.info(f"📈 Stats - Stations: {stats['stations']}, Sampling Points: {stats['sampling_points']}, Measurements: {stats['measurements']}")
        
        # Cleanup file if requested
        should_cleanup = cleanup if cleanup is not None else self.cleanup_after_processing
        if should_cleanup:
            try:
                filepath.unlink()
                logger.info(f"🗑️  Deleted processed file: {filepath.name}")
            except Exception as e:
                logger.warning(f"⚠️  Could not delete file {filepath.name}: {e}")
        
        return stats

    async def run_from_url(
        self,
        url: str,
        skip_download: bool = False,
    ) -> Dict[str, int]:
        """
        Download from URL and process Parquet file.
        
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
        
        logger.info(f"🚀 Starting ETL for URL: {url}")
        
        # 1. Download
        download_start = time.time()
        if skip_download:
            filename = url.split("/")[-1]
            filepath = Path(self.downloader.output_dir) / filename
            logger.info(f"⏩ Skipping download, using: {filepath}")
        else:
            filepath = self.downloader.download(url)
            download_time = time.time() - download_start
            logger.info(f"📥 Download completed in {download_time:.2f}s")
        
        # 2. Process file
        stats = await self.process_parquet_file(filepath)
        
        return stats



    async def run_batch_from_urls(
        self,
        urls: List[str],
        max_files: Optional[int] = None,
    ) -> Dict[str, int]:
        """
        Run ETL for multiple URLs in batch with parallel processing.
        
        Args:
            urls: List of Parquet URLs
            max_files: Max files to process (None = all)
            
        Returns:
            Aggregated statistics
        """
        logger.info(f"🚀 Starting parallel batch ETL for {len(urls)} URLs (max {self.max_concurrent_files} concurrent)")
        
        if max_files:
            urls = urls[:max_files]
        
        total_stats = {
            "files_processed": 0,
            "stations": 0,
            "sampling_points": 0,
            "measurements": 0,
            "errors": 0,
        }
        
        # Semaphore per limitare file concorrenti
        semaphore = asyncio.Semaphore(self.max_concurrent_files)
        
        async def process_with_semaphore(url: str, index: int):
            """Process single URL with semaphore control."""
            async with semaphore:
                try:
                    logger.info(f"⚡ [{index}/{len(urls)}] Processing: {url.split('/')[-1]}")
                    stats = await self.run_from_url(url)
                    return {"success": True, "stats": stats}
                except Exception as e:
                    logger.error(f"❌ [{index}/{len(urls)}] Error: {e}", exc_info=True)
                    return {"success": False, "error": str(e)}
        
        # Process all URLs in parallel (limited by semaphore)
        tasks = [process_with_semaphore(url, i+1) for i, url in enumerate(urls)]
        results = await asyncio.gather(*tasks)
        
        # Aggregate results
        for result in results:
            if result["success"]:
                total_stats["files_processed"] += 1
                total_stats["stations"] += result["stats"]["stations"]
                total_stats["sampling_points"] += result["stats"]["sampling_points"]
                total_stats["measurements"] += result["stats"]["measurements"]
            else:
                total_stats["errors"] += 1
        
        logger.info(f"✅ Parallel batch ETL complete - {total_stats}")
        return total_stats

    async def _load_to_database(self, data: Dict[str, List[Dict]]) -> Dict[str, int]:
        """
        Load parsed data into database.
        
        PREREQUISITO: Stations e Sampling Points devono essere già caricati via CSV!
        Questa pipeline inserisce SOLO measurements per sampling_points esistenti.
        
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
            # 1. Stations - SKIPPED (caricati da CSV DataExtract)
            # 2. Sampling Points - SKIPPED (caricati da CSV DataExtract)
            # Solo measurements vengono inseriti dalla pipeline ETL
            
            # 3. Measurements (bulk insert in batches)
            meas_repo = MeasurementRepository(session)
            measurements = data["measurements"]
            total_batches = (len(measurements) + self.batch_size - 1) // self.batch_size
            
            logger.info(f"📦 Inserting {len(measurements)} measurements in {total_batches} batches...")
            
            for i in range(0, len(measurements), self.batch_size):
                batch = measurements[i : i + self.batch_size]
                batch_num = i // self.batch_size + 1
                try:
                    # Scegli metodo in base a upsert_mode
                    if self.upsert_mode:
                        count = await meas_repo.bulk_upsert(batch)  # Gestisce duplicati
                    else:
                        count = await meas_repo.bulk_copy(batch)  # Veloce, no duplicati
                    
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
