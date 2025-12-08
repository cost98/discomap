"""
Sync Scheduler - Automated Data Synchronization

This script handles automated synchronization of EEA air quality data.
It can be run manually or scheduled via cron/Task Scheduler.

Usage:
    python scripts/sync_scheduler.py              # Full sync
    python scripts/sync_scheduler.py --test       # Test mode (no actual download)
    python scripts/sync_scheduler.py --incremental # Sync since last run
    python scripts/sync_scheduler.py --hourly     # Hourly sync (recent data only)
"""

import argparse
import csv
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.analyzer import get_summary
from src.config import Config
from src.db_writer import PostgreSQLWriter
from src.downloader import EEADownloader
from src.logger import get_logger
from src.parser import read_from_zip, read_parquet
from src.utils import format_duration, format_file_size, get_last_n_days_range
from src.validators import clean_dataframe, validate_dataframe

logger = get_logger(__name__)


class SyncState:
    """Manage synchronization state and history."""

    def __init__(self, state_file: Path = None):
        self.state_file = state_file or Config.DATA_DIR / "sync_state.json"
        self.state = self._load_state()

    def _load_state(self) -> Dict:
        """Load state from file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load state: {e}")

        return {
            "last_sync": None,
            "last_full_sync": None,
            "sync_history": [],
            "total_records": 0,
            "failed_attempts": 0,
        }

    def _save_state(self):
        """Save state to file."""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def get_last_sync_time(self) -> Optional[datetime]:
        """Get timestamp of last successful sync."""
        if self.state["last_sync"]:
            return datetime.fromisoformat(self.state["last_sync"])
        return None

    def update_success(self, records_processed: int, sync_type: str = "incremental"):
        """Record successful sync."""
        now = datetime.now()

        self.state["last_sync"] = now.isoformat()
        if sync_type == "full":
            self.state["last_full_sync"] = now.isoformat()

        self.state["total_records"] += records_processed
        self.state["failed_attempts"] = 0

        self.state["sync_history"].append(
            {
                "timestamp": now.isoformat(),
                "type": sync_type,
                "records": records_processed,
                "status": "success",
            }
        )

        # Keep only last 100 history entries
        self.state["sync_history"] = self.state["sync_history"][-100:]

        self._save_state()
        logger.info(f"✅ Sync state updated: {records_processed} records processed")

    def update_failure(self, error: str, sync_type: str = "incremental"):
        """Record failed sync attempt."""
        now = datetime.now()

        self.state["failed_attempts"] += 1
        self.state["sync_history"].append(
            {
                "timestamp": now.isoformat(),
                "type": sync_type,
                "error": str(error),
                "status": "failed",
            }
        )

        self.state["sync_history"] = self.state["sync_history"][-100:]
        self._save_state()

        logger.error(f"❌ Sync failed: {error}")


class SyncScheduler:
    """Main synchronization orchestrator."""

    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.state = SyncState()
        self.downloader = EEADownloader(output_dir=Config.DATA_RAW)
        self.db_writer = PostgreSQLWriter()

        # Default sync configuration
        self.config = {
            "countries": [
                "IT", "FR", "ES", "DE", "NL", "BE", "AT", "PL"
            ],  # Main European countries (8)
            "pollutants": [
                "NO2", "PM10", "PM2.5", "O3", "SO2", "CO"
            ],  # Core pollutants (6)
            "dataset": Config.DATASET_E2A,  # Up-to-date data
            "aggregation": Config.AGG_HOURLY,
            "lookback_days": 7,  # Default lookback period
            "hourly_lookback_hours": 2,  # For hourly syncs
        }

        logger.info(f"🔄 SyncScheduler initialized (test_mode={test_mode})")

    def sync_full(self) -> bool:
        """Perform full synchronization (last N days)."""
        logger.info("=" * 60)
        logger.info("📦 Starting FULL SYNC")
        logger.info("=" * 60)

        # Start tracking operation
        operation_id = self.db_writer.start_sync_operation(
            operation_type="full",
            metadata={
                "countries": self.config["countries"],
                "pollutants": self.config["pollutants"],
                "lookback_days": self.config["lookback_days"],
            },
        )

        try:
            # Calculate date range
            start_date, end_date = get_last_n_days_range(self.config["lookback_days"])

            logger.info(f"📅 Date range: {start_date} to {end_date}")
            logger.info(f"🌍 Countries: {', '.join(self.config['countries'])}")
            logger.info(f"🏭 Pollutants: {', '.join(self.config['pollutants'])}")

            if self.test_mode:
                logger.info("⚠️  TEST MODE - No actual download")
                self.state.update_success(0, sync_type="full")
                self.db_writer.complete_sync_operation(operation_id, 0, 0)
                return True

            # Download data
            filename = f"sync_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            download_start = datetime.now()

            zip_path = self.downloader.download(
                countries=self.config["countries"],
                pollutants=self.config["pollutants"],
                dataset=self.config["dataset"],
                datetime_start=start_date,
                datetime_end=end_date,
                aggregation_type=self.config["aggregation"],
                filename=filename,
            )

            download_duration = (datetime.now() - download_start).total_seconds()

            if not zip_path or not zip_path.exists():
                raise Exception("Download failed - no file created")

            logger.info(
                f"✅ Downloaded: {format_file_size(zip_path.stat().st_size)} "
                f"in {format_duration(download_duration)}"
            )

            # Process and validate
            records_downloaded, records_inserted = self._process_downloaded_data(zip_path)

            # Update state
            self.state.update_success(records_inserted, sync_type="full")

            # Mark operation as completed
            self.db_writer.complete_sync_operation(operation_id, records_downloaded, records_inserted)

            logger.info("=" * 60)
            logger.info(f"✅ FULL SYNC COMPLETED - {records_inserted} records")
            logger.info("=" * 60)

            return True

        except Exception as e:
            logger.error(f"❌ Full sync failed: {e}", exc_info=True)
            self.state.update_failure(str(e), sync_type="full")
            self.db_writer.fail_sync_operation(operation_id, str(e))
            return False

    def sync_incremental(self) -> bool:
        """Perform incremental sync since last successful sync."""
        logger.info("=" * 60)
        logger.info("🔄 Starting INCREMENTAL SYNC")
        logger.info("=" * 60)

        # Start tracking operation
        operation_id = self.db_writer.start_sync_operation(
            operation_type="incremental",
            metadata={
                "countries": self.config["countries"],
                "pollutants": self.config["pollutants"],
            },
        )

        try:
            # Determine start date
            last_sync = self.state.get_last_sync_time()

            if last_sync:
                # Sync from last sync with 1 hour overlap to avoid gaps
                start_date = (last_sync - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.info(f"📅 Syncing from last sync: {last_sync}")
            else:
                # No previous sync - fallback to full sync
                logger.info("📅 No previous sync found - performing full sync")
                self.db_writer.fail_sync_operation(operation_id, "No previous sync - fallback to full")
                return self.sync_full()

            end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

            logger.info(f"📅 Date range: {start_date} to {end_date}")
            logger.info(f"🌍 Countries: {', '.join(self.config['countries'])}")
            logger.info(f"🏭 Pollutants: {', '.join(self.config['pollutants'])}")

            if self.test_mode:
                logger.info("⚠️  TEST MODE - No actual download")
                self.state.update_success(0, sync_type="incremental")
                self.db_writer.complete_sync_operation(operation_id, 0, 0)
                return True

            # Download incremental data
            filename = f"sync_incremental_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            download_start = datetime.now()

            zip_path = self.downloader.download(
                countries=self.config["countries"],
                pollutants=self.config["pollutants"],
                dataset=self.config["dataset"],
                datetime_start=start_date,
                datetime_end=end_date,
                aggregation_type=self.config["aggregation"],
                filename=filename,
            )

            download_duration = (datetime.now() - download_start).total_seconds()

            if not zip_path or not zip_path.exists():
                logger.warning("⚠️  No new data available")
                self.db_writer.complete_sync_operation(operation_id, 0, 0)
                return True

            logger.info(
                f"✅ Downloaded: {format_file_size(zip_path.stat().st_size)} "
                f"in {format_duration(download_duration)}"
            )

            # Process and validate
            records_downloaded, records_inserted = self._process_downloaded_data(zip_path)

            # Update state
            self.state.update_success(records_inserted, sync_type="incremental")

            # Mark operation as completed
            self.db_writer.complete_sync_operation(operation_id, records_downloaded, records_inserted)

            logger.info("=" * 60)
            logger.info(f"✅ INCREMENTAL SYNC COMPLETED - {records_inserted} records")
            logger.info("=" * 60)

            return True

        except Exception as e:
            logger.error(f"❌ Incremental sync failed: {e}", exc_info=True)
            self.state.update_failure(str(e), sync_type="incremental")
            self.db_writer.fail_sync_operation(operation_id, str(e))
            return False

    def sync_hourly(self) -> bool:
        """Quick hourly sync (last 2 hours of data)."""
        logger.info("=" * 60)
        logger.info("⏰ Starting HOURLY SYNC")
        logger.info("=" * 60)

        # Start tracking operation
        operation_id = self.db_writer.start_sync_operation(
            operation_type="hourly",
            metadata={
                "countries": self.config["countries"],
                "pollutants": self.config["pollutants"],
                "lookback_hours": self.config["hourly_lookback_hours"],
            },
        )

        try:
            # Get last 2 hours
            now = datetime.now()
            start_time = now - timedelta(hours=self.config["hourly_lookback_hours"])

            start_date = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")

            logger.info(
                f"📅 Last {self.config['hourly_lookback_hours']} hours: {start_date} to {end_date}"
            )

            if self.test_mode:
                logger.info("⚠️  TEST MODE - No actual download")
                self.state.update_success(0, sync_type="hourly")
                self.db_writer.complete_sync_operation(operation_id, 0, 0)
                return True

            # Download recent data
            filename = f"sync_hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"

            zip_path = self.downloader.download(
                countries=self.config["countries"],
                pollutants=self.config["pollutants"],
                dataset=self.config["dataset"],
                datetime_start=start_date,
                datetime_end=end_date,
                aggregation_type=self.config["aggregation"],
                filename=filename,
            )

            if not zip_path or not zip_path.exists():
                logger.info("ℹ️  No new hourly data available")
                self.db_writer.complete_sync_operation(operation_id, 0, 0)
                return True

            # Quick process (no deep validation for hourly)
            records_downloaded, records_inserted = self._process_downloaded_data(zip_path, quick=True)

            # Update state
            self.state.update_success(records_inserted, sync_type="hourly")

            # Mark operation as completed
            self.db_writer.complete_sync_operation(operation_id, records_downloaded, records_inserted)

            logger.info(f"✅ HOURLY SYNC COMPLETED - {records_inserted} records")

            return True

        except Exception as e:
            logger.error(f"❌ Hourly sync failed: {e}", exc_info=True)
            self.state.update_failure(str(e), sync_type="hourly")
            self.db_writer.fail_sync_operation(operation_id, str(e))
            return False

    def sync_custom_period(
        self, start_date: str, end_date: str, sync_type: str = "custom"
    ) -> bool:
        """
        Perform sync for a custom date range.
        Uses dataset specified in self.config["dataset"].
        
        Args:
            start_date: Start date (ISO format: YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD)
            end_date: End date (ISO format: YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD)
            sync_type: Type label for tracking (default: 'custom')
        
        Returns:
            bool: Success status
        """
        logger.info("=" * 60)
        logger.info(f"📅 Starting CUSTOM PERIOD SYNC")
        logger.info("=" * 60)

        try:
            # Normalize dates to ISO format if needed
            if len(start_date) == 10:  # YYYY-MM-DD
                start_date = f"{start_date}T00:00:00Z"
            if len(end_date) == 10:  # YYYY-MM-DD
                end_date = f"{end_date}T23:59:59Z"

            logger.info(f"📅 Date range: {start_date} to {end_date}")
            logger.info(f"🌍 Countries: {', '.join(self.config['countries'])}")
            logger.info(f"🏭 Pollutants: {', '.join(self.config['pollutants'])}")

            # Use dataset from config
            from src.config import DATASET_NAMES
            dataset = self.config.get("dataset", Config.DATASET_E2A)
            dataset_name = DATASET_NAMES.get(dataset, f"Dataset {dataset}")
            logger.info(f"📦 Using dataset: {dataset_name}")

            # Start tracking operation
            operation_id = self.db_writer.start_sync_operation(
                operation_type=sync_type,
                metadata={
                    "countries": self.config["countries"],
                    "pollutants": self.config["pollutants"],
                    "start_date": start_date,
                    "end_date": end_date,
                    "dataset": dataset,
                    "dataset_name": dataset_name,
                },
            )

            if self.test_mode:
                logger.info("⚠️  TEST MODE - No actual download")
                self.db_writer.complete_sync_operation(operation_id, 0, 0)
                return True

            # Download data
            filename = f"sync_{sync_type}_{start_date[:10]}_{end_date[:10]}_dataset{dataset}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            download_start = datetime.now()

            zip_path = self.downloader.download(
                countries=self.config["countries"],
                pollutants=self.config["pollutants"],
                dataset=dataset,
                datetime_start=start_date,
                datetime_end=end_date,
                aggregation_type=self.config["aggregation"],
                filename=filename,
            )

            download_duration = (datetime.now() - download_start).total_seconds()

            if not zip_path or not zip_path.exists():
                logger.warning("⚠️  No data available for this period")
                self.db_writer.complete_sync_operation(operation_id, 0, 0)
                return True

            logger.info(
                f"✅ Downloaded: {format_file_size(zip_path.stat().st_size)} "
                f"in {format_duration(download_duration)}"
            )

            # Process and validate
            records_downloaded, records_inserted = self._process_downloaded_data(zip_path)

            # Update state
            self.state.update_success(records_inserted, sync_type=sync_type)

            # Mark operation as completed
            self.db_writer.complete_sync_operation(operation_id, records_downloaded, records_inserted)

            logger.info("=" * 60)
            logger.info(f"✅ CUSTOM SYNC COMPLETED - {records_inserted} records")
            logger.info("=" * 60)

            return True

        except Exception as e:
            logger.error(f"❌ Custom sync failed: {e}", exc_info=True)
            self.state.update_failure(str(e), sync_type=sync_type)
            self.db_writer.fail_sync_operation(operation_id, str(e))
            return False

    def sync_custom_period_urls(
        self,
        start_date: str,
        end_date: str,
        sync_type: str = "custom_urls",
        max_workers: int = 4,
    ) -> bool:
        """
        Perform sync for a custom date range using URL-based download.
        
        This method is more reliable for large date ranges:
        1. Requests CSV with list of Parquet URLs
        2. Downloads files in parallel
        3. Processes each file independently
        
        Best for: Large historical syncs (>7 days) that may timeout with direct download.
        
        Args:
            start_date: Start date (ISO format: YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD)
            end_date: End date (ISO format: YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD)
            sync_type: Type label for tracking (default: 'custom_urls')
            max_workers: Number of parallel downloads (default: 4)
        
        Returns:
            bool: Success status
        """
        logger.info("=" * 60)
        logger.info(f"📋 Starting URL-BASED SYNC (parallel download)")
        logger.info("=" * 60)

        # Start tracking operation
        operation_id = self.db_writer.start_sync_operation(
            operation_type=sync_type,
            metadata={
                "countries": self.config["countries"],
                "pollutants": self.config["pollutants"],
                "start_date": start_date,
                "end_date": end_date,
                "max_workers": max_workers,
            },
        )

        try:
            # Normalize dates to ISO format if needed
            if len(start_date) == 10:  # YYYY-MM-DD
                start_date = f"{start_date}T00:00:00Z"
            if len(end_date) == 10:  # YYYY-MM-DD
                end_date = f"{end_date}T23:59:59Z"

            logger.info(f"📅 Date range: {start_date} to {end_date}")
            logger.info(f"🌍 Countries: {', '.join(self.config['countries'])}")
            logger.info(f"🏭 Pollutants: {', '.join(self.config['pollutants'])}")
            logger.info(f"⚡ Parallel workers: {max_workers}")

            if self.test_mode:
                logger.info("⚠️  TEST MODE - No actual download")
                self.db_writer.complete_sync_operation(operation_id, 0, 0)
                return True

            # Step 1: Get URL list from API
            logger.info("📋 Requesting URL list from EEA API...")
            csv_path = self.downloader.download_urls(
                countries=self.config["countries"],
                pollutants=self.config["pollutants"],
                dataset=self.config["dataset"],
                datetime_start=start_date,
                datetime_end=end_date,
            )

            # Step 2: Parse CSV to get URLs
            urls = []
            with open(csv_path, "r", encoding="utf-8-sig") as f:  # utf-8-sig removes BOM
                # Read content and strip BOM if present
                content = f.read()
                if content.startswith('\ufeff'):
                    content = content[1:]
                
                # Parse CSV from clean content
                import io
                reader = csv.DictReader(io.StringIO(content))
                for row in reader:
                    # Get URL from the first column (whatever it's called)
                    url = list(row.values())[0] if row else None
                    if url and url.startswith('http'):
                        urls.append(url)

            logger.info(f"📋 Found {len(urls)} Parquet files to download")

            if not urls:
                logger.warning("⚠️  No URLs found in response")
                self.db_writer.complete_sync_operation(operation_id, 0, 0)
                return True

            # Step 3: Download files in parallel
            parquet_dir = Config.DATA_RAW / "parquet_files"
            parquet_dir.mkdir(parents=True, exist_ok=True)

            downloaded_files = []
            total_downloaded = 0
            total_inserted = 0

            def download_file(url: str, index: int) -> Optional[Path]:
                """Download single Parquet file."""
                try:
                    filename = f"file_{index:04d}.parquet"
                    filepath = parquet_dir / filename

                    response = requests.get(url, timeout=120)
                    response.raise_for_status()

                    filepath.write_bytes(response.content)
                    return filepath
                except Exception as e:
                    logger.warning(f"Failed to download {url}: {e}")
                    return None

            logger.info(f"⬇️  Downloading {len(urls)} files with {max_workers} workers...")
            download_start = datetime.now()

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_url = {
                    executor.submit(download_file, url, i): (url, i)
                    for i, url in enumerate(urls)
                }

                for future in as_completed(future_to_url):
                    filepath = future.result()
                    if filepath:
                        downloaded_files.append(filepath)
                        if len(downloaded_files) % 10 == 0:
                            logger.info(
                                f"   📥 Downloaded {len(downloaded_files)}/{len(urls)} files..."
                            )

            download_duration = (datetime.now() - download_start).total_seconds()
            logger.info(
                f"✅ Downloaded {len(downloaded_files)}/{len(urls)} files "
                f"in {format_duration(download_duration)}"
            )

            # Step 4: Process each Parquet file
            logger.info("🔄 Processing and inserting data...")
            process_start = datetime.now()

            for i, parquet_file in enumerate(downloaded_files):
                try:
                    # Read Parquet file
                    df = read_parquet(parquet_file)
                    if df is None or df.empty:
                        continue

                    # Validate (returns ValidationResult, not DataFrame)
                    validation = validate_dataframe(df, strict=False)
                    
                    # Log validation results
                    if not validation.is_valid:
                        logger.warning(
                            f"File {parquet_file.name}: {validation.valid_records}/{validation.total_records} valid records"
                        )
                    
                    # Even if validation failed, try to clean and insert valid records
                    cleaned_df = clean_dataframe(df)
                    if cleaned_df is None or cleaned_df.empty:
                        logger.warning(f"No valid data after cleaning {parquet_file.name}")
                        continue

                    # First, insert sampling points (upsert to avoid duplicates)
                    try:
                        self.db_writer.upsert_sampling_points(cleaned_df)
                    except Exception as e:
                        logger.error(f"Failed to upsert sampling points for {parquet_file.name}: {e}")
                        continue
                    
                    # Then insert measurements
                    inserted = self.db_writer.insert_measurements(cleaned_df)
                    total_downloaded += len(df)
                    total_inserted += inserted

                    if (i + 1) % 10 == 0:
                        logger.info(
                            f"   💾 Processed {i+1}/{len(downloaded_files)} files "
                            f"({total_inserted:,} records inserted)"
                        )

                    # Cleanup
                    parquet_file.unlink()

                except Exception as e:
                    logger.warning(f"Failed to process {parquet_file}: {e}")
                    continue

            process_duration = (datetime.now() - process_start).total_seconds()

            # Update state
            self.state.update_success(total_inserted, sync_type=sync_type)

            # Mark operation as completed
            self.db_writer.complete_sync_operation(operation_id, total_downloaded, total_inserted)

            logger.info("=" * 60)
            logger.info(
                f"✅ URL-BASED SYNC COMPLETED - {total_inserted:,} records inserted "
                f"(from {total_downloaded:,} downloaded)"
            )
            logger.info(f"   ⏱️  Download: {format_duration(download_duration)}")
            logger.info(f"   ⏱️  Processing: {format_duration(process_duration)}")
            logger.info("=" * 60)

            # Cleanup CSV
            csv_path.unlink()

            return True

        except Exception as e:
            logger.error(f"❌ URL-based sync failed: {e}", exc_info=True)
            self.state.update_failure(str(e), sync_type=sync_type)
            self.db_writer.fail_sync_operation(operation_id, str(e))
            return False

    def _process_downloaded_data(self, zip_path: Path, quick: bool = False) -> tuple[int, int]:
        """
        Process, validate and store downloaded data.
        
        Returns:
            tuple: (records_downloaded, records_inserted)
        """
        logger.info("🔍 Processing downloaded data...")

        try:
            # Extract and read parquet files
            df = read_from_zip(zip_path)

            if df.empty:
                logger.warning("⚠️  No data found in archive")
                return (0, 0)

            initial_count = len(df)
            logger.info(f"📂 Loaded {initial_count:,} records from archive")

            if quick:
                # Quick mode: minimal validation, still insert to DB
                logger.info(f"⚡ Quick mode: skipping validation")
                
                try:
                    db_writer = PostgreSQLWriter()
                    points_upserted = db_writer.upsert_sampling_points(df)
                    inserted = db_writer.insert_measurements(df)
                    db_writer.close_all()
                    logger.info(f"   ✅ Inserted {inserted:,} measurements")
                    return (initial_count, inserted)
                except Exception as db_error:
                    logger.error(f"   ❌ Database insertion failed: {db_error}")
                    return (initial_count, 0)

            # Full validation
            validation = validate_dataframe(df, strict=False)
            logger.info(
                f"   Validation: {validation.valid_records:,} valid, "
                f"{validation.invalid_records:,} invalid"
            )

            if validation.issues:
                logger.warning(f"   ⚠️  {len(validation.issues)} validation issues found")

            # Clean data
            df_clean = clean_dataframe(
                df, remove_invalid=True, remove_duplicates=True, remove_nulls=True
            )

            cleaned_count = len(df_clean)
            logger.info(
                f"   Cleaned: {cleaned_count:,} records "
                f"({cleaned_count/initial_count*100:.1f}% retained)"
            )

            # Get summary
            summary = get_summary(df_clean)
            logger.info(
                f"   Summary: {summary['sampling_points']} stations, "
                f"{len(summary['pollutants'])} pollutants"
            )

            # Export cleaned data
            zip_name = zip_path.stem
            output_file = Config.DATA_PROCESSED / f"{zip_name}_cleaned.parquet"
            from src.parser import export_to_parquet

            export_to_parquet(df_clean, output_file)
            logger.info(f"   ✅ Exported to: {output_file.name}")

            # Insert into database
            inserted = 0
            try:
                db_writer = PostgreSQLWriter()

                # First, insert sampling points (stations)
                logger.info("   📍 Upserting sampling points...")
                points_upserted = db_writer.upsert_sampling_points(df_clean)
                logger.info(f"   ✅ Upserted {points_upserted:,} sampling points")

                # Then, insert measurements
                logger.info("   📊 Inserting measurements...")
                inserted = db_writer.insert_measurements(df_clean)
                logger.info(f"   ✅ Inserted {inserted:,} measurements into database")

                db_writer.close_all()
            except Exception as db_error:
                logger.error(f"   ❌ Database insertion failed: {db_error}")
                # Continue even if DB insert fails - data is already saved to file

            # Cleanup: delete ZIP file and temp files
            try:
                logger.info(f"   🗑️  Cleaning up temporary files...")
                if zip_path.exists():
                    zip_path.unlink()
                    logger.info(f"   ✅ Deleted ZIP: {zip_path.name}")
                
                # Delete processed file (already in DB)
                if output_file.exists():
                    output_file.unlink()
                    logger.info(f"   ✅ Deleted processed: {output_file.name}")
                
                # Also cleanup temp directory
                temp_dir = Config.get_temp_dir()
                import shutil
                if temp_dir.exists():
                    for item in temp_dir.iterdir():
                        if item.is_file():
                            item.unlink()
                        elif item.is_dir():
                            shutil.rmtree(item)
                    logger.info(f"   ✅ Cleaned temp directory")
            except Exception as cleanup_error:
                logger.warning(f"   ⚠️  Cleanup failed: {cleanup_error}")

            logger.info(f"✅ Processing complete: {cleaned_count:,} total records")
            return (initial_count, inserted)

        except Exception as e:
            logger.error(f"❌ Failed to process data: {e}", exc_info=True)
            raise

    def get_status(self) -> Dict:
        """Get current sync status and statistics."""
        last_sync = self.state.get_last_sync_time()

        # Calculate recent success rate
        recent_history = self.state.state["sync_history"][-10:]
        successful = sum(1 for h in recent_history if h["status"] == "success")
        success_rate = (successful / len(recent_history) * 100) if recent_history else 0

        status = {
            "last_sync": last_sync.isoformat() if last_sync else None,
            "last_full_sync": self.state.state["last_full_sync"],
            "total_records": self.state.state["total_records"],
            "failed_attempts": self.state.state["failed_attempts"],
            "recent_success_rate": f"{success_rate:.1f}%",
            "sync_count": len(self.state.state["sync_history"]),
            "config": self.config,
        }

        return status

    def print_status(self):
        """Print formatted sync status."""
        status = self.get_status()

        print("\n" + "=" * 60)
        print("📊 SYNC STATUS")
        print("=" * 60)
        print(f"Last Sync:        {status['last_sync'] or 'Never'}")
        print(f"Last Full Sync:   {status['last_full_sync'] or 'Never'}")
        print(f"Total Records:    {status['total_records']:,}")
        print(f"Failed Attempts:  {status['failed_attempts']}")
        print(f"Success Rate:     {status['recent_success_rate']} (last 10 runs)")
        print(f"Total Syncs:      {status['sync_count']}")
        print("\nConfiguration:")
        print(f"  Countries:      {', '.join(status['config']['countries'])}")
        print(f"  Pollutants:     {', '.join(status['config']['pollutants'])}")
        print(f"  Dataset:        E2a (UTD)")
        print(f"  Lookback:       {status['config']['lookback_days']} days")
        print("=" * 60 + "\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="EEA Air Quality Data Sync Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard syncs (use E2a by default)
  python scripts/sync_scheduler.py                    # Full sync (last 7 days, E2a)
  python scripts/sync_scheduler.py --incremental      # Sync since last run
  python scripts/sync_scheduler.py --hourly           # Quick hourly sync
  python scripts/sync_scheduler.py --status           # Show sync status
  python scripts/sync_scheduler.py --test             # Test mode (no download)
  
  # Custom period syncs with dataset selection
  python scripts/sync_scheduler.py --days 30 --dataset 1                                          # Last 30 days (E2a)
  python scripts/sync_scheduler.py --start-date 2025-01-01 --end-date 2025-01-31 --dataset 1     # January 2025 (E2a)
  python scripts/sync_scheduler.py --start-date 2024-01-01 --end-date 2024-12-31 --dataset 2     # Full 2024 (E1a)
  python scripts/sync_scheduler.py --start-date 2020-01-01 --end-date 2023-12-31 --dataset 2     # Historical (E1a)
  
  # Filter by countries/pollutants
  python scripts/sync_scheduler.py --start-date 2024-11-01 --end-date 2024-11-30 --dataset 2 --countries IT FR ES
  python scripts/sync_scheduler.py --days 90 --dataset 1 --pollutants NO2 PM10
  
Datasets (must specify with --dataset):
  1 = E2a (Up-To-Date): 2025+ data, unverified, near real-time [DEFAULT]
  2 = E1a (Verified): 2013-2024 data, verified, reported annually by Sep 30
  3 = Airbase (Historical): 2002-2012 data, pre-directive
  
Scheduling:
  Windows Task Scheduler:
    schtasks /create /tn "DiscoMap-Hourly" /tr "python C:\\path\\to\\sync_scheduler.py --hourly" /sc hourly
  
  Linux Cron:
    0 * * * * cd /path/to/discomap && .venv/bin/python scripts/sync_scheduler.py --hourly
    0 2 * * * cd /path/to/discomap && .venv/bin/python scripts/sync_scheduler.py --incremental
        """,
    )

    parser.add_argument(
        "--incremental", action="store_true", help="Incremental sync since last run"
    )
    parser.add_argument("--hourly", action="store_true", help="Quick hourly sync (last 2 hours)")
    parser.add_argument("--full", action="store_true", help="Full sync (last 7 days)")
    parser.add_argument("--test", action="store_true", help="Test mode (no actual download)")
    parser.add_argument("--status", action="store_true", help="Show sync status and exit")
    parser.add_argument("--countries", nargs="+", help="Countries to sync (default: IT)")
    parser.add_argument(
        "--pollutants", nargs="+", help="Pollutants to sync (default: NO2 PM10 O3 SO2)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for custom sync (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for custom sync (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Number of days to sync (from end-date or now, going backwards)",
    )
    parser.add_argument(
        "--dataset",
        type=int,
        choices=[1, 2, 3],
        required=False,
        default=1,
        help="Dataset: 1=E2a (UTD, 2025+), 2=E1a (Verified, 2013-2024), 3=Airbase (2002-2012). Default: 1 (E2a)",
    )
    parser.add_argument(
        "--use-urls",
        action="store_true",
        help="Use URL-based download (more reliable for large date ranges, downloads files in parallel)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Number of parallel download workers when using --use-urls (default: 4)",
    )

    args = parser.parse_args()

    # Initialize scheduler
    scheduler = SyncScheduler(test_mode=args.test)

    # Override config if specified
    if args.countries:
        scheduler.config["countries"] = args.countries
    if args.pollutants:
        scheduler.config["pollutants"] = args.pollutants
    if args.dataset:
        scheduler.config["dataset"] = args.dataset
        from src.config import DATASET_NAMES
        logger.info(f"📦 Using dataset: {DATASET_NAMES.get(args.dataset, f'Dataset {args.dataset}')}")

    # Show status and exit
    if args.status:
        scheduler.print_status()
        return 0

    # Handle custom date range sync
    if args.start_date or args.end_date or args.days:
        from datetime import datetime, timedelta

        # Calculate dates
        if args.days:
            # Use --days to calculate range
            end_dt = datetime.now() if not args.end_date else datetime.fromisoformat(args.end_date.replace("Z", ""))
            start_dt = end_dt - timedelta(days=args.days)
            start_date = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_date = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif args.start_date and args.end_date:
            # Use explicit start and end dates
            start_date = args.start_date
            end_date = args.end_date
        else:
            logger.error("❌ Must specify both --start-date and --end-date, or use --days")
            return 1

        logger.info(f"📅 Custom period sync: {start_date} to {end_date}")
        
        # Use URL-based download if requested
        if args.use_urls:
            success = scheduler.sync_custom_period_urls(
                start_date, end_date, max_workers=args.max_workers
            )
        else:
            success = scheduler.sync_custom_period(start_date, end_date)
    # Determine sync type
    elif args.hourly:
        success = scheduler.sync_hourly()
    elif args.incremental:
        success = scheduler.sync_incremental()
    elif args.full:
        success = scheduler.sync_full()
    else:
        # Default: incremental (or full if first run)
        success = scheduler.sync_incremental()

    # Show final status
    scheduler.print_status()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
