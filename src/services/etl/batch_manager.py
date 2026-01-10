"""Batch Manager with safe concurrency control.

Manages execution of multiple URL batches with controlled concurrency
to prevent database pool exhaustion.
"""

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from src.logger import get_logger
from src.services.etl.pipeline import ETLPipeline

logger = get_logger(__name__)


class JobStatus(str, Enum):
    """Status of a batch job."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class BatchJob:
    """Information about a single batch job."""
    job_id: str
    urls: List[str]
    status: JobStatus = JobStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    succeeded: int = 0
    failed: int = 0
    error: Optional[str] = None
    duration_seconds: Optional[float] = None


@dataclass
class MasterJob:
    """Master job tracking multiple batches."""
    master_job_id: str
    total_urls: int
    total_batches: int
    batch_size: int
    batches: List[BatchJob] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    @property
    def status(self) -> str:
        """Calculate overall job status."""
        if not self.batches:
            return "created"
        
        statuses = [b.status for b in self.batches]
        
        if all(s == JobStatus.COMPLETED for s in statuses):
            return "completed"
        elif all(s == JobStatus.FAILED for s in statuses):
            return "failed"
        elif any(s == JobStatus.RUNNING for s in statuses):
            return "running"
        elif any(s == JobStatus.COMPLETED or s == JobStatus.FAILED for s in statuses):
            return "running"
        else:
            return "pending"
    
    @property
    def progress(self) -> Dict[str, int]:
        """Calculate progress statistics."""
        completed = sum(1 for b in self.batches if b.status == JobStatus.COMPLETED)
        failed = sum(1 for b in self.batches if b.status == JobStatus.FAILED)
        running = sum(1 for b in self.batches if b.status == JobStatus.RUNNING)
        pending = sum(1 for b in self.batches if b.status == JobStatus.PENDING)
        
        total_succeeded = sum(b.succeeded for b in self.batches)
        total_failed = sum(b.failed for b in self.batches)
        
        return {
            "batches_completed": completed,
            "batches_failed": failed,
            "batches_running": running,
            "batches_pending": pending,
            "urls_succeeded": total_succeeded,
            "urls_failed": total_failed,
            "completion_pct": round((completed / self.total_batches * 100), 2) if self.total_batches > 0 else 0,
        }


class BatchManager:
    """
    Manages execution of multiple URL batches with controlled concurrency.
    
    Features:
    - Limits concurrent batch execution to prevent DB pool exhaustion
    - Tracks job status for all batches
    - Provides progress monitoring
    - Handles failures gracefully
    
    Example:
        >>> manager = BatchManager(max_concurrent_batches=10)
        >>> master_job = await manager.submit_file(urls, batch_size=50)
        >>> status = manager.get_job_status(master_job.master_job_id)
    """

    def __init__(
        self,
        max_concurrent_batches: int = 3,
        batch_size: int = 50,
        etl_batch_size: int = 50000,
    ):
        """
        Initialize batch manager.
        
        Args:
            max_concurrent_batches: Max number of batches running simultaneously (default 3)
            batch_size: Number of URLs per batch (default 50)
            etl_batch_size: Batch size for database inserts (default 50000)
        """
        self.max_concurrent_batches = max_concurrent_batches
        self.batch_size = batch_size
        self.etl_batch_size = etl_batch_size
        
        # In-memory job storage (use Redis/DB for production)
        self.jobs: Dict[str, MasterJob] = {}
        
        # Semaphore to limit concurrent batches
        self._semaphore = asyncio.Semaphore(max_concurrent_batches)
        
        logger.info(
            f"BatchManager initialized - max_concurrent_batches={max_concurrent_batches}, "
            f"batch_size={batch_size}, etl_batch_size={etl_batch_size}"
        )

    async def submit_file(self, urls: List[str]) -> MasterJob:
        """
        Submit a list of URLs for processing.
        
        Divides URLs into batches and schedules execution with controlled concurrency.
        
        Args:
            urls: List of Parquet URLs to process
            
        Returns:
            MasterJob with tracking information
        """
        master_job_id = str(uuid.uuid4())
        
        # Divide URLs into batches
        batches = []
        for i in range(0, len(urls), self.batch_size):
            batch_urls = urls[i:i + self.batch_size]
            batch_job = BatchJob(
                job_id=str(uuid.uuid4()),
                urls=batch_urls,
            )
            batches.append(batch_job)
        
        master_job = MasterJob(
            master_job_id=master_job_id,
            total_urls=len(urls),
            total_batches=len(batches),
            batch_size=self.batch_size,
            batches=batches,
        )
        
        self.jobs[master_job_id] = master_job
        
        logger.info(
            f"🚀 Master job {master_job_id} created - "
            f"{len(urls)} URLs divided into {len(batches)} batches"
        )
        
        # Start processing asynchronously (no await - fire and forget)
        asyncio.create_task(self._process_master_job(master_job))
        
        return master_job

    async def _process_master_job(self, master_job: MasterJob):
        """Process all batches of a master job with controlled concurrency."""
        master_job.started_at = datetime.utcnow()
        
        logger.info(f"🎯 Starting master job {master_job.master_job_id}")
        
        # Process all batches with concurrency control
        tasks = [
            self._process_batch_safe(batch)
            for batch in master_job.batches
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        master_job.completed_at = datetime.utcnow()
        duration = (master_job.completed_at - master_job.started_at).total_seconds()
        
        progress = master_job.progress
        logger.info(
            f"✅ Master job {master_job.master_job_id} completed in {duration:.1f}s - "
            f"Batches: {progress['batches_completed']}/{master_job.total_batches} completed, "
            f"URLs: {progress['urls_succeeded']}/{master_job.total_urls} succeeded"
        )

    async def _process_batch_safe(self, batch: BatchJob):
        """Process a single batch with semaphore control."""
        # Wait for semaphore (ensures max N batches running)
        async with self._semaphore:
            await self._process_batch(batch)

    async def _process_batch(self, batch: BatchJob):
        """Process a single batch of URLs."""
        batch.status = JobStatus.RUNNING
        batch.started_at = datetime.utcnow()
        
        logger.info(f"📦 Batch {batch.job_id[:8]} starting - {len(batch.urls)} URLs")
        
        try:
            pipeline = ETLPipeline(
                batch_size=self.etl_batch_size,
                max_concurrent_files=3,  # Optimal from testing
                cleanup_after_processing=True,
            )
            
            # Process all URLs in this batch
            results = await pipeline.run_batch_from_urls(batch.urls)
            
            batch.succeeded = results['files_processed']
            batch.failed = results['errors']
            batch.status = JobStatus.COMPLETED
            
            logger.info(
                f"✅ Batch {batch.job_id[:8]} completed - "
                f"{batch.succeeded} succeeded, {batch.failed} failed"
            )
            
        except Exception as e:
            batch.status = JobStatus.FAILED
            batch.error = str(e)
            logger.error(f"❌ Batch {batch.job_id[:8]} failed: {e}")
        
        finally:
            batch.completed_at = datetime.utcnow()
            if batch.started_at:
                batch.duration_seconds = (
                    batch.completed_at - batch.started_at
                ).total_seconds()

    def get_job_status(self, master_job_id: str) -> Optional[MasterJob]:
        """Get status of a master job."""
        return self.jobs.get(master_job_id)

    def list_jobs(self, limit: int = 50) -> List[MasterJob]:
        """List all jobs (most recent first)."""
        jobs = list(self.jobs.values())
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        return jobs[:limit]
