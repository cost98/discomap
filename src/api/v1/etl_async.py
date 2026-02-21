"""Asynchronous ETL endpoints with batch processing (URL list based)."""

import asyncio
import logging
import uuid
from datetime import datetime
from enum import Enum
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator

from src.logger import get_logger
from src.services import ETLPipeline

logger = get_logger(__name__)
router = APIRouter(prefix="/async", tags=["\U0001F4E5 ETL - Batch"])

# In-memory job tracking (per produzione: Redis/Database)
_jobs: dict[str, dict] = {}


class JobStatus(str, Enum):
    """Job execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class BatchETLRequest(BaseModel):
    """Batch ETL request (async - multiple URLs)."""
    urls: list[str]
    
    @field_validator('urls')
    @classmethod
    def validate_urls(cls, v):
        if not v:
            raise ValueError("Lista URLs vuota")
        if len(v) > 50:
            raise ValueError("Massimo 50 URL per batch")
        return v


class BatchETLResponse(BaseModel):
    """Batch ETL response with job ID."""
    job_id: str
    status: JobStatus
    message: str
    total_urls: int
    created_at: datetime


class BatchJobStatus(BaseModel):
    """Batch job status details."""
    job_id: str
    status: JobStatus
    total_urls: int
    processed_urls: int
    failed_urls: int
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    stats: list[dict] | None = None
    errors: list[str] | None = None


@router.post("/batch", response_model=BatchETLResponse, status_code=202)
async def run_batch_etl(
    request: BatchETLRequest,
    upsert: bool = False,
):
    """
    Run ETL pipeline for multiple URLs (ASYNCHRONOUS).
    
    Starts background processing and returns job ID immediately.
    Use GET /async/status/{job_id} to check progress.
    
    Recommended for:
    - Multiple URLs (2-50)
    - Long-running operations
    - Avoiding HTTP timeouts
    
    Args:
        request: Batch ETL configuration with URL list
        
    Returns:
        Job ID and initial status (HTTP 202 Accepted)
        
    Example:
        ```json
        POST /api/v1/etl/async/batch
        {
            "urls": [
                "https://example.com/file1.parquet",
                "https://example.com/file2.parquet"
            ]
        }
        ```
    """
    job_id = str(uuid.uuid4())
    created_at = datetime.now()
    
    # Initialize job tracking
    _jobs[job_id] = {
        "status": JobStatus.PENDING,
        "total_urls": len(request.urls),
        "processed_urls": 0,
        "failed_urls": 0,
        "created_at": created_at,
        "started_at": None,
        "completed_at": None,
        "stats": [],
        "errors": [],
        "upsert": upsert,
    }
    
    # Start background task
    asyncio.create_task(
        _process_batch_job(job_id, request.urls, upsert)
    )
    
    logger.info(f"📦 Batch job {job_id} created: {len(request.urls)} URLs")
    
    return BatchETLResponse(
        job_id=job_id,
        status=JobStatus.PENDING,
        message=f"Batch job created with {len(request.urls)} URLs",
        total_urls=len(request.urls),
        created_at=created_at,
    )


@router.get("/status/{job_id}", response_model=BatchJobStatus)
async def get_batch_status(job_id: str):
    """
    Get batch job status and progress.
    
    Returns:
        Job status, progress, and results
        
    Example:
        ```
        GET /api/v1/etl/async/status/550e8400-e29b-41d4-a716-446655440000
        ```
    """
    if job_id not in _jobs:
        raise HTTPException(
            status_code=404,
            detail=f"Job {job_id} not found"
        )
    
    job = _jobs[job_id]
    
    return BatchJobStatus(
        job_id=job_id,
        status=job["status"],
        total_urls=job["total_urls"],
        processed_urls=job["processed_urls"],
        failed_urls=job["failed_urls"],
        created_at=job["created_at"],
        started_at=job["started_at"],
        completed_at=job["completed_at"],
        stats=job["stats"] if job["status"] == JobStatus.COMPLETED else None,
        errors=job["errors"] if job["errors"] else None,
    )


async def _process_batch_job(
    job_id: str,
    urls: list[str],
    upsert: bool = False,
):
    """
    Background task to process batch ETL job.
    
    Args:
        job_id: Unique job identifier
        urls: List of Parquet URLs to process
        upsert: Use bulk_upsert instead of bulk_copy
    """
    job = _jobs[job_id]
    job["status"] = JobStatus.RUNNING
    job["started_at"] = datetime.now()
    
    logger.info(f"🚀 Starting batch job {job_id}: {len(urls)} URLs (upsert={upsert})")
    
    pipeline = ETLPipeline(upsert_mode=upsert)
    
    for i, url in enumerate(urls, 1):
        try:
            logger.info(f"📥 [{i}/{len(urls)}] Processing: {url}")
            
            stats = await pipeline.run_from_url(url)
            
            job["stats"].append({
                "url": url,
                "success": True,
                "stats": stats,
            })
            job["processed_urls"] += 1
            
            logger.info(f"✅ [{i}/{len(urls)}] Completed: {stats['measurements']} measurements")
            
        except Exception as e:
            logger.error(f"❌ [{i}/{len(urls)}] Failed: {url} - {e}")
            job["errors"].append(f"{url}: {str(e)}")
            job["failed_urls"] += 1
            job["stats"].append({
                "url": url,
                "success": False,
                "error": str(e),
            })
    
    # Finalize job
    job["status"] = JobStatus.COMPLETED if job["failed_urls"] == 0 else JobStatus.FAILED
    job["completed_at"] = datetime.now()
    
    duration = (job["completed_at"] - job["started_at"]).total_seconds()
    logger.info(
        f"🏁 Batch job {job_id} finished: "
        f"{job['processed_urls']} succeeded, {job['failed_urls']} failed "
        f"in {duration:.2f}s"
    )

