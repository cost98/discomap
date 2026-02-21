"""File-based batch ETL endpoints with safe concurrency control."""

import logging
from fastapi import APIRouter, File, HTTPException, UploadFile

from src.logger import get_logger
from src.services.etl.batch_manager import BatchManager
from src.services.etl.models import (
    FileUploadResponse,
    MasterJobResponse,
    ProgressResponse,
    BatchJobResponse,
    JobListResponse,
)

logger = get_logger(__name__)
router = APIRouter(prefix="/async/file", tags=["\U0001F4E5 ETL - Files"])

# Batch manager for file-based processing with controlled concurrency
batch_manager = BatchManager(
    max_concurrent_batches=3,  # Safe concurrency limit (ridotto per evitare contention)
    batch_size=50,  # URLs per batch
    etl_batch_size=50000,  # DB insert batch size
)


@router.post("", response_model=FileUploadResponse)
async def upload_url_file(
    file: UploadFile = File(...),
    upsert: bool = False,
):
    """
    Upload a text file containing URLs for batch processing with SAFE concurrency control.
    
    File format: One URL per line (text/plain).
    
    Process:
    1. Parse URLs from file
    2. Divide into batches of 50 URLs
    3. Execute max 3 batches concurrently (safe limit)
    4. Return master job ID for tracking
    
    Example:
        ```bash
        curl -X POST http://localhost:8000/api/v1/etl/async/file \\
             -F "file=@urls.txt"
        ```
    
    Returns:
        Master job ID and processing information
    """
    try:
        # Read file content
        content = await file.read()
        text = content.decode('utf-8')
        
        # Parse URLs (one per line, skip empty lines and comments)
        urls = [
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.strip().startswith('#')
        ]
        
        if not urls:
            raise HTTPException(status_code=400, detail="No valid URLs found in file")
        
        logger.info(f"📁 File uploaded: {file.filename} - {len(urls)} URLs (upsert={upsert})")
        
        # Submit to batch manager
        master_job = await batch_manager.submit_file(urls, upsert=upsert)
        
        # Estimate duration (based on 143s for 50 URLs with max_concurrent=3)
        # With 3 concurrent batches, we can process ~3*50=150 URLs in 143s
        estimated_minutes = (len(urls) / 150) * 2.4  # 143s ≈ 2.4min
        
        return FileUploadResponse(
            master_job_id=master_job.master_job_id,
            message=f"Processing started for {len(urls)} URLs in {master_job.total_batches} batches",
            total_urls=len(urls),
            total_batches=master_job.total_batches,
            estimated_duration_minutes=round(estimated_minutes, 1),
        )
        
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded text")
    except Exception as e:
        logger.error(f"Error processing file upload: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/status/{master_job_id}", response_model=MasterJobResponse)
async def get_file_job_status(master_job_id: str, include_batches: bool = False):
    """
    Get status of a file-based master job.
    
    Args:
        master_job_id: Master job ID from file upload response
        include_batches: Include detailed batch information (default: false)
    
    Example:
        ```bash
        curl http://localhost:8000/api/v1/etl/async/file/status/{job_id}
        curl http://localhost:8000/api/v1/etl/async/file/status/{job_id}?include_batches=true
        ```
    
    Returns:
        Job status with progress information
    """
    master_job = batch_manager.get_job_status(master_job_id)
    
    if not master_job:
        raise HTTPException(status_code=404, detail=f"Job {master_job_id} not found")
    
    # Convert to response model
    batches = None
    if include_batches:
        batches = [
            BatchJobResponse(
                job_id=b.job_id,
                status=b.status.value,
                urls_count=len(b.urls),
                started_at=b.started_at,
                completed_at=b.completed_at,
                succeeded=b.succeeded,
                failed=b.failed,
                error=b.error,
                duration_seconds=b.duration_seconds,
            )
            for b in master_job.batches
        ]
    
    progress = master_job.progress
    
    return MasterJobResponse(
        master_job_id=master_job.master_job_id,
        status=master_job.status,
        total_urls=master_job.total_urls,
        total_batches=master_job.total_batches,
        batch_size=master_job.batch_size,
        created_at=master_job.created_at,
        started_at=master_job.started_at,
        completed_at=master_job.completed_at,
        progress=ProgressResponse(**progress),
        batches=batches,
    )


@router.get("/jobs", response_model=JobListResponse)
async def list_file_jobs(limit: int = 50):
    """
    List all file-based master jobs (most recent first).
    
    Args:
        limit: Maximum number of jobs to return (default: 50)
    
    Example:
        ```bash
        curl http://localhost:8000/api/v1/etl/async/file/jobs
        curl http://localhost:8000/api/v1/etl/async/file/jobs?limit=10
        ```
    
    Returns:
        List of master jobs with their status
    """
    jobs = batch_manager.list_jobs(limit=limit)
    
    job_responses = []
    for job in jobs:
        progress = job.progress
        job_responses.append(
            MasterJobResponse(
                master_job_id=job.master_job_id,
                status=job.status,
                total_urls=job.total_urls,
                total_batches=job.total_batches,
                batch_size=job.batch_size,
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at,
                progress=ProgressResponse(**progress),
                batches=None,  # Don't include batch details in list view
            )
        )
    
    return JobListResponse(
        jobs=job_responses,
        total=len(job_responses),
    )
