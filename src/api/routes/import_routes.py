"""
Consolidated Import API Routes

Single unified API for importing EEA air quality data.
Replaces old sync API and bulk import API with cleaner interface.
"""

from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field

from src.logger import get_logger
from src.services.parquet_copy_importer import import_eea_data_with_copy
from src.database.session import get_async_session

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/import", tags=["Import"])

# In-memory job storage (replace with database in production)
jobs: Dict[str, dict] = {}


# === Models ===

class ImportRequest(BaseModel):
    """Request to import Parquet files from URLs."""
    urls: List[str] = Field(..., description="List of Parquet file URLs (from /sync/get-urls)", min_items=1)
    max_workers: int = Field(8, description="Parallel workers", ge=1, le=32)


class JobResponse(BaseModel):
    """Response with job information."""
    job_id: str
    status: str
    created_at: str
    message: str


class JobStatus(BaseModel):
    """Detailed job status."""
    job_id: str
    status: str
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    elapsed_seconds: Optional[float] = None
    import_rate: Optional[float] = None
    error_message: Optional[str] = None


class StatsResponse(BaseModel):
    """Import statistics."""
    total_jobs: int
    completed_jobs: int
    failed_jobs: int
    running_jobs: int
    total_records_imported: int
    average_import_rate: float


# === Background Tasks ===

async def _run_import_job(job_id: str, urls: List[str], max_workers: int):
    """Execute import job in background."""
    jobs[job_id]["status"] = "running"
    jobs[job_id]["started_at"] = datetime.utcnow().isoformat()
    start_time = datetime.utcnow()
    
    try:
        async for session in get_async_session():
            result = await import_eea_data_with_copy(
                session=session,
                parquet_urls=urls,
                max_workers=max_workers,
            )
            
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
            jobs[job_id]["total_records"] = result.get("total_records", 0)
            jobs[job_id]["processed_records"] = result.get("total_records", 0)
            jobs[job_id]["elapsed_seconds"] = (datetime.utcnow() - start_time).total_seconds()
            jobs[job_id]["import_rate"] = result.get("rate", 0)
            break
            
    except Exception as e:
        logger.error(f"Import job {job_id} failed: {e}")
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
        jobs[job_id]["error_message"] = str(e)
        jobs[job_id]["elapsed_seconds"] = (datetime.utcnow() - start_time).total_seconds()


# === Endpoints ===

@router.post("/parquet", response_model=JobResponse, summary="Import Parquet files from URLs")
async def import_from_urls(
    request: ImportRequest,
    background_tasks: BackgroundTasks,
):
    """
    Import EEA Parquet files from URL list using high-performance COPY.
    
    - **urls**: List of Parquet file URLs to import
    - **max_workers**: Number of parallel workers (default: 8)
    
    Returns job ID for tracking progress.
    """
    job_id = str(uuid4())
    
    jobs[job_id] = {
        "job_id": job_id,
        "status": "pending",
        "created_at": datetime.utcnow().isoformat(),
        "total_records": 0,
        "processed_records": 0,
        "failed_records": 0,
        "urls": request.urls,
        "max_workers": request.max_workers,
    }
    
    background_tasks.add_task(
        _run_import_job,
        job_id,
        request.urls,
        request.max_workers,
    )
    
    return JobResponse(
        job_id=job_id,
        status="pending",
        created_at=jobs[job_id]["created_at"],
        message=f"Import job created for {len(request.urls)} files"
    )


@router.post("/upload", response_model=JobResponse, summary="Upload and import Parquet file")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="Parquet file"),
    max_workers: int = Form(8, description="Parallel workers"),
):
    """
    Upload a Parquet file and import it using COPY.
    
    - **file**: Parquet file to upload
    - **max_workers**: Parallel workers (default: 8)
    """
    if not file.filename.endswith('.parquet'):
        raise HTTPException(400, "Only .parquet files are supported")
    
    # Save to temp location and get URL
    # For now, return not implemented
    raise HTTPException(501, "File upload not yet implemented - use /parquet with URLs")


@router.get("/jobs", summary="List all import jobs")
async def list_jobs(
    status: Optional[str] = None,
    limit: int = 100,
) -> Dict:
    """
    List import jobs with optional filtering.
    
    - **status**: Filter by status (pending|running|completed|failed)
    - **limit**: Maximum number of results
    """
    filtered_jobs = list(jobs.values())
    
    if status:
        filtered_jobs = [j for j in filtered_jobs if j["status"] == status]
    
    filtered_jobs = sorted(filtered_jobs, key=lambda x: x["created_at"], reverse=True)[:limit]
    
    return {
        "total": len(filtered_jobs),
        "jobs": filtered_jobs
    }


@router.get("/jobs/{job_id}", response_model=JobStatus, summary="Get job status")
async def get_job_status(job_id: str):
    """
    Get detailed status of an import job.
    
    - **job_id**: Job identifier
    """
    if job_id not in jobs:
        raise HTTPException(404, f"Job {job_id} not found")
    
    job = jobs[job_id]
    
    return JobStatus(
        job_id=job["job_id"],
        status=job["status"],
        created_at=job["created_at"],
        started_at=job.get("started_at"),
        completed_at=job.get("completed_at"),
        total_records=job.get("total_records", 0),
        processed_records=job.get("processed_records", 0),
        failed_records=job.get("failed_records", 0),
        elapsed_seconds=job.get("elapsed_seconds"),
        import_rate=job.get("import_rate"),
        error_message=job.get("error_message"),
    )


@router.delete("/jobs/{job_id}", summary="Cancel import job")
async def cancel_job(job_id: str):
    """
    Cancel a running import job.
    
    - **job_id**: Job identifier
    """
    if job_id not in jobs:
        raise HTTPException(404, f"Job {job_id} not found")
    
    if jobs[job_id]["status"] not in ["pending", "running"]:
        raise HTTPException(400, f"Job {job_id} is not running (status: {jobs[job_id]['status']})")
    
    jobs[job_id]["status"] = "cancelled"
    jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
    
    return {"job_id": job_id, "status": "cancelled", "message": "Job cancelled"}


@router.get("/stats", response_model=StatsResponse, summary="Import statistics")
async def get_stats():
    """
    Get overall import statistics.
    """
    total_jobs = len(jobs)
    completed_jobs = sum(1 for j in jobs.values() if j["status"] == "completed")
    failed_jobs = sum(1 for j in jobs.values() if j["status"] == "failed")
    running_jobs = sum(1 for j in jobs.values() if j["status"] in ["pending", "running"])
    
    total_records = sum(j.get("total_records", 0) for j in jobs.values() if j["status"] == "completed")
    
    rates = [j.get("import_rate", 0) for j in jobs.values() if j.get("import_rate")]
    avg_rate = sum(rates) / len(rates) if rates else 0
    
    return StatsResponse(
        total_jobs=total_jobs,
        completed_jobs=completed_jobs,
        failed_jobs=failed_jobs,
        running_jobs=running_jobs,
        total_records_imported=total_records,
        average_import_rate=avg_rate,
    )
