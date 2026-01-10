"""Pydantic models for ETL API responses."""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class BatchJobResponse(BaseModel):
    """Response model for a single batch job."""
    job_id: str
    status: str
    urls_count: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    succeeded: int = 0
    failed: int = 0
    error: Optional[str] = None
    duration_seconds: Optional[float] = None


class ProgressResponse(BaseModel):
    """Progress statistics for a master job."""
    batches_completed: int
    batches_failed: int
    batches_running: int
    batches_pending: int
    urls_succeeded: int
    urls_failed: int
    completion_pct: float


class MasterJobResponse(BaseModel):
    """Response model for a master job."""
    master_job_id: str
    status: str
    total_urls: int
    total_batches: int
    batch_size: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: ProgressResponse
    batches: Optional[List[BatchJobResponse]] = None


class FileUploadResponse(BaseModel):
    """Response after file upload."""
    master_job_id: str
    message: str
    total_urls: int
    total_batches: int
    estimated_duration_minutes: float


class JobListResponse(BaseModel):
    """Response for job list endpoint."""
    jobs: List[MasterJobResponse]
    total: int
