"""
DiscoMap API Server

REST API for managing sync operations and monitoring air quality data.
Can be integrated with Grafana dashboards for interactive control.
"""

import asyncio
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import pandas as pd
import io

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import Config
from src.db_writer import PostgreSQLWriter
from src.logger import get_logger
from src.sync_scheduler import SyncScheduler

logger = get_logger(__name__)

# Global sync state
active_syncs: Dict[str, dict] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    logger.info("🚀 API Server starting...")
    yield
    logger.info("🛑 API Server shutting down...")


app = FastAPI(
    title="DiscoMap Sync API",
    description="REST API for managing EEA air quality data synchronization",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS for Grafana
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify Grafana URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# === Models ===


class SyncRequest(BaseModel):
    """Request model for starting a sync."""

    sync_type: str = Field(..., description="Type: 'incremental', 'hourly', 'custom', 'from_urls'")
    countries: Optional[List[str]] = Field(
        default=["IT"], description="Country codes"
    )
    pollutants: Optional[List[str]] = Field(
        default=["NO2", "PM10", "PM2.5", "O3"], description="Pollutant codes"
    )
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD) for custom sync")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD) for custom sync")
    days: Optional[int] = Field(None, description="Number of days to sync (alternative to dates)")
    dataset: Optional[int] = Field(None, description="Force dataset: 1=E2a (2025+), 2=E1a (2013-2024), 3=Airbase (2002-2012). Auto-detects if not specified.")
    aggregation_type: Optional[str] = Field("hour", description="Data aggregation: 'hour' (hourly), 'day' (daily), 'var' (variable)")
    use_urls: bool = Field(False, description="Use URL-based download (more stable for large requests)")
    parquet_urls: Optional[List[str]] = Field(None, description="Direct list of Parquet URLs (for from_urls sync_type)")
    max_workers: int = Field(8, description="Parallel download workers")


class SyncResponse(BaseModel):
    """Response model for sync operations."""

    sync_id: str
    status: str
    message: str
    operation_id: Optional[int] = None


class SyncStatus(BaseModel):
    """Status of a sync operation."""

    sync_id: str
    operation_id: Optional[int]
    status: str
    sync_type: str
    started_at: str
    duration_seconds: Optional[float]
    records_downloaded: int
    records_inserted: int
    error: Optional[str]


class OperationSummary(BaseModel):
    """Summary of sync operations."""

    operation_id: int
    operation_type: str
    country_code: Optional[str]
    pollutant_code: Optional[int]
    start_time: str
    end_time: Optional[str]
    status: str
    records_downloaded: int
    records_inserted: int
    duration_seconds: Optional[float]
    error_message: Optional[str]


# === Background Task Functions ===


def run_sync_task(
    sync_id: str,
    sync_type: str,
    countries: List[str],
    pollutants: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: Optional[int] = None,
    dataset: Optional[int] = None,
    aggregation_type: str = "hour",
    use_urls: bool = False,
    parquet_urls: Optional[List[str]] = None,
    max_workers: int = 8,
):
    """Run sync in background."""
    try:
        logger.info(f"🔄 Starting background sync: {sync_id} (type: {sync_type})")

        # Update state
        active_syncs[sync_id]["status"] = "running"
        active_syncs[sync_id]["started_at"] = datetime.now().isoformat()

        # Initialize scheduler
        scheduler = SyncScheduler(test_mode=False)
        scheduler.config["countries"] = countries
        scheduler.config["pollutants"] = pollutants
        scheduler.config["aggregation"] = aggregation_type
        scheduler.config["use_urls"] = use_urls
        scheduler.config["max_workers"] = max_workers
        if dataset:
            scheduler.config["dataset"] = dataset
            from src.config import DATASET_NAMES
            logger.info(f"📦 Forcing dataset: {DATASET_NAMES.get(dataset, f'Dataset {dataset}')}")

        # Run appropriate sync - always use URL-based method
        success = False

        # Calculate date range based on sync type
        if sync_type == "incremental":
            # Incremental sync: last N days (default 7)
            if not days:
                days = 7
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days)
            start_date = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_date = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            success = scheduler.sync_custom_period_urls(
                start_date, end_date, max_workers=max_workers
            )
        elif sync_type == "hourly":
            # Hourly sync: last N hours (default 24)
            if not days:
                days = 1
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(hours=days * 24)
            start_date = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_date = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            success = scheduler.sync_custom_period_urls(
                start_date, end_date, max_workers=max_workers
            )
        elif sync_type == "custom":
            # Calculate dates
            if days:
                end_dt = (
                    datetime.now()
                    if not end_date
                    else datetime.fromisoformat(end_date.replace("Z", ""))
                )
                start_dt = end_dt - timedelta(days=days)
                start_date = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                end_date = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            if not start_date or not end_date:
                raise ValueError("Custom sync requires start_date and end_date")

            # Use sync_custom_period (auto-detects or uses forced dataset)
            success = scheduler.sync_custom_period(start_date, end_date)
        elif sync_type == "from_urls":
            # Direct URL sync - bypass API
            if not parquet_urls:
                raise ValueError("from_urls sync requires parquet_urls list")
            success = scheduler.sync_from_urls(parquet_urls, max_workers=max_workers)
        else:
            raise ValueError(f"Unknown sync type: {sync_type}")

        # Update state
        active_syncs[sync_id]["status"] = "completed" if success else "failed"
        active_syncs[sync_id]["completed_at"] = datetime.now().isoformat()
        active_syncs[sync_id]["success"] = success

        logger.info(
            f"✅ Background sync completed: {sync_id} (success: {success})"
        )

    except Exception as e:
        logger.error(f"❌ Background sync failed: {sync_id} - {e}", exc_info=True)
        active_syncs[sync_id]["status"] = "failed"
        active_syncs[sync_id]["error"] = str(e)
        active_syncs[sync_id]["completed_at"] = datetime.now().isoformat()


# === API Endpoints ===


@app.get("/")
async def root():
    """API root endpoint."""
    return {
        "name": "DiscoMap API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "start_sync": "POST /sync/start",
            "sync_status": "GET /sync/status/{sync_id}",
            "import_stations": "POST /stations/import",
            "database_stats": "GET /stats",
            "operations_history": "GET /operations/history",
        },
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


# === Sync Operations ===


@app.post("/sync/get-urls")
async def get_parquet_urls(
    countries: str = Query("IT", description="Comma-separated country codes"),
    pollutants: str = Query("PM10", description="Comma-separated pollutant codes"),
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    dataset: int = Query(2, description="Dataset: 1=E2a, 2=E1a, 3=Airbase"),
):
    """Get list of Parquet URLs from EEA API without downloading."""
    try:
        from src.downloader import EEADownloader
        downloader = EEADownloader()
        
        country_list = [c.strip() for c in countries.split(",")]
        pollutant_list = [p.strip() for p in pollutants.split(",")]
        
        urls = downloader.get_parquet_urls(
            countries=country_list,
            pollutants=pollutant_list,
            start_date=start_date,
            end_date=end_date,
            dataset=dataset
        )
        
        return {"urls": urls, "count": len(urls)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sync/start", response_model=SyncResponse)
async def start_sync(request: SyncRequest, background_tasks: BackgroundTasks):
    """
    Start a new sync operation.
    
    Returns immediately with sync_id for tracking.
    Actual sync runs in background.
    
    Example:
    ```bash
    curl -X POST "http://localhost:8000/sync/start" \\
      -H "Content-Type: application/json" \\
      -d '{
        "sync_type": "incremental",
        "countries": ["IT"],
        "pollutants": ["PM10", "PM2.5"],
        "days": 7
      }'
    ```
    """
    # Generate sync ID
    sync_id = f"{request.sync_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Validate request
    if request.sync_type == "custom":
        if not request.start_date or not request.end_date:
            if not request.days:
                raise HTTPException(
                    status_code=400,
                    detail="Custom sync requires start_date+end_date or days parameter",
                )

    # Initialize state
    active_syncs[sync_id] = {
        "sync_type": request.sync_type,
        "status": "queued",
        "countries": request.countries,
        "pollutants": request.pollutants,
        "created_at": datetime.now().isoformat(),
    }

    # Queue background task
    background_tasks.add_task(
        run_sync_task,
        sync_id=sync_id,
        sync_type=request.sync_type,
        countries=request.countries,
        pollutants=request.pollutants,
        start_date=request.start_date,
        end_date=request.end_date,
        days=request.days,
        dataset=request.dataset,
        aggregation_type=request.aggregation_type,
        use_urls=request.use_urls,
        parquet_urls=request.parquet_urls,
        max_workers=request.max_workers,
    )

    logger.info(f"🆕 Queued sync: {sync_id}")

    return SyncResponse(
        sync_id=sync_id, status="queued", message="Sync queued successfully"
    )


@app.get("/sync/status/{sync_id}", response_model=SyncStatus)
async def get_sync_status(sync_id: str):
    """
    Get status of a specific sync operation.
    
    Example:
    ```bash
    curl "http://localhost:8000/sync/status/incremental_20251127_213000"
    ```
    """
    if sync_id not in active_syncs:
        raise HTTPException(status_code=404, detail=f"Sync ID not found: {sync_id}")

    sync_data = active_syncs[sync_id]

    # Calculate duration
    duration = None
    if "started_at" in sync_data:
        start = datetime.fromisoformat(sync_data["started_at"])
        end = (
            datetime.fromisoformat(sync_data["completed_at"])
            if "completed_at" in sync_data
            else datetime.now()
        )
        duration = (end - start).total_seconds()

    return SyncStatus(
        sync_id=sync_id,
        operation_id=sync_data.get("operation_id"),
        status=sync_data["status"],
        sync_type=sync_data["sync_type"],
        started_at=sync_data.get("started_at", sync_data["created_at"]),
        duration_seconds=duration,
        records_downloaded=sync_data.get("records_downloaded", 0),
        records_inserted=sync_data.get("records_inserted", 0),
        error=sync_data.get("error"),
    )


# === Operations History ===


@app.get("/operations/history")
async def get_operations_history(
    limit: int = Query(50, description="Number of operations to return"),
    status: Optional[str] = Query(None, description="Filter by status"),
):
    """
    Get recent sync operations from database.
    
    Example:
    ```
    curl "http://localhost:8000/operations/history?limit=10&status=completed"
    ```
    """
    try:
        db = PostgreSQLWriter()

        query = """
            SELECT 
                operation_id,
                operation_type,
                start_time,
                end_time,
                status,
                records_downloaded,
                records_inserted,
                EXTRACT(EPOCH FROM (COALESCE(end_time, NOW()) - start_time)) as duration_seconds,
                error_message,
                metadata
            FROM airquality.sync_operations
            WHERE 1=1
        """

        params = []
        if status:
            query += " AND status = %s"
            params.append(status)

        query += " ORDER BY start_time DESC LIMIT %s"
        params.append(limit)

        results = db.execute_query(query, tuple(params))
        db.close_all()

        operations = []
        for row in results:
            operations.append({
                "operation_id": row[0],
                "operation_type": row[1],
                "start_time": row[2].isoformat() if row[2] else None,
                "end_time": row[3].isoformat() if row[3] else None,
                "status": row[4],
                "records_downloaded": row[5] or 0,
                "records_inserted": row[6] or 0,
                "duration_seconds": float(row[7]) if row[7] else None,
                "error_message": row[8],
                "metadata": row[9],
            })

        return {"count": len(operations), "operations": operations}

    except Exception as e:
        logger.error(f"Failed to fetch operations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# === Database Statistics ===


@app.get("/stats")
async def get_database_stats():
    """
    Get database statistics.
    
    Example:
    ```
    curl "http://localhost:8000/stats"
    ```
    """
    try:
        db = PostgreSQLWriter()

        # Measurements stats
        measurements_query = """
            SELECT 
                COUNT(*) as total_measurements,
                COUNT(DISTINCT sampling_point_id) as total_stations,
                MIN(time) as oldest_measurement,
                MAX(time) as newest_measurement,
                COUNT(DISTINCT pollutant_code) as pollutants_count
            FROM airquality.measurements
        """

        # Operations stats
        operations_query = """
            SELECT 
                COUNT(*) as total_operations,
                COUNT(*) FILTER (WHERE status = 'completed') as completed,
                COUNT(*) FILTER (WHERE status = 'running') as running,
                COUNT(*) FILTER (WHERE status = 'failed') as failed,
                MAX(start_time) as last_sync
            FROM airquality.sync_operations
        """

        measurements_result = db.execute_query(measurements_query)
        operations_result = db.execute_query(operations_query)
        db.close_all()

        m_row = measurements_result[0]
        o_row = operations_result[0]

        return {
            "measurements": {
                "total": m_row[0],
                "stations": m_row[1],
                "oldest": m_row[2].isoformat() if m_row[2] else None,
                "newest": m_row[3].isoformat() if m_row[3] else None,
                "pollutants": m_row[4],
            },
            "operations": {
                "total": o_row[0],
                "completed": o_row[1],
                "running": o_row[2],
                "failed": o_row[3],
                "last_sync": o_row[4].isoformat() if o_row[4] else None,
            },
        }

    except Exception as e:
        logger.error(f"Failed to fetch stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# === Station Metadata Import ===


class StationMetadataImportResult(BaseModel):
    """Result of station metadata import."""

    success: bool
    stations_processed: int
    stations_inserted: int
    stations_updated: int
    errors: List[str] = []


@app.post("/stations/import", response_model=StationMetadataImportResult)
async def import_station_metadata(
    file: UploadFile = File(..., description="CSV file with station metadata")
):
    """
    Import station metadata from CSV file.
    
    Expected CSV columns:
    - sampling_point_id (required)
    - station_name
    - station_type (e.g., traffic, background, industrial)
    - area_type (e.g., urban, suburban, rural)
    - latitude
    - longitude
    - altitude
    - country_code
    
    Example:
    ```
    curl -X POST "http://localhost:8000/stations/import" \\
         -F "file=@stations.csv"
    ```
    """
    try:
        # Validate file type
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV")
        
        # Read CSV
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        logger.info(f"📥 Importing station metadata from {file.filename}")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"Rows: {len(df)}")
        
        # Validate required column
        if 'sampling_point_id' not in df.columns:
            raise HTTPException(
                status_code=400,
                detail="CSV must contain 'sampling_point_id' column"
            )
        
        # Initialize database writer
        db_writer = PostgreSQLWriter()
        
        # Track results
        stations_processed = 0
        stations_inserted = 0
        stations_updated = 0
        errors = []
        
        # Process each station
        for idx, row in df.iterrows():
            try:
                sampling_point_id = str(row['sampling_point_id'])
                
                # Build update query dynamically based on available columns
                update_fields = []
                params = [sampling_point_id]
                param_idx = 2
                
                column_mapping = {
                    'station_name': 'station_name',
                    'station_type': 'station_type',
                    'area_type': 'area_type',
                    'latitude': 'latitude',
                    'longitude': 'longitude',
                    'altitude': 'altitude',
                    'country_code': 'country_code',
                }
                
                for csv_col, db_col in column_mapping.items():
                    if csv_col in df.columns and pd.notna(row[csv_col]):
                        update_fields.append(f"{db_col} = ${param_idx}")
                        params.append(row[csv_col])
                        param_idx += 1
                
                if not update_fields:
                    continue  # Skip if no fields to update
                
                # Execute upsert
                conn = db_writer.get_connection()
                cursor = conn.cursor()
                
                query = f"""
                    INSERT INTO airquality.sampling_points 
                    (sampling_point_id, {', '.join([k for k in column_mapping.values() if k in ' '.join(update_fields)])})
                    VALUES ($1, {', '.join([f'${i}' for i in range(2, param_idx)])})
                    ON CONFLICT (sampling_point_id) 
                    DO UPDATE SET 
                        {', '.join(update_fields)},
                        updated_at = NOW()
                """
                
                cursor.execute(query, params)
                
                if cursor.rowcount == 1:
                    stations_inserted += 1
                else:
                    stations_updated += 1
                
                stations_processed += 1
                
                conn.commit()
                cursor.close()
                
            except Exception as e:
                error_msg = f"Row {idx}: {str(e)}"
                errors.append(error_msg)
                logger.warning(error_msg)
        
        db_writer.close()
        
        logger.info(
            f"✅ Import complete: {stations_processed} processed, "
            f"{stations_inserted} inserted, {stations_updated} updated, "
            f"{len(errors)} errors"
        )
        
        return StationMetadataImportResult(
            success=len(errors) == 0,
            stations_processed=stations_processed,
            stations_inserted=stations_inserted,
            stations_updated=stations_updated,
            errors=errors[:10]  # Limit to first 10 errors
        )
        
    except pd.errors.EmptyDataError:
        raise HTTPException(status_code=400, detail="CSV file is empty")
    except Exception as e:
        logger.error(f"Failed to import station metadata: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("API_PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
