"""TimescaleDB chunk compression endpoints."""

from typing import Optional
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import text

from src.database.engine import get_engine
from src.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(tags=["\U0001F527 Database Optimization"])


# Response models
class CompressionRequest(BaseModel):
    """Compression request."""
    older_than_days: int = 30
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "older_than_days": 30
            }
        }
    }


class CompressionResponse(BaseModel):
    """Compression response."""
    message: str
    status: str
    chunks_compressed: Optional[int] = None
    older_than_days: int
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "message": "Compression task started in background",
                "status": "queued",
                "chunks_compressed": None,
                "older_than_days": 30
            }
        }
    }


class CompressionStatusResponse(BaseModel):
    """Compression status response."""
    total_chunks: int
    compressed_chunks: int
    uncompressed_chunks: int
    compression_enabled: bool
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "total_chunks": 835,
                "compressed_chunks": 600,
                "uncompressed_chunks": 235,
                "compression_enabled": True
            }
        }
    }


@router.post("/compress", response_model=CompressionResponse)
async def compress_old_chunks(background_tasks: BackgroundTasks, request: CompressionRequest = CompressionRequest()):
    """
    Compress chunks older than specified days.
    
    Args:
        older_than_days: Compress chunks older than this many days (default: 30)
    
    This operation compresses historical data to save ~90% of disk space.
    Compressed chunks are read-only but queries still work normally.
    
    Examples:
        Default (30 days): POST /compression/compress
        Custom threshold: POST /compression/compress with body:
            {"older_than_days": 7}
    
    Returns:
        Confirmation message
    """
    try:
        # Start compression in background
        background_tasks.add_task(compress_chunks_background, request.older_than_days)
        
        return CompressionResponse(
            message=f"Compression task started for chunks older than {request.older_than_days} days",
            status="queued",
            older_than_days=request.older_than_days
        )
        
    except Exception as e:
        logger.error(f"Error starting compression: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", response_model=CompressionStatusResponse)
async def get_compression_status():
    """
    Get compression status for the measurements table.
    
    Returns chunk compression statistics and whether compression is enabled.
    """
    try:
        engine = get_engine()
        
        async with engine.connect() as conn:
            # Check if compression is enabled
            compression_check = await conn.execute(text("""
                SELECT compression_enabled 
                FROM timescaledb_information.hypertables 
                WHERE hypertable_name = 'measurements'
            """))
            compression_enabled = compression_check.scalar()
            
            # Get chunk statistics
            result = await conn.execute(text("""
                SELECT 
                    COUNT(*) as total_chunks,
                    COUNT(*) FILTER (WHERE is_compressed) as compressed_chunks,
                    COUNT(*) FILTER (WHERE NOT is_compressed) as uncompressed_chunks
                FROM timescaledb_information.chunks
                WHERE hypertable_name = 'measurements'
            """))
            row = result.fetchone()
            
            return CompressionStatusResponse(
                total_chunks=row[0] or 0,
                compressed_chunks=row[1] or 0,
                uncompressed_chunks=row[2] or 0,
                compression_enabled=bool(compression_enabled)
            )
            
    except Exception as e:
        logger.error(f"Error getting compression status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def compress_chunks_background(older_than_days: int):
    """Background task to compress old chunks."""
    try:
        logger.info(f"🗜️ Starting chunk compression for data older than {older_than_days} days...")
        
        engine = get_engine()
        
        # First, check if compression is enabled
        async with engine.connect() as conn:
            compression_check = await conn.execute(text("""
                SELECT compression_enabled 
                FROM timescaledb_information.hypertables 
                WHERE hypertable_name = 'measurements'
            """))
            compression_enabled = compression_check.scalar()
            
            if not compression_enabled:
                logger.error("❌ Compression is not enabled on measurements table. Enable it first using POST /compression/enable")
                return
        
        # Get list of chunks to compress
        async with engine.begin() as conn:
            result = await conn.execute(text(f"""
                SELECT i::text as chunk_name
                FROM show_chunks('airquality.measurements', older_than => INTERVAL '{older_than_days} days') i
            """))
            chunk_names = [row[0] for row in result.fetchall()]
        
        total_chunks = len(chunk_names)
        logger.info(f"📦 Found {total_chunks} chunks to compress")
        
        if total_chunks == 0:
            logger.info("ℹ️ No chunks found to compress")
            return
        
        # Compress chunks one by one to show progress
        compressed_count = 0
        for idx, chunk_name in enumerate(chunk_names, 1):
            try:
                async with engine.begin() as conn:
                    await conn.execute(text(f"""
                        SELECT compress_chunk('{chunk_name}', if_not_compressed => true)
                    """))
                compressed_count += 1
                
                # Log progress every 10 chunks
                if idx % 10 == 0 or idx == total_chunks:
                    logger.info(f"🗜️ Progress: {idx}/{total_chunks} chunks processed ({compressed_count} compressed)")
                    
            except Exception as chunk_error:
                logger.warning(f"⚠️ Failed to compress chunk {chunk_name}: {chunk_error}")
                continue
        
        logger.info(f"✅ Compression completed: {compressed_count}/{total_chunks} chunks compressed")
    except Exception as e:
        logger.error(f"❌ Error compressing chunks: {e}")
