"""
Database engine and session management for DiscoMap.

Provides async SQLAlchemy engine, session factory, and connection utilities.
Uses AsyncPG driver for optimal PostgreSQL performance.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from src.config import Config
from src.database.models import Base

logger = logging.getLogger(__name__)

# Global engine and session factory
_engine: Optional[AsyncEngine] = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


def get_database_url() -> str:
    """
    Build async PostgreSQL connection URL.

    Returns:
        PostgreSQL URL for asyncpg driver
    """
    config = Config()
    return (
        f"postgresql+asyncpg://{config.DB_USER}:{config.DB_PASSWORD}"
        f"@{config.DB_HOST}:{config.DB_PORT}/{config.DB_NAME}"
    )


def get_engine(
    url: Optional[str] = None,
    echo: bool = False,
    pool_size: int = 5,
    max_overflow: int = 10,
    pool_pre_ping: bool = True,
) -> AsyncEngine:
    """
    Get or create async SQLAlchemy engine.

    Args:
        url: Database URL (defaults to config)
        echo: Log all SQL statements
        pool_size: Number of connections in pool
        max_overflow: Max overflow connections
        pool_pre_ping: Test connections before use

    Returns:
        AsyncEngine instance
    """
    global _engine

    if _engine is None:
        db_url = url or get_database_url()
        logger.info(f"Creating async database engine: {db_url.split('@')[1]}")

        _engine = create_async_engine(
            db_url,
            echo=echo,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_pre_ping=pool_pre_ping,
            # Use NullPool for serverless/short-lived connections
            # poolclass=NullPool,
        )

    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """
    Get or create async session factory.

    Returns:
        Async session maker bound to engine
    """
    global _session_factory

    if _session_factory is None:
        engine = get_engine()
        _session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )

    return _session_factory


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Get async database session (dependency injection).

    Usage:
        async with get_session() as session:
            result = await session.execute(select(Station))

    Yields:
        AsyncSession instance
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager for database sessions.

    Usage:
        async with get_db_session() as session:
            await session.execute(...)

    Yields:
        AsyncSession instance
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db(drop_all: bool = False) -> None:
    """
    Initialize database schema.

    WARNING: This will create all tables. Use Alembic migrations in production.

    Args:
        drop_all: Drop all existing tables first (DANGEROUS!)
    """
    engine = get_engine()

    async with engine.begin() as conn:
        if drop_all:
            logger.warning("Dropping all database tables!")
            await conn.run_sync(Base.metadata.drop_all)

        logger.info("Creating database tables...")
        await conn.run_sync(Base.metadata.create_all)

    logger.info("Database initialization complete")


async def close_db() -> None:
    """Close database engine and cleanup connections."""
    global _engine, _session_factory

    if _engine:
        logger.info("Closing database engine")
        await _engine.dispose()
        _engine = None
        _session_factory = None
