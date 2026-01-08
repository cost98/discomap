"""
Database engine e session management.

Gestione semplificata delle connessioni async a PostgreSQL.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from src.config import Config
from src.database.models import Base

logger = logging.getLogger(__name__)

# Engine globale (singleton)
_engine: AsyncEngine = None
_session_factory: async_sessionmaker[AsyncSession] = None


def get_engine() -> AsyncEngine:
    """Ottieni o crea engine async PostgreSQL."""
    global _engine
    
    if _engine is None:
        from src.config import settings
        
        logger.info(f"Creazione engine database: {settings.database_url.split('@')[1]}")
        _engine = create_async_engine(
            settings.database_url,
            echo=False,  # Metti True per debug SQL
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,  # Verifica connessioni prima dell'uso
        )
    
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Ottieni factory per creare sessioni async."""
    global _session_factory
    
    if _session_factory is None:
        engine = get_engine()
        _session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    
    return _session_factory


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager per sessioni database.
    
    Uso:
        async with get_db_session() as session:
            result = await session.execute(select(Station))
            await session.commit()
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        else:
            await session.commit()


async def close_db() -> None:
    """Chiudi engine e cleanup connessioni."""
    global _engine, _session_factory
    
    if _engine:
        logger.info("Chiusura database engine")
        await _engine.dispose()
        _engine = None
        _session_factory = None
