"""
Repository per Station e SamplingPoint.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import SamplingPoint, Station


class StationRepository:
    """Repository per operazioni su Station."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_code(self, station_code: str) -> Optional[Station]:
        """Ottieni stazione per codice."""
        result = await self.session.execute(
            select(Station).where(Station.station_code == station_code)
        )
        return result.scalar_one_or_none()

    async def create_or_update(self, data: dict) -> Station:
        """Crea o aggiorna stazione."""
        station = await self.get_by_code(data["station_code"])
        
        if station:
            # Aggiorna esistente
            for key, value in data.items():
                setattr(station, key, value)
            station.updated_at = datetime.utcnow()
        else:
            # Crea nuova
            station = Station(**data)
            self.session.add(station)
        
        await self.session.flush()
        return station


class SamplingPointRepository:
    """Repository per operazioni su SamplingPoint."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, sampling_point_id: str) -> Optional[SamplingPoint]:
        """Ottieni sampling point per ID."""
        result = await self.session.execute(
            select(SamplingPoint).where(SamplingPoint.sampling_point_id == sampling_point_id)
        )
        return result.scalar_one_or_none()

    async def create_or_update(self, data: dict) -> SamplingPoint:
        """Crea o aggiorna sampling point."""
        sp = await self.get_by_id(data["sampling_point_id"])
        
        if sp:
            # Aggiorna esistente
            for key, value in data.items():
                setattr(sp, key, value)
            sp.updated_at = datetime.utcnow()
        else:
            # Crea nuovo
            sp = SamplingPoint(**data)
            self.session.add(sp)
        
        await self.session.flush()
        return sp
