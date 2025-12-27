"""
Repository per Station.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Station


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
