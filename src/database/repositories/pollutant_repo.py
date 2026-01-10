"""
Repository per Pollutant.
"""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Pollutant


class PollutantRepository:
    """Repository per operazioni su Pollutant."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_code(self, pollutant_code: int) -> Optional[Pollutant]:
        """Ottieni pollutant per codice."""
        result = await self.session.execute(
            select(Pollutant).where(Pollutant.pollutant_code == pollutant_code)
        )
        return result.scalar_one_or_none()

    async def get_all(self) -> list[Pollutant]:
        """Ottieni tutti i pollutants."""
        result = await self.session.execute(select(Pollutant))
        return list(result.scalars().all())

    async def create_or_update(self, data: dict) -> Pollutant:
        """Crea o aggiorna pollutant."""
        pollutant = await self.get_by_code(data["pollutant_code"])
        
        if pollutant:
            # Aggiorna esistente
            for key, value in data.items():
                if hasattr(pollutant, key):
                    setattr(pollutant, key, value)
        else:
            # Crea nuovo
            pollutant = Pollutant(**data)
            self.session.add(pollutant)
        
        await self.session.flush()
        return pollutant
