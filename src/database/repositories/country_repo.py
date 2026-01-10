"""
Repository per Country.
"""

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Country


class CountryRepository:
    """Repository per operazioni su Country."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_code(self, country_code: str) -> Optional[Country]:
        """Ottieni country per codice."""
        result = await self.session.execute(
            select(Country).where(Country.country_code == country_code)
        )
        return result.scalar_one_or_none()

    async def get_all(self) -> list[Country]:
        """Ottieni tutti i countries."""
        result = await self.session.execute(select(Country))
        return list(result.scalars().all())
