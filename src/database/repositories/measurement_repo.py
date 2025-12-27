"""
Repository per Measurement (time-series).
"""

from datetime import datetime
from typing import List, Sequence

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Measurement


class MeasurementRepository:
    """Repository per operazioni su Measurement (time-series)."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def bulk_insert(self, measurements: List[dict]) -> int:
        """
        Inserimento bulk di misurazioni (ottimizzato per TimescaleDB).
        
        Uso:
            measurements = [
                {"time": datetime(...), "sampling_point_id": "...", "value": 25.5, ...},
                ...
            ]
            count = await repo.bulk_insert(measurements)
        """
        if not measurements:
            return 0
        
        # Usa bulk insert di SQLAlchemy
        await self.session.execute(
            Measurement.__table__.insert(),
            measurements
        )
        await self.session.flush()
        return len(measurements)

    async def get_latest(self, sampling_point_id: str, limit: int = 100) -> Sequence[Measurement]:
        """Ottieni ultime N misurazioni per sampling point."""
        result = await self.session.execute(
            select(Measurement)
            .where(Measurement.sampling_point_id == sampling_point_id)
            .order_by(Measurement.time.desc())
            .limit(limit)
        )
        return result.scalars().all()

    async def delete_time_range(
        self,
        sampling_point_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> int:
        """Elimina misurazioni in un range temporale (per re-sync)."""
        result = await self.session.execute(
            delete(Measurement).where(
                Measurement.sampling_point_id == sampling_point_id,
                Measurement.time >= start_time,
                Measurement.time <= end_time,
            )
        )
        await self.session.flush()
        return result.rowcount
