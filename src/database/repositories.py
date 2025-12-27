"""
Repository pattern per operazioni database.

Interfacce semplificate per CRUD operations.
"""

from datetime import datetime
from typing import List, Optional, Sequence

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Country, Measurement, Pollutant, SamplingPoint, Station


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


class MeasurementRepository(BaseRepository):
    """Repository for Measurement entities (time-series data)."""

    async def create_many(self, measurements: List[dict]) -> int:
        """Bulk insert measurements (optimized for TimescaleDB)."""
        # Use bulk_insert_mappings for better performance
        if measurements:
            await self.session.execute(
                Measurement.__table__.insert(), measurements
            )
            await self.session.commit()
        return len(measurements)

    async def get_latest(
        self,
        sampling_point_id: str,
        limit: int = 100,
    ) -> Sequence[Measurement]:
        """Get latest measurements for a sampling point."""
        result = await self.session.execute(
            select(Measurement)
            .where(Measurement.sampling_point_id == sampling_point_id)
            .order_by(Measurement.time.desc())
            .limit(limit)
        ):
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