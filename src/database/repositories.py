"""
Repository pattern for database operations.

Provides async CRUD operations for DiscoMap entities using SQLAlchemy.
"""

from datetime import datetime
from typing import List, Optional, Sequence

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.database.models import (
    Country,
    Measurement,
    Pollutant,
    SamplingPoint,
    Station,
    ValidityFlag,
    VerificationStatus,
)


class BaseRepository:
    """Base repository with common CRUD operations."""

    def __init__(self, session: AsyncSession):
        self.session = session


class StationRepository(BaseRepository):
    """Repository for Station entities."""

    async def get_by_code(self, station_code: str) -> Optional[Station]:
        """Get station by code."""
        result = await self.session.execute(
            select(Station)
            .where(Station.station_code == station_code)
            .options(selectinload(Station.sampling_points))
        )
        return result.scalar_one_or_none()

    async def get_all(
        self, country_code: Optional[str] = None, limit: int = 1000
    ) -> Sequence[Station]:
        """Get all stations, optionally filtered by country."""
        query = select(Station)
        if country_code:
            query = query.where(Station.country_code == country_code)
        query = query.limit(limit)

        result = await self.session.execute(query)
        return result.scalars().all()

    async def create_or_update(self, station_data: dict) -> Station:
        """Create new station or update existing."""
        existing = await self.get_by_code(station_data["station_code"])

        if existing:
            # Update existing
            await self.session.execute(
                update(Station)
                .where(Station.station_code == station_data["station_code"])
                .values(**station_data, updated_at=datetime.utcnow())
            )
            await self.session.commit()
            return await self.get_by_code(station_data["station_code"])
        else:
            # Create new
            station = Station(**station_data)
            self.session.add(station)
            await self.session.commit()
            await self.session.refresh(station)
            return station

    async def bulk_create_or_update(self, stations_data: List[dict]) -> int:
        """Bulk create or update stations."""
        count = 0
        for station_data in stations_data:
            await self.create_or_update(station_data)
            count += 1
        return count


class SamplingPointRepository(BaseRepository):
    """Repository for SamplingPoint entities."""

    async def get_by_id(self, sampling_point_id: str) -> Optional[SamplingPoint]:
        """Get sampling point by ID."""
        result = await self.session.execute(
            select(SamplingPoint)
            .where(SamplingPoint.sampling_point_id == sampling_point_id)
            .options(
                selectinload(SamplingPoint.station),
                selectinload(SamplingPoint.pollutant),
            )
        )
        return result.scalar_one_or_none()

    async def get_by_station(self, station_code: str) -> Sequence[SamplingPoint]:
        """Get all sampling points for a station."""
        result = await self.session.execute(
            select(SamplingPoint).where(SamplingPoint.station_code == station_code)
        )
        return result.scalars().all()

    async def create_or_update(self, sampling_point_data: dict) -> SamplingPoint:
        """Create new sampling point or update existing."""
        existing = await self.get_by_id(sampling_point_data["sampling_point_id"])

        if existing:
            # Update existing
            await self.session.execute(
                update(SamplingPoint)
                .where(
                    SamplingPoint.sampling_point_id
                    == sampling_point_data["sampling_point_id"]
                )
                .values(**sampling_point_data, updated_at=datetime.utcnow())
            )
            await self.session.commit()
            return await self.get_by_id(sampling_point_data["sampling_point_id"])
        else:
            # Create new
            sp = SamplingPoint(**sampling_point_data)
            self.session.add(sp)
            await self.session.commit()
            await self.session.refresh(sp)
            return sp

    async def bulk_create_or_update(self, sampling_points_data: List[dict]) -> int:
        """Bulk create or update sampling points."""
        count = 0
        for sp_data in sampling_points_data:
            await self.create_or_update(sp_data)
            count += 1
        return count


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
        )
        return result.scalars().all()

    async def get_time_range(
        self,
        sampling_point_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> Sequence[Measurement]:
        """Get measurements within time range."""
        result = await self.session.execute(
            select(Measurement)
            .where(
                Measurement.sampling_point_id == sampling_point_id,
                Measurement.time >= start_time,
                Measurement.time <= end_time,
            )
            .order_by(Measurement.time.desc())
        )
        return result.scalars().all()

    async def delete_time_range(
        self,
        sampling_point_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> int:
        """Delete measurements within time range (for re-sync)."""
        result = await self.session.execute(
            delete(Measurement).where(
                Measurement.sampling_point_id == sampling_point_id,
                Measurement.time >= start_time,
                Measurement.time <= end_time,
            )
        )
        await self.session.commit()
        return result.rowcount


class PollutantRepository(BaseRepository):
    """Repository for Pollutant entities."""

    async def get_by_code(self, pollutant_code: int) -> Optional[Pollutant]:
        """Get pollutant by code."""
        result = await self.session.execute(
            select(Pollutant).where(Pollutant.pollutant_code == pollutant_code)
        )
        return result.scalar_one_or_none()

    async def get_all(self) -> Sequence[Pollutant]:
        """Get all pollutants."""
        result = await self.session.execute(select(Pollutant))
        return result.scalars().all()


class CountryRepository(BaseRepository):
    """Repository for Country entities."""

    async def get_by_code(self, country_code: str) -> Optional[Country]:
        """Get country by code."""
        result = await self.session.execute(
            select(Country).where(Country.country_code == country_code)
        )
        return result.scalar_one_or_none()

    async def get_all(self) -> Sequence[Country]:
        """Get all countries."""
        result = await self.session.execute(select(Country))
        return result.scalars().all()
