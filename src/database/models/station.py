"""
Modelli stazioni (Station, SamplingPoint).
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Station(Base):
    """Tabella stations - Stazioni di monitoraggio fisiche."""

    __tablename__ = "stations"
    __table_args__ = (
        Index("idx_stations_country", "country_code"),
        Index("idx_stations_coords", "latitude", "longitude"),
        {"schema": "airquality"},
    )

    station_code: Mapped[str] = mapped_column(String(50), primary_key=True)
    country_code: Mapped[Optional[str]] = mapped_column(String(2), ForeignKey("airquality.countries.country_code"))
    station_name: Mapped[Optional[str]] = mapped_column(String(200))
    station_type: Mapped[Optional[str]] = mapped_column(String(50))  # traffic/background/industrial
    area_type: Mapped[Optional[str]] = mapped_column(String(50))  # urban/suburban/rural
    latitude: Mapped[Optional[float]] = mapped_column(Float)
    longitude: Mapped[Optional[float]] = mapped_column(Float)
    altitude: Mapped[Optional[float]] = mapped_column(Float)
    municipality: Mapped[Optional[str]] = mapped_column(String(100))
    region: Mapped[Optional[str]] = mapped_column(String(100))
    start_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    end_date: Mapped[Optional[datetime]] = mapped_column(DateTime)
    metadata: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()")
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()")


class SamplingPoint(Base):
    """Tabella sampling_points - Sensori/strumenti alle stazioni."""

    __tablename__ = "sampling_points"
    __table_args__ = (
        Index("idx_sampling_points_station", "station_code"),
        Index("idx_sampling_points_country", "country_code"),
        Index("idx_sampling_points_pollutant", "pollutant_code"),
        {"schema": "airquality"},
    )

    sampling_point_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    station_code: Mapped[Optional[str]] = mapped_column(String(50), ForeignKey("airquality.stations.station_code"))
    country_code: Mapped[Optional[str]] = mapped_column(String(2), ForeignKey("airquality.countries.country_code"))
    instrument_type: Mapped[Optional[str]] = mapped_column(String(50))
    pollutant_code: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("airquality.pollutants.pollutant_code"))
    start_date: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    end_date: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    metadata: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()")
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()")
