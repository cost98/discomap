"""
SQLAlchemy models for DiscoMap air quality database.

Modelli semplificati che mappano le tabelle PostgreSQL esistenti.
Schema definito in .docker/postgres/create-tables.sql
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all database models."""

    pass


class Country(Base):
    """Tabella countries - Codici ISO paesi."""

    __tablename__ = "countries"
    __table_args__ = {"schema": "airquality"}

    country_code: Mapped[str] = mapped_column(String(2), primary_key=True)
    country_name: Mapped[str] = mapped_column(String(100))
    region: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()")


class Pollutant(Base):
    """EEA pollutant codes and metadata."""

    __tablename__ = "pollutants"
    __table_args__ = {"schema": "airquality"}

    pollutant_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    pollutant_name: Mapped[str] = mapped_column(String(20), nullable=False)
    pollutant_label: Mapped[Optional[str]] = mapped_column(String(100))
    unit: Mapped[Optional[str]] = mapped_column(String(20))
    description: Mapped[Optional[str]] = mapped_column(Text)
    creTabella pollutants - Codici inquinanti EEA (PM10, NO2, etc.)."""

    __tablename__ = "pollutants"
    __table_args__ = {"schema": "airquality"}

    pollutant_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    pollutant_name: Mapped[str] = mapped_column(String(20))
    pollutant_label: Mapped[Optional[str]] = mapped_column(String(100))
    unit: Mapped[Optional[str]] = mapped_column(String(20))
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()"validity_name: Mapped[str] = mapped_column(String(50), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Relationships
    meaTabella validity_flags - Codici validità misurazioni."""

    __tablename__ = "validity_flags"
    __table_args__ = {"schema": "airquality"}

    validity_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    valTabella verification_status - Stato verifica dati."""

    __tablename__ = "verification_status"
    __table_args__ = {"schema": "airquality"}

    verification_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    verification_name: Mapped[str] = mapped_column(String(50))
    description: Mapped[Optional[str]] = mapped_column(Text
    __tablename__ = "stations"
    __table_args__ = (
        Index("idx_stations_country", "country_code"),
        Index("idx_stations_coords", "latitude", "longitude"),
        {"schema": "airquality"},
    )

    station_code: Mapped[str] = mapped_column(String(50), primary_key=True)
    couTabella stations - Stazioni di monitoraggio fisiche."""

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
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()"    Index("idx_sampling_points_pollutant", "pollutant_code"),
        {"schema": "airquality"},
    )

    sampling_point_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    station_code: Mapped[Optional[str]] = mapped_column(
        String(50), ForeignKey("airquality.stations.station_code")
    )
    country_code: Mapped[Optional[str]] = mapped_column(
        String(2), ForeignKey("airquality.countries.country_code")
    )
    instrument_type: Mapped[Optional[str]] = mapped_column(String(50))
    pollutant_code: Mapped[Optional[int]] = mapped_column(
       Tabella sampling_points - Sensori/strumenti alle stazioni."""

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
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()"    ),
        {"schema": "airquality"},
    )

    # TimescaleDB hypertable primary key: (time, sampling_point_id)
    time: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True, nullable=False
    )
    sampling_point_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey("airquality.sampling_points.sampling_point_id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    )
    pollutant_code: Mapped[int] = mapped_column(
        Integer, ForeignKey("airquality.pollutants.pollutant_code"), nullable=False
    )
    value: Mapped[Optional[float]] = mapped_column(Double)
    unit: Mapped[Optional[str]] = mapped_column(String(20))
    aggregation_type: Mapped[Optional[str]] = mapped_column(String(10))
    validity: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("airquality.validity_flags.validity_code")
    )
    verTabella measurements - Dati time-series misurazioni (TimescaleDB hypertable)."""

    __tablename__ = "measurements"
    __table_args__ = (
        Index("idx_measurements_sampling_point", "sampling_point_id", "time"),
        Index("idx_measurements_pollutant", "pollutant_code", "time"),
        {"schema": "airquality"},
    )

    # Primary key composita per TimescaleDB hypertable
    time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), primary_key=True)
    sampling_point_id: Mapped[str] = mapped_column(
        String(100),
        ForeignKey("airquality.sampling_points.sampling_point_id", ondelete="CASCADE"),
        primary_key=True,
    )
    
    # Dati misurazione
    pollutant_code: Mapped[int] = mapped_column(Integer, ForeignKey("airquality.pollutants.pollutant_code"))
    value: Mapped[Optional[float]] = mapped_column(Float)
    unit: Mapped[Optional[str]] = mapped_column(String(20))
    aggregation_type: Mapped[Optional[str]] = mapped_column(String(10))  # hour/day/etc
    
    # Quality flags
    validity: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("airquality.validity_flags.validity_code"))
    verification: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("airquality.verification_status.verification_code"))
    data_capture: Mapped[Optional[float]] = mapped_column(Float)
    
    # Metadata
    result_time: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    observation_id: Mapped[Optional[str]] = mapped_column(String(100)