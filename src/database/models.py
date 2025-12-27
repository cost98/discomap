"""
SQLAlchemy models for DiscoMap air quality database.

Uses SQLAlchemy 2.0+ declarative mapping with type hints.
Corresponds to schema in .docker/postgres/create-tables.sql
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    JSON,
    Column,
    Date,
    DateTime,
    Double,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all database models."""

    pass


class Country(Base):
    """ISO country codes and names."""

    __tablename__ = "countries"
    __table_args__ = {"schema": "airquality"}

    country_code: Mapped[str] = mapped_column(String(2), primary_key=True)
    country_name: Mapped[str] = mapped_column(String(100), nullable=False)
    region: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default="NOW()"
    )

    # Relationships
    stations: Mapped[list["Station"]] = relationship(
        "Station", back_populates="country", lazy="selectin"
    )
    sampling_points: Mapped[list["SamplingPoint"]] = relationship(
        "SamplingPoint", back_populates="country", lazy="selectin"
    )


class Pollutant(Base):
    """EEA pollutant codes and metadata."""

    __tablename__ = "pollutants"
    __table_args__ = {"schema": "airquality"}

    pollutant_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    pollutant_name: Mapped[str] = mapped_column(String(20), nullable=False)
    pollutant_label: Mapped[Optional[str]] = mapped_column(String(100))
    unit: Mapped[Optional[str]] = mapped_column(String(20))
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default="NOW()"
    )

    # Relationships
    sampling_points: Mapped[list["SamplingPoint"]] = relationship(
        "SamplingPoint", back_populates="pollutant", lazy="selectin"
    )
    measurements: Mapped[list["Measurement"]] = relationship(
        "Measurement", back_populates="pollutant", lazy="selectin"
    )


class ValidityFlag(Base):
    """Measurement validity flags lookup."""

    __tablename__ = "validity_flags"
    __table_args__ = {"schema": "airquality"}

    validity_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    validity_name: Mapped[str] = mapped_column(String(50), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Relationships
    measurements: Mapped[list["Measurement"]] = relationship(
        "Measurement", back_populates="validity_flag", lazy="selectin"
    )


class VerificationStatus(Base):
    """Measurement verification status lookup."""

    __tablename__ = "verification_status"
    __table_args__ = {"schema": "airquality"}

    verification_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    verification_name: Mapped[str] = mapped_column(String(50), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Relationships
    measurements: Mapped[list["Measurement"]] = relationship(
        "Measurement", back_populates="verification_status", lazy="selectin"
    )


class Station(Base):
    """Physical monitoring stations (unique locations)."""

    __tablename__ = "stations"
    __table_args__ = (
        Index("idx_stations_country", "country_code"),
        Index("idx_stations_coords", "latitude", "longitude"),
        {"schema": "airquality"},
    )

    station_code: Mapped[str] = mapped_column(String(50), primary_key=True)
    country_code: Mapped[Optional[str]] = mapped_column(
        String(2), ForeignKey("airquality.countries.country_code")
    )
    station_name: Mapped[Optional[str]] = mapped_column(String(200))
    station_type: Mapped[Optional[str]] = mapped_column(String(50))
    area_type: Mapped[Optional[str]] = mapped_column(String(50))
    latitude: Mapped[Optional[float]] = mapped_column(Double)
    longitude: Mapped[Optional[float]] = mapped_column(Double)
    altitude: Mapped[Optional[float]] = mapped_column(Float)
    municipality: Mapped[Optional[str]] = mapped_column(String(100))
    region: Mapped[Optional[str]] = mapped_column(String(100))
    start_date: Mapped[Optional[datetime]] = mapped_column(Date)
    end_date: Mapped[Optional[datetime]] = mapped_column(Date)
    metadata: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default="NOW()"
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default="NOW()", onupdate=datetime.utcnow
    )

    # Relationships
    country: Mapped[Optional["Country"]] = relationship(
        "Country", back_populates="stations"
    )
    sampling_points: Mapped[list["SamplingPoint"]] = relationship(
        "SamplingPoint", back_populates="station", lazy="selectin"
    )


class SamplingPoint(Base):
    """Individual sensors/instruments at monitoring stations."""

    __tablename__ = "sampling_points"
    __table_args__ = (
        Index("idx_sampling_points_station", "station_code"),
        Index("idx_sampling_points_country", "country_code"),
        Index("idx_sampling_points_pollutant", "pollutant_code"),
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
        Integer, ForeignKey("airquality.pollutants.pollutant_code")
    )
    start_date: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    end_date: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    metadata: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default="NOW()"
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default="NOW()", onupdate=datetime.utcnow
    )

    # Relationships
    station: Mapped[Optional["Station"]] = relationship(
        "Station", back_populates="sampling_points"
    )
    country: Mapped[Optional["Country"]] = relationship(
        "Country", back_populates="sampling_points"
    )
    pollutant: Mapped[Optional["Pollutant"]] = relationship(
        "Pollutant", back_populates="sampling_points"
    )
    measurements: Mapped[list["Measurement"]] = relationship(
        "Measurement", back_populates="sampling_point", lazy="selectin"
    )


class Measurement(Base):
    """Air quality measurements time-series data (TimescaleDB hypertable)."""

    __tablename__ = "measurements"
    __table_args__ = (
        Index(
            "idx_measurements_sampling_point",
            "sampling_point_id",
            "time",
            postgresql_ops={"time": "DESC"},
        ),
        Index(
            "idx_measurements_pollutant",
            "pollutant_code",
            "time",
            postgresql_ops={"time": "DESC"},
        ),
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
    verification: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("airquality.verification_status.verification_code")
    )
    data_capture: Mapped[Optional[float]] = mapped_column(Float)
    result_time: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True))
    observation_id: Mapped[Optional[str]] = mapped_column(String(100))

    # Relationships
    sampling_point: Mapped["SamplingPoint"] = relationship(
        "SamplingPoint", back_populates="measurements"
    )
    pollutant: Mapped["Pollutant"] = relationship(
        "Pollutant", back_populates="measurements"
    )
    validity_flag: Mapped[Optional["ValidityFlag"]] = relationship(
        "ValidityFlag", back_populates="measurements"
    )
    verification_status: Mapped[Optional["VerificationStatus"]] = relationship(
        "VerificationStatus", back_populates="measurements"
    )
