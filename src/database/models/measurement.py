"""
Modello misurazioni time-series (Measurement).
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Float, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Measurement(Base):
    """Tabella measurements - Dati time-series misurazioni (TimescaleDB hypertable)."""

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
    observation_id: Mapped[Optional[str]] = mapped_column(String(100))
