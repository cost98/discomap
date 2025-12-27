"""
Modello Pollutant - Tabella pollutants (codici inquinanti EEA).
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, String, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Pollutant(Base):
    """Tabella pollutants - Codici inquinanti EEA (PM10, NO2, etc.)."""

    __tablename__ = "pollutants"
    __table_args__ = {"schema": "airquality"}

    pollutant_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    pollutant_name: Mapped[str] = mapped_column(String(20))
    pollutant_label: Mapped[Optional[str]] = mapped_column(String(100))
    unit: Mapped[Optional[str]] = mapped_column(String(20))
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()")
