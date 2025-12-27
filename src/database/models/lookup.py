"""
Modelli lookup/dimensioni (Country, Pollutant, ValidityFlag, VerificationStatus).
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import Integer, String, Text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Country(Base):
    """Tabella countries - Codici ISO paesi."""

    __tablename__ = "countries"
    __table_args__ = {"schema": "airquality"}

    country_code: Mapped[str] = mapped_column(String(2), primary_key=True)
    country_name: Mapped[str] = mapped_column(String(100))
    region: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default="NOW()")


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


class ValidityFlag(Base):
    """Tabella validity_flags - Codici validità misurazioni."""

    __tablename__ = "validity_flags"
    __table_args__ = {"schema": "airquality"}

    validity_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    validity_name: Mapped[str] = mapped_column(String(50))
    description: Mapped[Optional[str]] = mapped_column(Text)


class VerificationStatus(Base):
    """Tabella verification_status - Stato verifica dati."""

    __tablename__ = "verification_status"
    __table_args__ = {"schema": "airquality"}

    verification_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    verification_name: Mapped[str] = mapped_column(String(50))
    description: Mapped[Optional[str]] = mapped_column(Text)
