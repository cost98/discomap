"""
Modello Country - Tabella countries (codici ISO paesi).
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import String
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
