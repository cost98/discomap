"""
Modello ValidityFlag - Tabella validity_flags (codici validità misurazioni).
"""

from typing import Optional

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ValidityFlag(Base):
    """Tabella validity_flags - Codici validità misurazioni."""

    __tablename__ = "validity_flags"
    __table_args__ = {"schema": "airquality"}

    validity_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    validity_name: Mapped[str] = mapped_column(String(50))
    description: Mapped[Optional[str]] = mapped_column(Text)
