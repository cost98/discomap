"""
Modello VerificationStatus - Tabella verification_status (stato verifica dati).
"""

from typing import Optional

from sqlalchemy import Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class VerificationStatus(Base):
    """Tabella verification_status - Stato verifica dati."""

    __tablename__ = "verification_status"
    __table_args__ = {"schema": "airquality"}

    verification_code: Mapped[int] = mapped_column(Integer, primary_key=True)
    verification_name: Mapped[str] = mapped_column(String(50))
    description: Mapped[Optional[str]] = mapped_column(Text)
