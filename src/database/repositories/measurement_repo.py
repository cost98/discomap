"""
Repository per Measurement (time-series).
"""

from datetime import datetime
from io import StringIO
from typing import List, Sequence

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import Measurement
from src.logger import get_logger

logger = get_logger(__name__)


class MeasurementRepository:
    """Repository per operazioni su Measurement (time-series)."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def bulk_insert(self, measurements: List[dict]) -> int:
        """
        Inserimento bulk di misurazioni (ottimizzato con executemany).
        
        Uso:
            measurements = [
                {"time": datetime(...), "sampling_point_id": "...", "value": 25.5, ...},
                ...
            ]
            count = await repo.bulk_insert(measurements)
        """
        if not measurements:
            return 0
        
        # Usa execute con insert - asyncpg usa executemany ottimizzato
        await self.session.execute(
            Measurement.__table__.insert(),
            measurements
        )
        await self.session.flush()
        return len(measurements)

    async def bulk_copy(self, measurements: List[dict]) -> int:
        """
        Inserimento bulk usando PostgreSQL COPY (5-10x più veloce di INSERT).
        
        COPY bypassa il parser SQL e scrive direttamente nella tabella.
        Ideale per grandi volumi di dati (>1000 righe).
        
        Uso:
            measurements = [
                {"time": datetime(...), "sampling_point_id": "...", "value": 25.5, ...},
                ...
            ]
            count = await repo.bulk_copy(measurements)
        """
        if not measurements:
            return 0
        
        # Prepara i dati in formato testo (tab-separated) per COPY
        # Colonne usate dalla tabella `airquality.measurements`
        csv_buffer = StringIO()
        for m in measurements:
            # Formato timestamp ISO 8601 con timezone (PostgreSQL TIMESTAMPTZ)
            time_str = m["time"].strftime("%Y-%m-%d %H:%M:%S+00")

            # Campi obbligatori
            sampling_point_id = m["sampling_point_id"]
            pollutant_code = m["pollutant_code"]
            value = m.get("value")

            # Campi opzionali: supporta nomi vecchi/nuovi e usa \N per NULL
            unit = m.get("unit") if m.get("unit") is not None else "\\N"
            aggregation = m.get("aggregation_type") if m.get("aggregation_type") is not None else "\\N"
            validity = m.get("validity") or m.get("validity_flag_id") or "\\N"
            verification = m.get("verification") or m.get("verification_status_id") or "\\N"
            data_capture = m.get("data_capture") if m.get("data_capture") is not None else "\\N"
            result_time = (
                m.get("result_time").strftime("%Y-%m-%d %H:%M:%S+00")
                if m.get("result_time") is not None
                else "\\N"
            )
            observation_id = m.get("observation_id") or "\\N"

            # Serializza value (NULL -> \N)
            value_str = "\\N" if value is None else str(value)

            # Scrive riga (tab-separated)
            csv_buffer.write(
                f"{time_str}\t{sampling_point_id}\t{pollutant_code}\t{value_str}\t{unit}\t{aggregation}\t{validity}\t{verification}\t{data_capture}\t{result_time}\t{observation_id}\n"
            )

        # Ottieni connessione raw asyncpg dalla session SQLAlchemy
        csv_data = csv_buffer.getvalue().encode("utf-8")

        conn = await self.session.connection()
        raw_conn = await conn.get_raw_connection()

        # asyncpg copy_to_table richiede un stream di bytes
        from io import BytesIO
        bytes_buffer = BytesIO(csv_data)

        # Ensure copy reads from the start
        bytes_buffer.seek(0)

        # Set search_path so copy_to_table can target the schema by table name
        await raw_conn.driver_connection.execute("SET search_path TO airquality, public")

        # Call copy_to_table with unqualified table name (schema via search_path)
        await raw_conn.driver_connection.copy_to_table(
            "measurements",
            source=bytes_buffer,
            columns=[
                "time",
                "sampling_point_id",
                "pollutant_code",
                "value",
                "unit",
                "aggregation_type",
                "validity",
                "verification",
                "data_capture",
                "result_time",
                "observation_id",
            ],
            format="text",
        )
        
        await self.session.flush()
        logger.info(f"COPY inserted {len(measurements)} measurements")
        return len(measurements)

    async def get_latest(self, sampling_point_id: str, limit: int = 100) -> Sequence[Measurement]:
        """Ottieni ultime N misurazioni per sampling point."""
        result = await self.session.execute(
            select(Measurement)
            .where(Measurement.sampling_point_id == sampling_point_id)
            .order_by(Measurement.time.desc())
            .limit(limit)
        )
        return result.scalars().all()

    async def delete_time_range(
        self,
        sampling_point_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> int:
        """Elimina misurazioni in un range temporale (per re-sync)."""
        result = await self.session.execute(
            delete(Measurement).where(
                Measurement.sampling_point_id == sampling_point_id,
                Measurement.time >= start_time,
                Measurement.time <= end_time,
            )
        )
        await self.session.flush()
        return result.rowcount
