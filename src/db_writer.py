"""
PostgreSQL Database Writer for DiscoMap

Handles writing air quality data to PostgreSQL/TimescaleDB.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# Database connection
try:
    import psycopg2
    from psycopg2.extras import execute_values
    from psycopg2.pool import SimpleConnectionPool
except ImportError:
    print("⚠️  psycopg2 not installed. Install with: pip install psycopg2-binary")
    psycopg2 = None

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import Config
from src.logger import get_logger

logger = get_logger(__name__)


class PostgreSQLWriter:
    """Write air quality data to PostgreSQL/TimescaleDB."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        user: str = None,
        password: str = None,
        min_connections: int = 1,
        max_connections: int = 5,
    ):
        """Initialize PostgreSQL connection pool."""
        if psycopg2 is None:
            raise ImportError("psycopg2 is required. Install with: pip install psycopg2-binary")

        # Get connection details from environment or parameters
        self.host = host or os.getenv("DB_HOST", "localhost")
        self.port = port or int(os.getenv("DB_PORT", "5432"))
        self.database = database or os.getenv("DB_NAME", "discomap")
        self.user = user or os.getenv("DB_USER", "discomap")
        self.password = password or os.getenv("DB_PASSWORD", "changeme")

        # Create connection pool
        try:
            self.pool = SimpleConnectionPool(
                min_connections,
                max_connections,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            logger.info(
                f"✅ PostgreSQL connection pool created: {self.host}:{self.port}/{self.database}"
            )
        except Exception as e:
            logger.error(f"❌ Failed to create connection pool: {e}")
            raise

    def get_connection(self):
        """Get connection from pool."""
        return self.pool.getconn()

    def return_connection(self, conn):
        """Return connection to pool."""
        self.pool.putconn(conn)

    def close_all(self):
        """Close all connections in pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("PostgreSQL connection pool closed")

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()
            self.return_connection(conn)
            logger.info(f"✅ PostgreSQL connection OK: {version}")
            return True
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False

    def insert_measurements(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        Insert measurements from DataFrame into PostgreSQL.

        Returns:
            Number of rows inserted
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to insert")
            return 0

        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Prepare data for insertion
            columns = [
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
            ]

            # Map DataFrame columns to database columns
            df_mapped = df.copy()
            df_mapped = df_mapped.rename(
                columns={
                    "Start": "time",
                    "Samplingpoint": "sampling_point_id",
                    "Pollutant": "pollutant_code",
                    "Value": "value",
                    "Unit": "unit",
                    "AggType": "aggregation_type",
                    "Validity": "validity",
                    "Verification": "verification",
                    "DataCapture": "data_capture",
                    "ResultTime": "result_time",
                    "FkObservationLog": "observation_id",
                }
            )

            # Ensure all required columns exist
            for col in columns:
                if col not in df_mapped.columns:
                    df_mapped[col] = None

            # Convert to list of tuples
            data = [tuple(row) for row in df_mapped[columns].values]

            # Build INSERT query with ON CONFLICT
            query = f"""
                INSERT INTO airquality.measurements 
                ({', '.join(columns)})
                VALUES %s
                ON CONFLICT DO NOTHING
            """

            # Execute batch insert
            inserted = 0
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                execute_values(cursor, query, batch)
                inserted += cursor.rowcount

                if (i + batch_size) % 10000 == 0:
                    logger.info(f"Inserted {i + batch_size}/{len(data)} rows...")

            conn.commit()
            logger.info(f"✅ Inserted {inserted} measurements into PostgreSQL")

            return inserted

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Failed to insert measurements: {e}")
            raise
        finally:
            cursor.close()
            self.return_connection(conn)

    def upsert_sampling_points(self, df: pd.DataFrame) -> int:
        """
        Insert or update sampling points and extract stations.
        
        Extracts:
        - Physical stations from sampling_point_id (e.g., IT0508A)
        - Instrument type (e.g., 8_chemi, 5_BETA)
        - Pollutant code from measurements

        Returns:
            Number of sampling points upserted
        """
        if df.empty or "Samplingpoint" not in df.columns:
            return 0

        # Get unique sampling points - handle different pollutant column names
        pollutant_col = None
        for col in ["AirPollutantCode", "Pollutant", "pollutant_code"]:
            if col in df.columns:
                pollutant_col = col
                break
        
        if pollutant_col:
            points = df[["Samplingpoint", pollutant_col]].drop_duplicates()
        else:
            points = df[["Samplingpoint"]].drop_duplicates()
            points[pollutant_col] = None  # Add null column

        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # First, extract and upsert unique stations
            station_query = """
                INSERT INTO airquality.stations 
                (station_code, country_code)
                VALUES (%s, %s)
                ON CONFLICT (station_code) DO NOTHING
            """
            
            station_data = []
            for _, row in points.iterrows():
                sp_id = row["Samplingpoint"]
                if "/SPO." in sp_id:
                    # Extract station code: IT/SPO.IT0508A_8_chemi_1990... -> IT0508A
                    import re
                    match = re.search(r'SPO\.([A-Z]{2}[0-9]+[A-Z]?)_', sp_id)
                    if match:
                        station_code = match.group(1)
                        country_code = sp_id.split("/")[0]
                        station_data.append((station_code, country_code))
            
            # Remove duplicates
            station_data = list(set(station_data))
            if station_data:
                cursor.executemany(station_query, station_data)
                logger.info(f"✅ Upserted {cursor.rowcount} stations")

            # Now insert sampling points with extracted metadata
            sp_query = """
                INSERT INTO airquality.sampling_points 
                (sampling_point_id, station_code, country_code, instrument_type, pollutant_code)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (sampling_point_id) DO UPDATE SET
                    pollutant_code = COALESCE(EXCLUDED.pollutant_code, sampling_points.pollutant_code)
            """

            sp_data = []
            for _, row in points.iterrows():
                sp_id = row["Samplingpoint"]
                country_code = sp_id.split("/")[0] if "/" in sp_id else None
                station_code = None
                instrument_type = None
                
                if "/SPO." in sp_id:
                    import re
                    # Extract station code
                    station_match = re.search(r'SPO\.([A-Z]{2}[0-9]+[A-Z]?)_', sp_id)
                    if station_match:
                        station_code = station_match.group(1)
                    
                    # Extract instrument type: IT0508A_8_chemi_1990... -> 8_chemi
                    instrument_match = re.search(r'[A-Z]{2}[0-9]+[A-Z]?_([^_]+_[^_]+)_[0-9]{4}', sp_id)
                    if instrument_match:
                        instrument_type = instrument_match.group(1)
                
                # Get pollutant code from the appropriate column
                pollutant_code = None
                if pollutant_col and pollutant_col in row.index:
                    try:
                        pollutant_code = int(row[pollutant_col]) if pd.notna(row[pollutant_col]) else None
                    except (ValueError, TypeError):
                        pass
                
                sp_data.append((sp_id, station_code, country_code, instrument_type, pollutant_code))

            cursor.executemany(sp_query, sp_data)
            conn.commit()

            upserted = cursor.rowcount
            logger.info(f"✅ Upserted {upserted} sampling points")


            return upserted

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Failed to upsert sampling points: {e}")
            raise
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_latest_timestamp(
        self, sampling_point_id: str = None, pollutant_code: int = None
    ) -> Optional[datetime]:
        """Get latest measurement timestamp for filtering."""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            query = "SELECT MAX(time) FROM airquality.measurements WHERE 1=1"
            params = []

            if sampling_point_id:
                query += " AND sampling_point_id = %s"
                params.append(sampling_point_id)

            if pollutant_code:
                query += " AND pollutant_code = %s"
                params.append(pollutant_code)

            cursor.execute(query, params)
            result = cursor.fetchone()[0]

            return result

        finally:
            cursor.close()
            self.return_connection(conn)

    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Execute a SELECT query and return results."""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return results
        finally:
            cursor.close()
            self.return_connection(conn)

    def get_statistics(self) -> Dict:
        """Get database statistics."""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            stats = {}

            # Total measurements
            cursor.execute("SELECT COUNT(*) FROM airquality.measurements")
            stats["total_measurements"] = cursor.fetchone()[0]

            # Unique stations
            cursor.execute(
                "SELECT COUNT(DISTINCT sampling_point_id) FROM airquality.sampling_points"
            )
            stats["total_stations"] = cursor.fetchone()[0]

            # Date range
            cursor.execute("SELECT MIN(time), MAX(time) FROM airquality.measurements")
            min_date, max_date = cursor.fetchone()
            stats["date_range"] = {"min": min_date, "max": max_date}

            # Pollutants
            cursor.execute(
                """
                SELECT p.pollutant_name, COUNT(*) as count
                FROM airquality.measurements m
                JOIN airquality.pollutants p ON m.pollutant_code = p.pollutant_code
                GROUP BY p.pollutant_name
                ORDER BY count DESC
            """
            )
            stats["pollutants"] = dict(cursor.fetchall())

            # Database size
            cursor.execute(
                """
                SELECT pg_size_pretty(pg_database_size(%s))
            """,
                (self.database,),
            )
            stats["database_size"] = cursor.fetchone()[0]

            return stats

        finally:
            cursor.close()
            self.return_connection(conn)

    def start_sync_operation(
        self,
        operation_type: str,
        country_code: Optional[str] = None,
        pollutant_code: Optional[int] = None,
        metadata: Optional[Dict] = None,
    ) -> int:
        """
        Start tracking a sync operation.
        
        Args:
            operation_type: Type of operation ('initial', 'incremental', 'hourly')
            country_code: Country code (optional, stored in metadata)
            pollutant_code: Pollutant code (optional, stored in metadata)
            metadata: Additional metadata (optional)
            
        Returns:
            operation_id: ID of the created operation record
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Add country_code and pollutant_code to metadata if provided
            if metadata is None:
                metadata = {}
            if country_code:
                metadata['country_code'] = country_code
            if pollutant_code:
                metadata['pollutant_code'] = pollutant_code
            
            cursor.execute(
                """
                INSERT INTO airquality.sync_operations 
                (operation_type, metadata, status)
                VALUES (%s, %s, 'running')
                RETURNING operation_id
                """,
                (operation_type, json.dumps(metadata) if metadata else None),
            )
            operation_id = cursor.fetchone()[0]
            conn.commit()
            logger.info(f"📝 Started tracking sync operation: {operation_id} ({operation_type})")
            return operation_id

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to start sync operation tracking: {e}")
            return None

        finally:
            cursor.close()
            self.return_connection(conn)

    def update_sync_operation(
        self,
        operation_id: int,
        status: str = "running",
        records_downloaded: int = 0,
        records_inserted: int = 0,
        error_message: Optional[str] = None,
    ):
        """
        Update sync operation status.
        
        Args:
            operation_id: Operation ID
            status: Status ('running', 'completed', 'failed')
            records_downloaded: Number of records downloaded
            records_inserted: Number of records inserted
            error_message: Error message if failed
        """
        if operation_id is None:
            return

        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                UPDATE airquality.sync_operations
                SET status = %s,
                    records_downloaded = %s,
                    records_inserted = %s,
                    error_message = %s,
                    end_time = CASE WHEN %s IN ('completed', 'failed') THEN NOW() ELSE end_time END
                WHERE operation_id = %s
                """,
                (
                    status,
                    records_downloaded,
                    records_inserted,
                    error_message,
                    status,
                    operation_id,
                ),
            )
            conn.commit()

        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to update sync operation: {e}")

        finally:
            cursor.close()
            self.return_connection(conn)

    def complete_sync_operation(self, operation_id: int, records_downloaded: int, records_inserted: int):
        """Mark sync operation as completed."""
        self.update_sync_operation(
            operation_id, status="completed", records_downloaded=records_downloaded, records_inserted=records_inserted
        )
        logger.info(f"✅ Completed sync operation: {operation_id} ({records_inserted} records)")

    def fail_sync_operation(self, operation_id: int, error_message: str):
        """Mark sync operation as failed."""
        self.update_sync_operation(operation_id, status="failed", error_message=error_message)
        logger.error(f"❌ Failed sync operation: {operation_id} - {error_message}")


def main():
    """Test PostgreSQL writer."""
    import argparse

    parser = argparse.ArgumentParser(description="PostgreSQL Writer for DiscoMap")
    parser.add_argument("--test", action="store_true", help="Test connection")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--import", dest="import_file", help="Import parquet file")

    args = parser.parse_args()

    writer = PostgreSQLWriter()

    if args.test:
        if writer.test_connection():
            print("✅ Connection test successful")
        else:
            print("❌ Connection test failed")
            sys.exit(1)

    if args.stats:
        stats = writer.get_statistics()
        print("\n📊 Database Statistics:")
        print(f"Total Measurements: {stats['total_measurements']:,}")
        print(f"Total Stations: {stats['total_stations']}")
        print(f"Date Range: {stats['date_range']['min']} to {stats['date_range']['max']}")
        print(f"Database Size: {stats['database_size']}")
        print("\nPollutants:")
        for name, count in stats["pollutants"].items():
            print(f"  {name}: {count:,}")

    if args.import_file:
        df = pd.read_parquet(args.import_file)
        print(f"📂 Loaded {len(df):,} records from {args.import_file}")

        # Upsert sampling points
        writer.upsert_sampling_points(df)

        # Insert measurements
        inserted = writer.insert_measurements(df)
        print(f"✅ Inserted {inserted:,} measurements")

    writer.close_all()


if __name__ == "__main__":
    main()
