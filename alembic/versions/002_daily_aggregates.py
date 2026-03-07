"""Add daily continuous aggregate

Revision ID: 002_daily_aggregates
Revises: 001_initial
Create Date: 2026-01-09 20:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '002_daily_aggregates'
down_revision: Union[str, None] = '001_initial'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create continuous aggregate for daily statistics."""
    
    # Set search path
    op.execute("SET search_path TO airquality, public")
    
    # Create continuous aggregate materialized view
    # Only includes valid measurements (validity >= 1)
    # Aggregates by station_code (joining with sampling_points)
    op.execute("""
        CREATE MATERIALIZED VIEW airquality.daily_measurements
        WITH (timescaledb.continuous) AS
        SELECT 
            time_bucket('1 day', m.time) AS day,
            sp.station_code,
            m.pollutant_code,
            AVG(m.value) as avg_value,
            MIN(m.value) as min_value,
            MAX(m.value) as max_value,
            STDDEV(m.value) as stddev_value,
            COUNT(*) as count,
            MIN(m.time) as first_measurement,
            MAX(m.time) as last_measurement
        FROM airquality.measurements m
        JOIN airquality.sampling_points sp ON m.sampling_point_id = sp.sampling_point_id
        WHERE m.validity >= 1 AND m.aggregation_type = 'hour'
        GROUP BY day, sp.station_code, m.pollutant_code
        WITH NO DATA
    """)
    
    # Add refresh policy (refresh every 1 hour, process recent 30 days)
    op.execute("""
        SELECT add_continuous_aggregate_policy('airquality.daily_measurements',
            start_offset => INTERVAL '30 days',
            end_offset => INTERVAL '1 day',
            schedule_interval => INTERVAL '1 hour'
        )
    """)
    
    # Create indexes for fast queries
    op.execute("""
        CREATE INDEX idx_daily_measurements_day 
        ON airquality.daily_measurements (day DESC)
    """)
    
    op.execute("""
        CREATE INDEX idx_daily_measurements_station 
        ON airquality.daily_measurements (station_code, day DESC)
    """)
    
    op.execute("""
        CREATE INDEX idx_daily_measurements_pollutant 
        ON airquality.daily_measurements (pollutant_code, day DESC)
    """)


def downgrade() -> None:
    """Remove continuous aggregate."""
    
    op.execute("SET search_path TO airquality, public")
    
    # Drop indexes (automatically dropped with view, but explicit is better)
    op.execute("DROP INDEX IF EXISTS airquality.idx_daily_measurements_pollutant")
    op.execute("DROP INDEX IF EXISTS airquality.idx_daily_measurements_station")
    op.execute("DROP INDEX IF EXISTS airquality.idx_daily_measurements_day")
    
    # Drop continuous aggregate (this also removes the policy)
    op.execute("DROP MATERIALIZED VIEW IF EXISTS airquality.daily_measurements CASCADE")
