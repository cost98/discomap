"""Initial schema from SQLAlchemy models

Revision ID: 001_initial
Revises: 
Create Date: 2025-12-27 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001_initial'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create all tables from SQLAlchemy models."""
    
    # Create schema
    op.execute("CREATE SCHEMA IF NOT EXISTS airquality")
    op.execute("SET search_path TO airquality, public")
    
    # Enable TimescaleDB extension
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
    
    # Create countries table
    op.create_table(
        'countries',
        sa.Column('country_code', sa.String(length=2), nullable=False),
        sa.Column('country_name', sa.String(length=100), nullable=False),
        sa.Column('region', sa.String(length=50), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('country_code'),
        schema='airquality'
    )
    
    # Create pollutants table
    op.create_table(
        'pollutants',
        sa.Column('pollutant_code', sa.Integer(), nullable=False),
        sa.Column('pollutant_name', sa.String(length=20), nullable=False),
        sa.Column('pollutant_label', sa.String(length=100), nullable=True),
        sa.Column('unit', sa.String(length=20), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.PrimaryKeyConstraint('pollutant_code'),
        schema='airquality'
    )
    
    # Create validity_flags table
    op.create_table(
        'validity_flags',
        sa.Column('validity_code', sa.Integer(), nullable=False),
        sa.Column('validity_name', sa.String(length=50), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('validity_code'),
        schema='airquality'
    )
    
    # Create verification_status table
    op.create_table(
        'verification_status',
        sa.Column('verification_code', sa.Integer(), nullable=False),
        sa.Column('verification_name', sa.String(length=50), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('verification_code'),
        schema='airquality'
    )
    
    # Create stations table
    op.create_table(
        'stations',
        sa.Column('station_code', sa.String(length=50), nullable=False),
        sa.Column('country_code', sa.String(length=2), nullable=True),
        sa.Column('station_name', sa.String(length=200), nullable=True),
        sa.Column('station_type', sa.String(length=50), nullable=True),
        sa.Column('area_type', sa.String(length=50), nullable=True),
        sa.Column('latitude', sa.Double(), nullable=True),
        sa.Column('longitude', sa.Double(), nullable=True),
        sa.Column('altitude', sa.Float(), nullable=True),
        sa.Column('municipality', sa.String(length=100), nullable=True),
        sa.Column('region', sa.String(length=100), nullable=True),
        sa.Column('start_date', sa.Date(), nullable=True),
        sa.Column('end_date', sa.Date(), nullable=True),
        sa.Column('extra_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['country_code'], ['airquality.countries.country_code'], ),
        sa.PrimaryKeyConstraint('station_code'),
        schema='airquality'
    )
    op.create_index('idx_stations_coords', 'stations', ['latitude', 'longitude'], schema='airquality')
    op.create_index('idx_stations_country', 'stations', ['country_code'], schema='airquality')
    
    # Create sampling_points table
    op.create_table(
        'sampling_points',
        sa.Column('sampling_point_id', sa.String(length=100), nullable=False),
        sa.Column('station_code', sa.String(length=50), nullable=True),
        sa.Column('country_code', sa.String(length=2), nullable=True),
        sa.Column('instrument_type', sa.String(length=50), nullable=True),
        sa.Column('pollutant_code', sa.Integer(), nullable=True),
        sa.Column('start_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('end_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('extra_metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['country_code'], ['airquality.countries.country_code'], ),
        sa.ForeignKeyConstraint(['pollutant_code'], ['airquality.pollutants.pollutant_code'], ),
        sa.ForeignKeyConstraint(['station_code'], ['airquality.stations.station_code'], ),
        sa.PrimaryKeyConstraint('sampling_point_id'),
        schema='airquality'
    )
    op.create_index('idx_sampling_points_country', 'sampling_points', ['country_code'], schema='airquality')
    op.create_index('idx_sampling_points_pollutant', 'sampling_points', ['pollutant_code'], schema='airquality')
    op.create_index('idx_sampling_points_station', 'sampling_points', ['station_code'], schema='airquality')
    
    # Create measurements table (will be converted to hypertable)
    op.create_table(
        'measurements',
        sa.Column('time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('sampling_point_id', sa.String(length=100), nullable=False),
        sa.Column('pollutant_code', sa.Integer(), nullable=False),
        sa.Column('value', sa.Double(), nullable=True),
        sa.Column('unit', sa.String(length=20), nullable=True),
        sa.Column('aggregation_type', sa.String(length=10), nullable=True),
        sa.Column('validity', sa.Integer(), nullable=True),
        sa.Column('verification', sa.Integer(), nullable=True),
        sa.Column('data_capture', sa.Float(), nullable=True),
        sa.Column('result_time', sa.DateTime(timezone=True), nullable=True),
        sa.Column('observation_id', sa.String(length=100), nullable=True),
        sa.ForeignKeyConstraint(['pollutant_code'], ['airquality.pollutants.pollutant_code'], ),
        sa.ForeignKeyConstraint(['sampling_point_id'], ['airquality.sampling_points.sampling_point_id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['validity'], ['airquality.validity_flags.validity_code'], ),
        sa.ForeignKeyConstraint(['verification'], ['airquality.verification_status.verification_code'], ),
        schema='airquality'
    )
    op.create_index('idx_measurements_pollutant', 'measurements', ['pollutant_code', 'time'], schema='airquality')
    op.create_index('idx_measurements_sampling_point', 'measurements', ['sampling_point_id', 'time'], schema='airquality')
    
    # Convert measurements to TimescaleDB hypertable
    op.execute("""
        SELECT create_hypertable(
            'airquality.measurements',
            'time',
            if_not_exists => TRUE,
            chunk_time_interval => INTERVAL '7 days'
        )
    """)
    
    # Insert lookup data - Countries
    op.execute("""
        INSERT INTO airquality.countries (country_code, country_name, region) VALUES
        ('IT', 'Italy', 'Southern Europe'),
        ('AT', 'Austria', 'Central Europe'),
        ('BE', 'Belgium', 'Western Europe'),
        ('FR', 'France', 'Western Europe'),
        ('DE', 'Germany', 'Central Europe'),
        ('ES', 'Spain', 'Southern Europe')
        ON CONFLICT (country_code) DO NOTHING
    """)
    
    # Insert lookup data - Pollutants
    op.execute("""
        INSERT INTO airquality.pollutants (pollutant_code, pollutant_name, pollutant_label, unit) VALUES
        (1, 'SO2', 'Sulphur dioxide', 'µg/m³'),
        (5, 'PM10', 'Particulate matter < 10 µm', 'µg/m³'),
        (6001, 'PM2.5', 'Particulate matter < 2.5 µm', 'µg/m³'),
        (7, 'O3', 'Ozone', 'µg/m³'),
        (8, 'NO2', 'Nitrogen dioxide', 'µg/m³'),
        (10, 'CO', 'Carbon monoxide', 'mg/m³')
        ON CONFLICT (pollutant_code) DO NOTHING
    """)
    
    # Insert lookup data - Validity flags
    op.execute("""
        INSERT INTO airquality.validity_flags (validity_code, validity_name, description) VALUES
        (1, 'Valid', 'Valid measurement'),
        (2, 'Invalid', 'Invalid measurement'),
        (3, 'Unverified', 'Not yet verified')
        ON CONFLICT (validity_code) DO NOTHING
    """)
    
    # Insert lookup data - Verification status
    op.execute("""
        INSERT INTO airquality.verification_status (verification_code, verification_name, description) VALUES
        (1, 'Verified', 'Data verified and approved'),
        (2, 'Preliminary', 'Preliminary data verification'),
        (3, 'Not verified', 'Data not verified')
        ON CONFLICT (verification_code) DO NOTHING
    """)


def downgrade() -> None:
    """Drop all tables."""
    op.drop_index('idx_measurements_sampling_point', table_name='measurements', schema='airquality')
    op.drop_index('idx_measurements_pollutant', table_name='measurements', schema='airquality')
    op.drop_table('measurements', schema='airquality')
    
    op.drop_index('idx_sampling_points_station', table_name='sampling_points', schema='airquality')
    op.drop_index('idx_sampling_points_pollutant', table_name='sampling_points', schema='airquality')
    op.drop_index('idx_sampling_points_country', table_name='sampling_points', schema='airquality')
    op.drop_table('sampling_points', schema='airquality')
    
    op.drop_index('idx_stations_country', table_name='stations', schema='airquality')
    op.drop_index('idx_stations_coords', table_name='stations', schema='airquality')
    op.drop_table('stations', schema='airquality')
    
    op.drop_table('verification_status', schema='airquality')
    op.drop_table('validity_flags', schema='airquality')
    op.drop_table('pollutants', schema='airquality')
    op.drop_table('countries', schema='airquality')
    
    op.execute("DROP SCHEMA IF EXISTS airquality CASCADE")
