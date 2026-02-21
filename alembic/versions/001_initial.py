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
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
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
        sa.Column('metadata', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
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
    # Composite index for filter queries (country + pollutant)
    op.create_index('idx_sampling_points_country_pollutant', 'sampling_points', ['country_code', 'pollutant_code'], schema='airquality')
    
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
    
    # Create partial index for valid measurements (validity >= 1)
    op.execute("""
        CREATE INDEX idx_measurements_validity 
        ON airquality.measurements(validity) 
        WHERE validity >= 1
    """)
    
    # Create composite index for queries filtering by sampling_point, validity and time
    op.execute("""
        CREATE INDEX idx_measurements_sp_validity_time 
        ON airquality.measurements(sampling_point_id, validity, time DESC) 
        WHERE validity >= 1
    """)
    
    # Convert measurements to TimescaleDB hypertable
    op.execute("""
        SELECT create_hypertable(
            'airquality.measurements',
            'time',
            if_not_exists => TRUE,
            chunk_time_interval => INTERVAL '7 days'
        )
    """)
    
    # Enable compression on measurements hypertable
    # This allows compressing older chunks to save ~90% disk space
    op.execute("""
        ALTER TABLE airquality.measurements SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'sampling_point_id',
            timescaledb.compress_orderby = 'time DESC, pollutant_code'
        )
    """)
    
    # Insert lookup data - Countries
    op.execute("""
        INSERT INTO airquality.countries (country_code, country_name, region) VALUES
        ('IT', 'Italy', 'Southern Europe'),
        ('PT', 'Portugal', 'Southern Europe'),
        ('ES', 'Spain', 'Southern Europe')
        ON CONFLICT (country_code) DO NOTHING
    """)
    
    # Insert lookup data - Pollutants
    op.execute("""
        INSERT INTO airquality.pollutants (pollutant_code, pollutant_name, pollutant_label, unit) VALUES
        (1, 'PM10', 'Particulate matter < 10 µm', 'µg/m³'),
        (5, 'PM2.5', 'Particulate matter < 2.5 µm', 'µg/m³'),
        (6, 'NO2', 'Nitrogen dioxide (air)', 'µg/m³'),
        (7, 'O3', 'Ozone', 'µg/m³'),
        (8, 'NO2', 'Nitrogen dioxide', 'µg/m³'),
        (9, 'NOx', 'Nitrogen oxides', 'µg/m³'),
        (10, 'CO', 'Carbon monoxide', 'mg/m³'),
        (14, 'SO2', 'Sulphur dioxide', 'µg/m³'),
        (20, 'C6H6', 'Benzene', 'µg/m³'),
        (21, 'Cd', 'Cadmium', 'ng/m³'),
        (38, 'NO', 'Nitrogen monoxide', 'µg/m³'),
        (46, 'PM1', 'Particulate matter < 1 µm', 'µg/m³'),
        (47, 'BC', 'Black Carbon', 'µg/m³'),
        (48, 'UFP', 'Ultrafine particles', '#/cm³'),
        (49, 'PN', 'Particle number', '#/cm³'),
        (59, 'BaP', 'Benzo(a)pyrene', 'ng/m³'),
        (5012, 'Ni', 'Nickel', 'ng/m³'),
        (5014, 'As', 'Arsenic', 'ng/m³'),
        (5015, 'Pb', 'Lead', 'µg/m³'),
        (5029, 'BaP', 'Benzo(a)pyrene (PM10)', 'ng/m³'),
        (6001, 'PM2.5', 'Particulate matter < 2.5 µm (legacy)', 'µg/m³')
        ON CONFLICT (pollutant_code) DO NOTHING
    """)
    
    # Insert lookup data - Validity flags
    op.execute("""
        INSERT INTO airquality.validity_flags (validity_code, validity_name, description) VALUES
        (-99, 'Not valid (maintenance)', 'Not valid due to station maintenance or calibration'),
        (-1, 'Not valid', 'Not valid'),
        (1, 'Valid', 'Valid measurement'),
        (2, 'Valid (below detection)', 'Valid, but below detection limit'),
        (3, 'Valid (below detection/LOQ)', 'Valid, but below detection limit and limit of quantification'),
        (4, 'Valid (CCQM.O3.2019)', 'Valid (Ozone only) using CCQM.O3.2019')
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
    op.drop_index('idx_measurements_sp_validity_time', table_name='measurements', schema='airquality')
    op.drop_index('idx_measurements_validity', table_name='measurements', schema='airquality')
    op.drop_index('idx_measurements_sampling_point', table_name='measurements', schema='airquality')
    op.drop_index('idx_measurements_pollutant', table_name='measurements', schema='airquality')
    op.drop_table('measurements', schema='airquality')
    
    op.drop_index('idx_sampling_points_country_pollutant', table_name='sampling_points', schema='airquality')
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
