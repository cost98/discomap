-- Create tables for DiscoMap air quality data
-- Optimized for TimescaleDB time-series storage

SET search_path TO airquality, public;

-- ============================================
-- Dimension Tables (Small, static reference data)
-- ============================================

-- Countries
CREATE TABLE IF NOT EXISTS countries (
    country_code VARCHAR(2) PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE countries IS 'ISO country codes and names';

-- Insert EU countries (focus on Italy for now, can expand later)
INSERT INTO countries (country_code, country_name, region) VALUES
('IT', 'Italy', 'Southern Europe'),
('AT', 'Austria', 'Central Europe'),
('BE', 'Belgium', 'Western Europe'),
('BG', 'Bulgaria', 'Eastern Europe'),
('HR', 'Croatia', 'Southern Europe'),
('CY', 'Cyprus', 'Southern Europe'),
('CZ', 'Czechia', 'Eastern Europe'),
('DK', 'Denmark', 'Northern Europe'),
('EE', 'Estonia', 'Northern Europe'),
('FI', 'Finland', 'Northern Europe'),
('FR', 'France', 'Western Europe'),
('DE', 'Germany', 'Central Europe'),
('GR', 'Greece', 'Southern Europe'),
('HU', 'Hungary', 'Eastern Europe'),
('IE', 'Ireland', 'Northern Europe'),
('LV', 'Latvia', 'Northern Europe'),
('LT', 'Lithuania', 'Northern Europe'),
('LU', 'Luxembourg', 'Western Europe'),
('MT', 'Malta', 'Southern Europe'),
('NL', 'Netherlands', 'Western Europe'),
('PL', 'Poland', 'Eastern Europe'),
('PT', 'Portugal', 'Southern Europe'),
('RO', 'Romania', 'Eastern Europe'),
('SK', 'Slovakia', 'Eastern Europe'),
('SI', 'Slovenia', 'Central Europe'),
('ES', 'Spain', 'Southern Europe'),
('SE', 'Sweden', 'Northern Europe')
ON CONFLICT (country_code) DO NOTHING;

-- Pollutants
CREATE TABLE IF NOT EXISTS pollutants (
    pollutant_code INTEGER PRIMARY KEY,
    pollutant_name VARCHAR(20) NOT NULL,
    pollutant_label VARCHAR(100),
    unit VARCHAR(20),
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE pollutants IS 'EEA pollutant codes and metadata';

-- Insert common pollutants
INSERT INTO pollutants (pollutant_code, pollutant_name, pollutant_label, unit, description) VALUES
    (1, 'SO2', 'Sulphur dioxide', 'µg/m³', 'Sulphur dioxide'),
    (5, 'PM10', 'Particulate matter < 10 µm', 'µg/m³', 'Particulate matter less than 10 micrometers'),
    (6001, 'PM2.5', 'Particulate matter < 2.5 µm', 'µg/m³', 'Particulate matter less than 2.5 micrometers'),
    (7, 'O3', 'Ozone', 'µg/m³', 'Ozone'),
    (8, 'NO2', 'Nitrogen dioxide', 'µg/m³', 'Nitrogen dioxide'),
    (9, 'NOX', 'Nitrogen oxides as NO2', 'µg/m³', 'Nitrogen oxides'),
    (10, 'CO', 'Carbon monoxide', 'mg/m³', 'Carbon monoxide'),
    (20, 'C6H6', 'Benzene', 'µg/m³', 'Benzene'),
    (5012, 'PM10', 'PM10 (aerosol)', 'µg/m³', 'PM10 aerosol'),
    (5029, 'PM2.5', 'PM2.5 (aerosol)', 'µg/m³', 'PM2.5 aerosol'),
    (12, 'Pb', 'Lead', 'µg/m³', 'Lead in PM10'),
    (14, 'Cd', 'Cadmium', 'ng/m³', 'Cadmium in PM10'),
    (15, 'Ni', 'Nickel', 'ng/m³', 'Nickel in PM10'),
    (18, 'As', 'Arsenic', 'ng/m³', 'Arsenic in PM10'),
    (21, 'C7H8', 'Toluene', 'µg/m³', 'Toluene'),
    (5610, 'BaP', 'Benzo(a)pyrene', 'ng/m³', 'Benzo(a)pyrene in PM10')
ON CONFLICT (pollutant_code) DO NOTHING;

-- Physical Stations (Unique locations)
CREATE TABLE IF NOT EXISTS stations (
    station_code VARCHAR(50) PRIMARY KEY,  -- e.g., 'IT0508A'
    country_code VARCHAR(2) REFERENCES countries(country_code),
    station_name VARCHAR(200),
    station_type VARCHAR(50),  -- 'traffic', 'background', 'industrial'
    area_type VARCHAR(50),     -- 'urban', 'suburban', 'rural'
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude REAL,
    municipality VARCHAR(100),
    region VARCHAR(100),
    start_date DATE,
    end_date DATE,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stations_country ON stations(country_code);
CREATE INDEX IF NOT EXISTS idx_stations_coords ON stations(latitude, longitude);

COMMENT ON TABLE stations IS 'Physical monitoring stations (unique locations)';

-- Sampling Points (Sensors/Instruments at stations)
CREATE TABLE IF NOT EXISTS sampling_points (
    sampling_point_id VARCHAR(100) PRIMARY KEY,  -- Full EEA ID with instrument type
    station_code VARCHAR(50) REFERENCES stations(station_code),
    country_code VARCHAR(2) REFERENCES countries(country_code),
    instrument_type VARCHAR(50),   -- e.g., '8_chemi', '5_BETA', '7_UV-P'
    pollutant_code INTEGER REFERENCES pollutants(pollutant_code),
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sampling_points_station ON sampling_points(station_code);
CREATE INDEX IF NOT EXISTS idx_sampling_points_country ON sampling_points(country_code);
CREATE INDEX IF NOT EXISTS idx_sampling_points_pollutant ON sampling_points(pollutant_code);

COMMENT ON TABLE sampling_points IS 'Individual sensors/instruments at monitoring stations';
COMMENT ON COLUMN sampling_points.sampling_point_id IS 'Full EEA ID: IT/SPO.{STATION}_{INSTRUMENT}_{START_DATE}';
COMMENT ON COLUMN sampling_points.station_code IS 'Reference to physical station (e.g., IT0508A)';

-- Validity Flags Lookup
CREATE TABLE IF NOT EXISTS validity_flags (
    validity_code INTEGER PRIMARY KEY,
    validity_name VARCHAR(50) NOT NULL,
    description TEXT
);

INSERT INTO validity_flags (validity_code, validity_name, description) VALUES
    (-99, 'Not valid due to station maintenance', 'Not valid due to station maintenance or other issues'),
    (-1, 'Not valid', 'Not valid measurement'),
    (1, 'Valid', 'Valid measurement'),
    (2, 'Valid (below detection limit)', 'Valid, but below detection limit'),
    (3, 'Valid (below detection limit and...)', 'Valid, but below detection limit and additional conditions'),
    (4, 'Valid (Ozone CCQM)', 'Valid (Ozone only) using CCQM.O3.2019')
ON CONFLICT (validity_code) DO NOTHING;

-- Verification Status Lookup
CREATE TABLE IF NOT EXISTS verification_status (
    verification_code INTEGER PRIMARY KEY,
    verification_name VARCHAR(50) NOT NULL,
    description TEXT
);

INSERT INTO verification_status (verification_code, verification_name, description) VALUES
    (1, 'Verified', 'Data verified and approved'),
    (2, 'Preliminary verified', 'Preliminary data verification'),
    (3, 'Not verified', 'Data not verified')
ON CONFLICT (verification_code) DO NOTHING;

-- ============================================
-- Fact Table (Main time-series data)
-- ============================================

-- Measurements (Will be converted to hypertable)
CREATE TABLE IF NOT EXISTS measurements (
    time TIMESTAMPTZ NOT NULL,
    sampling_point_id VARCHAR(100) NOT NULL,
    pollutant_code INTEGER NOT NULL,
    value DOUBLE PRECISION,
    unit VARCHAR(20),
    aggregation_type VARCHAR(10),
    validity INTEGER,
    verification INTEGER,
    data_capture REAL,
    result_time TIMESTAMPTZ,
    observation_id VARCHAR(100),
    
    -- Constraints
    CONSTRAINT fk_sampling_point FOREIGN KEY (sampling_point_id) 
        REFERENCES sampling_points(sampling_point_id) ON DELETE CASCADE,
    CONSTRAINT fk_pollutant FOREIGN KEY (pollutant_code) 
        REFERENCES pollutants(pollutant_code),
    CONSTRAINT fk_validity FOREIGN KEY (validity) 
        REFERENCES validity_flags(validity_code),
    CONSTRAINT fk_verification FOREIGN KEY (verification) 
        REFERENCES verification_status(verification_code)
);

-- Indexes (will be optimized after hypertable conversion)
CREATE INDEX IF NOT EXISTS idx_measurements_sampling_point 
    ON measurements(sampling_point_id, time DESC);
    
CREATE INDEX IF NOT EXISTS idx_measurements_pollutant 
    ON measurements(pollutant_code, time DESC);

COMMENT ON TABLE measurements IS 'Air quality measurements time-series data';

-- ============================================
-- Grant Permissions
-- ============================================
-- Note: Aggregated statistics are computed using TimescaleDB continuous aggregates
-- (measurements_hourly and measurements_daily) defined in create-hypertables.sql

-- Grant permissions
GRANT SELECT ON ALL TABLES IN SCHEMA airquality TO discomap_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA airquality TO discomap_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA airquality TO discomap_writer;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'DiscoMap tables created successfully';
END
$$;
