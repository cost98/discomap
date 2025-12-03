-- Initialize DiscoMap Database
-- Run on first container startup

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
-- PostGIS not available in base TimescaleDB image, will add if needed
-- CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create schema
CREATE SCHEMA IF NOT EXISTS airquality;

-- Set search path
ALTER DATABASE discomap SET search_path TO airquality, public;

-- Create application user roles
CREATE ROLE discomap_reader;
CREATE ROLE discomap_writer;

GRANT CONNECT ON DATABASE discomap TO discomap_reader, discomap_writer;
GRANT USAGE ON SCHEMA airquality TO discomap_reader, discomap_writer;

-- Grant permissions (will be set after tables are created)
ALTER DEFAULT PRIVILEGES IN SCHEMA airquality 
    GRANT SELECT ON TABLES TO discomap_reader;
    
ALTER DEFAULT PRIVILEGES IN SCHEMA airquality 
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO discomap_writer;

-- Create indexes schema for maintenance
CREATE SCHEMA IF NOT EXISTS maintenance;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'DiscoMap database initialized successfully';
END
$$;
