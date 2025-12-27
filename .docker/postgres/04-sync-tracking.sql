-- Sync operations tracking table
-- Tracks download and sync operations in progress

SET search_path TO airquality, public;

-- Sync operations log
CREATE TABLE IF NOT EXISTS sync_operations (
    operation_id SERIAL PRIMARY KEY,
    operation_type VARCHAR(50) NOT NULL,  -- 'initial', 'incremental', 'hourly'
    country_code VARCHAR(2) REFERENCES countries(country_code),
    pollutant_code INTEGER REFERENCES pollutants(pollutant_code),
    start_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    end_time TIMESTAMPTZ,
    status VARCHAR(20) NOT NULL DEFAULT 'running',  -- 'running', 'completed', 'failed'
    records_downloaded INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_sync_operations_status 
    ON sync_operations(status, start_time DESC);
    
CREATE INDEX IF NOT EXISTS idx_sync_operations_country 
    ON sync_operations(country_code, start_time DESC);

CREATE INDEX IF NOT EXISTS idx_sync_operations_time
    ON sync_operations(start_time DESC);

COMMENT ON TABLE sync_operations IS 'Log of all sync operations for monitoring';

-- View for current running operations
CREATE OR REPLACE VIEW sync_operations_current AS
SELECT 
    so.operation_id,
    so.operation_type,
    c.country_name,
    p.pollutant_name,
    so.start_time,
    EXTRACT(EPOCH FROM (NOW() - so.start_time))::INTEGER as duration_seconds,
    so.status,
    so.records_downloaded,
    so.records_inserted
FROM sync_operations so
LEFT JOIN countries c ON so.country_code = c.country_code
LEFT JOIN pollutants p ON so.pollutant_code = p.pollutant_code
WHERE so.status = 'running'
ORDER BY so.start_time DESC;

COMMENT ON VIEW sync_operations_current IS 'Currently running sync operations';

-- View for recent operations (last 24h)
CREATE OR REPLACE VIEW sync_operations_recent AS
SELECT 
    so.operation_id,
    so.operation_type,
    c.country_name,
    p.pollutant_name,
    so.start_time,
    so.end_time,
    EXTRACT(EPOCH FROM (COALESCE(so.end_time, NOW()) - so.start_time))::INTEGER as duration_seconds,
    so.status,
    so.records_downloaded,
    so.records_inserted,
    so.error_message
FROM sync_operations so
LEFT JOIN countries c ON so.country_code = c.country_code
LEFT JOIN pollutants p ON so.pollutant_code = p.pollutant_code
WHERE so.start_time >= NOW() - INTERVAL '24 hours'
ORDER BY so.start_time DESC
LIMIT 100;

COMMENT ON VIEW sync_operations_recent IS 'Recent sync operations (last 24 hours)';
