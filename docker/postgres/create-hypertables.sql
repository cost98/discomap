-- Convert tables to TimescaleDB hypertables
-- Enable time-series optimizations and compression

SET search_path TO airquality, public;

-- ============================================
-- Convert measurements to hypertable
-- ============================================

-- Create hypertable (partition by time)
SELECT create_hypertable(
    'measurements',
    'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create additional space partitioning (optional, for very large datasets)
-- SELECT add_dimension('measurements', 'sampling_point_id', number_partitions => 4);

-- Set compression policy (compress data older than 7 days)
ALTER TABLE measurements SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'sampling_point_id, pollutant_code',
    timescaledb.compress_orderby = 'time DESC'
);

-- Add compression policy
SELECT add_compression_policy('measurements', INTERVAL '7 days');

-- Add data retention policy (optional - keep last 2 years)
-- SELECT add_retention_policy('measurements', INTERVAL '2 years');

-- ============================================
-- Create continuous aggregates (materialized views)
-- ============================================
-- Note: Using continuous aggregates instead of separate stats tables
-- They are automatically maintained by TimescaleDB as new data arrives

-- Hourly aggregate (automatically maintained)
CREATE MATERIALIZED VIEW IF NOT EXISTS measurements_hourly
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 hour', time) AS hour,
    sampling_point_id,
    pollutant_code,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    STDDEV(value) AS stddev_value,
    COUNT(*) AS count_measurements,
    COUNT(*) FILTER (WHERE validity > 0) AS valid_count,
    AVG(data_capture) AS avg_data_capture
FROM measurements
WHERE validity IS NOT NULL
GROUP BY hour, sampling_point_id, pollutant_code;

-- Refresh policy (update every day, looking back 7 days)
SELECT add_continuous_aggregate_policy('measurements_hourly',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day'
);

-- Daily aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS measurements_daily
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('1 day', time) AS day,
    sampling_point_id,
    pollutant_code,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY value) AS p95_value,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY value) AS p99_value,
    COUNT(*) AS count_measurements,
    COUNT(*) FILTER (WHERE validity > 0) AS valid_count
FROM measurements
WHERE validity IS NOT NULL
GROUP BY day, sampling_point_id, pollutant_code;

SELECT add_continuous_aggregate_policy('measurements_daily',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day'
);

-- ============================================
-- Useful functions
-- ============================================

-- Function to get latest data timestamp
CREATE OR REPLACE FUNCTION get_latest_data_timestamp()
RETURNS TIMESTAMPTZ AS $$
BEGIN
    RETURN (SELECT MAX(time) FROM measurements);
END;
$$ LANGUAGE plpgsql;

-- Function to calculate exceedances
CREATE OR REPLACE FUNCTION count_exceedances(
    p_pollutant_code INTEGER,
    p_threshold DOUBLE PRECISION,
    p_start_date DATE,
    p_end_date DATE
) RETURNS TABLE(
    sampling_point_id VARCHAR,
    exceedance_count BIGINT,
    total_measurements BIGINT,
    exceedance_ratio NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        m.sampling_point_id,
        COUNT(*) FILTER (WHERE m.value > p_threshold) AS exceedance_count,
        COUNT(*) AS total_measurements,
        ROUND(
            COUNT(*) FILTER (WHERE m.value > p_threshold)::NUMERIC / 
            NULLIF(COUNT(*), 0) * 100, 
            2
        ) AS exceedance_ratio
    FROM measurements m
    WHERE m.pollutant_code = p_pollutant_code
        AND m.time::DATE BETWEEN p_start_date AND p_end_date
        AND m.validity > 0
    GROUP BY m.sampling_point_id
    ORDER BY exceedance_count DESC;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- Indexes for common queries
-- ============================================

-- Composite indexes for faster filtering
CREATE INDEX IF NOT EXISTS idx_measurements_country_pollutant_time
    ON measurements (
        (SELECT country_code FROM sampling_points sp WHERE sp.sampling_point_id = measurements.sampling_point_id),
        pollutant_code,
        time DESC
    );

-- Index for valid measurements only
CREATE INDEX IF NOT EXISTS idx_measurements_valid
    ON measurements (time DESC)
    WHERE validity > 0;

-- ============================================
-- Vacuum and analyze
-- ============================================

VACUUM ANALYZE measurements;
VACUUM ANALYZE hourly_stats;
VACUUM ANALYZE daily_stats;
VACUUM ANALYZE sampling_points;

-- Log completion
DO $$
BEGIN
    RAISE NOTICE 'TimescaleDB hypertables and optimizations configured successfully';
    RAISE NOTICE 'Compression enabled for data older than 7 days';
    RAISE NOTICE 'Continuous aggregates created with automatic refresh policies';
END
$$;
