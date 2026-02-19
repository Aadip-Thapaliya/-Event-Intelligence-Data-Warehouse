-- ============================================================
-- RAW SCHEMA: Landing zone for all ingested event data
-- Data lands here untouched before transformation
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- -----------------------------------------------
-- Raw events table: stores API responses as-is
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS raw.events (
    ingest_id           SERIAL PRIMARY KEY,
    source              VARCHAR(50) NOT NULL,          -- 'ticketmaster', 'csv', etc.
    raw_event_id        VARCHAR(100),                  -- original ID from source
    raw_payload         JSONB,                         -- full raw JSON from API
    event_name          TEXT,
    event_date          TEXT,                          -- raw string before parsing
    event_time          TEXT,
    venue_name          TEXT,
    venue_city          TEXT,
    venue_country       TEXT,
    venue_lat           NUMERIC(10, 7),
    venue_lon           NUMERIC(10, 7),
    category            TEXT,
    subcategory         TEXT,
    price_min           TEXT,
    price_max           TEXT,
    currency            TEXT,
    url                 TEXT,
    status              TEXT,
    ingested_at         TIMESTAMP DEFAULT NOW(),
    is_processed        BOOLEAN DEFAULT FALSE,
    process_error       TEXT                           -- error message if transform failed
);

-- Index for pipeline efficiency
CREATE INDEX IF NOT EXISTS idx_raw_events_processed ON raw.events(is_processed);
CREATE INDEX IF NOT EXISTS idx_raw_events_source ON raw.events(source);
CREATE INDEX IF NOT EXISTS idx_raw_events_ingested ON raw.events(ingested_at);

-- -----------------------------------------------
-- Ingestion log: tracks each pipeline run
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ingestion_log (
    log_id          SERIAL PRIMARY KEY,
    run_id          UUID DEFAULT gen_random_uuid(),
    source          VARCHAR(50),
    started_at      TIMESTAMP DEFAULT NOW(),
    finished_at     TIMESTAMP,
    records_fetched INTEGER DEFAULT 0,
    records_loaded  INTEGER DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'running',    -- running, success, failed
    error_message   TEXT
);

-- -----------------------------------------------
-- Data quality log: tracks quality check results
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS raw.quality_log (
    check_id        SERIAL PRIMARY KEY,
    run_id          UUID,
    check_name      VARCHAR(100),
    table_name      VARCHAR(100),
    records_checked INTEGER,
    records_failed  INTEGER,
    pass_rate       NUMERIC(5,2),
    checked_at      TIMESTAMP DEFAULT NOW(),
    status          VARCHAR(20)                        -- passed, warning, failed
);
