-- ============================================================
-- STAR SCHEMA: Analytical warehouse layer
-- Optimised for reporting and KPI queries
-- ============================================================

CREATE SCHEMA IF NOT EXISTS warehouse;

-- -----------------------------------------------
-- DIM: Date dimension (pre-populated)
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_id         SERIAL PRIMARY KEY,
    full_date       DATE NOT NULL UNIQUE,
    day_of_week     VARCHAR(10),
    day_number      SMALLINT,
    week_number     SMALLINT,
    month_number    SMALLINT,
    month_name      VARCHAR(10),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN,
    is_holiday      BOOLEAN DEFAULT FALSE
);

-- -----------------------------------------------
-- DIM: Venue dimension (SCD Type 2 ready)
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_venue (
    venue_sk        SERIAL PRIMARY KEY,              -- surrogate key
    venue_nk        VARCHAR(100) NOT NULL,           -- natural key from source
    venue_name      VARCHAR(255),
    city            VARCHAR(100),
    country         VARCHAR(100),
    country_code    CHAR(2),
    latitude        NUMERIC(10, 7),
    longitude       NUMERIC(10, 7),
    capacity        INTEGER,
    -- SCD Type 2 tracking
    valid_from      DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to        DATE,
    is_current      BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_venue_nk ON warehouse.dim_venue(venue_nk);
CREATE INDEX IF NOT EXISTS idx_dim_venue_current ON warehouse.dim_venue(is_current);

-- -----------------------------------------------
-- DIM: Category dimension
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_category (
    category_sk     SERIAL PRIMARY KEY,
    category_name   VARCHAR(100) NOT NULL,
    subcategory     VARCHAR(100),
    segment         VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(category_name, subcategory)
);

-- -----------------------------------------------
-- DIM: Source dimension
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.dim_source (
    source_sk       SERIAL PRIMARY KEY,
    source_name     VARCHAR(50) NOT NULL UNIQUE,
    source_type     VARCHAR(50),                     -- api, csv, scrape
    created_at      TIMESTAMP DEFAULT NOW()
);

-- -----------------------------------------------
-- FACT: Events fact table
-- Grain: one row per event
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS warehouse.fact_events (
    event_sk        SERIAL PRIMARY KEY,
    event_nk        VARCHAR(100) NOT NULL,           -- natural key from source

    -- Foreign keys to dimensions
    date_id         INTEGER REFERENCES warehouse.dim_date(date_id),
    venue_sk        INTEGER REFERENCES warehouse.dim_venue(venue_sk),
    category_sk     INTEGER REFERENCES warehouse.dim_category(category_sk),
    source_sk       INTEGER REFERENCES warehouse.dim_source(source_sk),

    -- Degenerate dimensions
    event_name      TEXT,
    event_url       TEXT,
    event_status    VARCHAR(50),                     -- onsale, offsale, cancelled, rescheduled

    -- Measures
    price_min       NUMERIC(10, 2),
    price_max       NUMERIC(10, 2),
    price_avg       NUMERIC(10, 2),
    currency        CHAR(3),
    price_min_eur   NUMERIC(10, 2),                  -- normalised to EUR

    -- Audit
    ingested_at     TIMESTAMP,
    loaded_at       TIMESTAMP DEFAULT NOW(),
    is_deleted      BOOLEAN DEFAULT FALSE,           -- soft delete
    UNIQUE(event_nk)
);

CREATE INDEX IF NOT EXISTS idx_fact_events_date ON warehouse.fact_events(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_venue ON warehouse.fact_events(venue_sk);
CREATE INDEX IF NOT EXISTS idx_fact_events_category ON warehouse.fact_events(category_sk);
CREATE INDEX IF NOT EXISTS idx_fact_events_status ON warehouse.fact_events(event_status);
