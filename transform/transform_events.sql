-- ============================================================
-- transform/transform_events.sql
-- Cleans raw event data and prepares it for the star schema.
-- Run after each ingestion batch.
-- ============================================================

-- -----------------------------------------------
-- STEP 0: Ensure constraints exist for ON CONFLICT
-- -----------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'dim_venue_nk_unique'
    ) THEN
        ALTER TABLE warehouse.dim_venue
        ADD CONSTRAINT dim_venue_nk_unique UNIQUE (venue_nk);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'dim_category_unique'
    ) THEN
        ALTER TABLE warehouse.dim_category
        ADD CONSTRAINT dim_category_unique UNIQUE (category_name, subcategory);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'dim_source_unique'
    ) THEN
        ALTER TABLE warehouse.dim_source
        ADD CONSTRAINT dim_source_unique UNIQUE (source_name);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fact_events_event_nk_key'
    ) THEN
        ALTER TABLE warehouse.fact_events
        ADD CONSTRAINT fact_events_event_nk_key UNIQUE (event_nk);
    END IF;
END$$;


-- -----------------------------------------------
-- STEP 1: Create staging view from raw data
-- -----------------------------------------------
CREATE OR REPLACE VIEW transform.stg_events AS
SELECT
    ingest_id,
    source,
    raw_event_id,
    TRIM(INITCAP(event_name)) AS event_name,
    CASE WHEN event_date ~ '^\d{4}-\d{2}-\d{2}$' THEN event_date::DATE ELSE NULL END AS event_date,
    CASE WHEN event_time ~ '^\d{2}:\d{2}' THEN event_time::TIME ELSE NULL END AS event_time,
    TRIM(venue_name) AS venue_name,
    TRIM(INITCAP(venue_city)) AS venue_city,
    TRIM(INITCAP(venue_country)) AS venue_country,
    venue_lat::NUMERIC(10,7) AS venue_lat,
    venue_lon::NUMERIC(10,7) AS venue_lon,
    COALESCE(NULLIF(TRIM(category), ''), 'Unknown') AS category,
    COALESCE(NULLIF(TRIM(subcategory), ''), 'General') AS subcategory,
    CASE WHEN price_min ~ '^\d+(\.\d+)?$' THEN price_min::NUMERIC(10,2) ELSE NULL END AS price_min,
    CASE WHEN price_max ~ '^\d+(\.\d+)?$' THEN price_max::NUMERIC(10,2) ELSE NULL END AS price_max,
    UPPER(TRIM(currency)) AS currency,
    url,
    LOWER(TRIM(status)) AS event_status,
    ingested_at
FROM raw.events
WHERE is_processed = FALSE;


-- -----------------------------------------------
-- STEP 2: Populate dim_date
-- -----------------------------------------------
INSERT INTO warehouse.dim_date (
    full_date, day_of_week, day_number, week_number,
    month_number, month_name, quarter, year, is_weekend
)
SELECT DISTINCT
    event_date,
    TO_CHAR(event_date, 'Day'),
    EXTRACT(DOW FROM event_date)::SMALLINT,
    EXTRACT(WEEK FROM event_date)::SMALLINT,
    EXTRACT(MONTH FROM event_date)::SMALLINT,
    TO_CHAR(event_date, 'Month'),
    EXTRACT(QUARTER FROM event_date)::SMALLINT,
    EXTRACT(YEAR FROM event_date)::SMALLINT,
    EXTRACT(DOW FROM event_date) IN (0,6)
FROM transform.stg_events
WHERE event_date IS NOT NULL
ON CONFLICT (full_date) DO NOTHING;


-- -----------------------------------------------
-- STEP 3: Upsert venues
-- -----------------------------------------------
INSERT INTO warehouse.dim_venue (
    venue_nk, venue_name, city, country, latitude, longitude
)
SELECT DISTINCT ON (venue_name, venue_city)
    MD5(COALESCE(venue_name,'') || COALESCE(venue_city,'')) AS venue_nk,
    venue_name,
    venue_city,
    venue_country,
    venue_lat,
    venue_lon
FROM transform.stg_events
WHERE venue_name IS NOT NULL
ON CONFLICT (venue_nk) DO NOTHING;


-- -----------------------------------------------
-- STEP 4: Upsert categories
-- -----------------------------------------------
INSERT INTO warehouse.dim_category (category_name, subcategory)
SELECT DISTINCT category, subcategory
FROM transform.stg_events
ON CONFLICT (category_name, subcategory) DO NOTHING;


-- -----------------------------------------------
-- STEP 5: Upsert sources
-- -----------------------------------------------
INSERT INTO warehouse.dim_source (source_name, source_type)
SELECT DISTINCT source,
    CASE source
        WHEN 'ticketmaster' THEN 'api'
        WHEN 'csv' THEN 'csv'
        ELSE 'unknown'
    END
FROM transform.stg_events
ON CONFLICT (source_name) DO NOTHING;


-- -----------------------------------------------
-- STEP 6: Upsert fact_events
-- -----------------------------------------------
INSERT INTO warehouse.fact_events (
    event_nk, date_id, venue_sk, category_sk, source_sk,
    event_name, event_url, event_status,
    price_min, price_max, price_avg, currency
)
SELECT
    MD5(source || raw_event_id) AS event_nk,
    d.date_id,
    v.venue_sk,
    c.category_sk,
    s.source_sk,
    st.event_name,
    st.url AS event_url,
    st.event_status,
    st.price_min,
    st.price_max,
    (st.price_min + st.price_max)/2 AS price_avg,
    st.currency
FROM transform.stg_events st
LEFT JOIN warehouse.dim_date d
    ON st.event_date = d.full_date
LEFT JOIN warehouse.dim_venue v
    ON MD5(COALESCE(st.venue_name,'') || COALESCE(st.venue_city,'')) = v.venue_nk
LEFT JOIN warehouse.dim_category c
    ON st.category = c.category_name AND st.subcategory = c.subcategory
LEFT JOIN warehouse.dim_source s
    ON st.source = s.source_name
ON CONFLICT (event_nk) DO NOTHING;
