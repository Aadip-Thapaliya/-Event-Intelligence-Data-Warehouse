-- ============================================================
-- warehouse/load_facts.sql
-- Loads transformed data into the fact_events table.
-- Run after transform_events.sql completes.
-- ============================================================

-- -----------------------------------------------
-- STEP 6: Load fact_events
-- Join all dimension keys and insert/upsert rows
-- Deduplicate staging data to prevent ON CONFLICT errors
-- -----------------------------------------------
WITH deduped AS (
    SELECT DISTINCT ON (source, raw_event_id)
        *
    FROM transform.stg_events
    ORDER BY source, raw_event_id, ingested_at DESC
)
INSERT INTO warehouse.fact_events (
    event_nk,
    date_id,
    venue_sk,
    category_sk,
    source_sk,
    event_name,
    event_url,
    event_status,
    price_min,
    price_max,
    price_avg,
    currency,
    price_min_eur,
    ingested_at
)
SELECT
    -- Natural key: source + raw ID
    d.source || '_' || d.raw_event_id                      AS event_nk,

    -- Date FK
    date_dim.date_id,

    -- Venue FK (current version)
    venue_dim.venue_sk,

    -- Category FK
    category_dim.category_sk,

    -- Source FK
    source_dim.source_sk,

    -- Degenerate dimensions
    d.event_name,
    d.url                                                   AS event_url,
    d.event_status,

    -- Measures
    d.price_min,
    d.price_max,
    CASE
        WHEN d.price_min IS NOT NULL AND d.price_max IS NOT NULL
        THEN ROUND((d.price_min + d.price_max) / 2, 2)
        ELSE COALESCE(d.price_min, d.price_max)
    END                                                     AS price_avg,
    d.currency,

    -- EUR normalisation (simplified)
    CASE
        WHEN d.currency = 'EUR' THEN d.price_min
        WHEN d.currency = 'USD' THEN ROUND(d.price_min * 0.92, 2)
        WHEN d.currency = 'GBP' THEN ROUND(d.price_min * 1.17, 2)
        ELSE d.price_min
    END                                                     AS price_min_eur,

    d.ingested_at

FROM deduped d

-- Join dimensions
LEFT JOIN warehouse.dim_date date_dim
    ON date_dim.full_date = d.event_date

LEFT JOIN warehouse.dim_venue venue_dim
    ON venue_dim.venue_nk = MD5(COALESCE(d.venue_name,'') || COALESCE(d.venue_city,''))
    AND venue_dim.is_current = TRUE

LEFT JOIN warehouse.dim_category category_dim
    ON category_dim.category_name = d.category
    AND category_dim.subcategory = d.subcategory

LEFT JOIN warehouse.dim_source source_dim
    ON source_dim.source_name = d.source

ON CONFLICT (event_nk) DO UPDATE SET
    event_status    = EXCLUDED.event_status,
    price_min       = EXCLUDED.price_min,
    price_max       = EXCLUDED.price_max,
    price_avg       = EXCLUDED.price_avg,
    price_min_eur   = EXCLUDED.price_min_eur,
    loaded_at       = NOW();


-- -----------------------------------------------
-- STEP 7: Mark raw records as processed
-- -----------------------------------------------
UPDATE raw.events
SET is_processed = TRUE
WHERE is_processed = FALSE
  AND raw_event_id IN (
      SELECT SPLIT_PART(event_nk, '_', 2)
      FROM warehouse.fact_events
  );
