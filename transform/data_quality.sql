-- ============================================================
-- transform/data_quality.sql
-- Runs automated data quality checks on raw and warehouse layers.
-- Results are logged to raw.quality_log.
-- ============================================================

-- -----------------------------------------------
-- CHECK 1: Null event names (critical field)
-- -----------------------------------------------
INSERT INTO raw.quality_log (check_name, table_name, records_checked, records_failed, pass_rate, status)
SELECT
    'null_event_names'                              AS check_name,
    'raw.events'                                    AS table_name,
    COUNT(*)                                        AS records_checked,
    COUNT(*) FILTER (WHERE event_name IS NULL)      AS records_failed,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE event_name IS NOT NULL) / NULLIF(COUNT(*), 0), 2
    )                                               AS pass_rate,
    CASE
        WHEN COUNT(*) FILTER (WHERE event_name IS NULL) = 0 THEN 'passed'
        WHEN COUNT(*) FILTER (WHERE event_name IS NULL)::FLOAT / COUNT(*) < 0.05 THEN 'warning'
        ELSE 'failed'
    END                                             AS status
FROM raw.events;


-- -----------------------------------------------
-- CHECK 2: Invalid date formats
-- -----------------------------------------------
INSERT INTO raw.quality_log (check_name, table_name, records_checked, records_failed, pass_rate, status)
SELECT
    'invalid_event_dates',
    'raw.events',
    COUNT(*),
    COUNT(*) FILTER (WHERE event_date !~ '^\d{4}-\d{2}-\d{2}$' OR event_date IS NULL),
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE event_date ~ '^\d{4}-\d{2}-\d{2}$') / NULLIF(COUNT(*), 0), 2
    ),
    CASE
        WHEN COUNT(*) FILTER (WHERE event_date !~ '^\d{4}-\d{2}-\d{2}$') = 0 THEN 'passed'
        WHEN COUNT(*) FILTER (WHERE event_date !~ '^\d{4}-\d{2}-\d{2}$')::FLOAT / COUNT(*) < 0.10 THEN 'warning'
        ELSE 'failed'
    END
FROM raw.events;


-- -----------------------------------------------
-- CHECK 3: Price consistency (min <= max)
-- -----------------------------------------------
INSERT INTO raw.quality_log (check_name, table_name, records_checked, records_failed, pass_rate, status)
SELECT
    'price_min_gt_max',
    'raw.events',
    COUNT(*) FILTER (WHERE price_min ~ '^\d+' AND price_max ~ '^\d+'),
    COUNT(*) FILTER (
        WHERE price_min ~ '^\d+' AND price_max ~ '^\d+'
        AND price_min::NUMERIC > price_max::NUMERIC
    ),
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE price_min ~ '^\d+' AND price_max ~ '^\d+'
            AND price_min::NUMERIC <= price_max::NUMERIC
        ) / NULLIF(COUNT(*) FILTER (WHERE price_min ~ '^\d+' AND price_max ~ '^\d+'), 0), 2
    ),
    CASE
        WHEN COUNT(*) FILTER (
            WHERE price_min ~ '^\d+' AND price_max ~ '^\d+'
            AND price_min::NUMERIC > price_max::NUMERIC
        ) = 0 THEN 'passed'
        ELSE 'warning'
    END
FROM raw.events;


-- -----------------------------------------------
-- CHECK 4: Duplicate event IDs per source
-- -----------------------------------------------
INSERT INTO raw.quality_log (check_name, table_name, records_checked, records_failed, pass_rate, status)
WITH dupes AS (
    SELECT source, raw_event_id, COUNT(*) AS cnt
    FROM raw.events
    GROUP BY source, raw_event_id
    HAVING COUNT(*) > 1
)
SELECT
    'duplicate_event_ids',
    'raw.events',
    (SELECT COUNT(*) FROM raw.events),
    COALESCE(SUM(cnt), 0),
    ROUND(100.0 - (100.0 * COALESCE(SUM(cnt), 0) / NULLIF((SELECT COUNT(*) FROM raw.events), 0)), 2),
    CASE WHEN COUNT(*) = 0 THEN 'passed' ELSE 'warning' END
FROM dupes;


-- -----------------------------------------------
-- CHECK 5: Orphan facts (missing dimension keys)
-- -----------------------------------------------
INSERT INTO raw.quality_log (check_name, table_name, records_checked, records_failed, pass_rate, status)
SELECT
    'orphan_fact_records',
    'warehouse.fact_events',
    COUNT(*),
    COUNT(*) FILTER (WHERE date_id IS NULL OR venue_sk IS NULL OR category_sk IS NULL),
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE date_id IS NOT NULL AND venue_sk IS NOT NULL AND category_sk IS NOT NULL)
        / NULLIF(COUNT(*), 0), 2
    ),
    CASE
        WHEN COUNT(*) FILTER (WHERE date_id IS NULL OR venue_sk IS NULL OR category_sk IS NULL) = 0
        THEN 'passed'
        ELSE 'warning'
    END
FROM warehouse.fact_events;


-- -----------------------------------------------
-- Quality summary report (run after checks)
-- -----------------------------------------------
SELECT
    check_name,
    table_name,
    records_checked,
    records_failed,
    pass_rate || '%'    AS pass_rate,
    status,
    checked_at
FROM raw.quality_log
ORDER BY checked_at DESC
LIMIT 20;
