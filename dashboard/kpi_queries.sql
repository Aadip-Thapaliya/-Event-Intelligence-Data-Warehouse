-- ============================================================
-- dashboard/kpi_queries.sql
-- Business-facing KPI views ready for Metabase / BI tools.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS reporting;

-- -----------------------------------------------
-- KPI 1: Events by category (volume + avg price)
-- -----------------------------------------------
CREATE OR REPLACE VIEW reporting.kpi_events_by_category AS
SELECT
    c.category_name,
    c.subcategory,
    COUNT(f.event_sk)                           AS total_events,
    COUNT(f.event_sk) FILTER (
        WHERE f.event_status = 'onsale'
    )                                           AS events_on_sale,
    ROUND(AVG(f.price_avg), 2)                  AS avg_ticket_price_eur,
    ROUND(MIN(f.price_min_eur), 2)              AS cheapest_ticket_eur,
    ROUND(MAX(f.price_max), 2)                  AS most_expensive_ticket
FROM warehouse.fact_events f
JOIN warehouse.dim_category c ON c.category_sk = f.category_sk
WHERE f.is_deleted = FALSE
GROUP BY c.category_name, c.subcategory
ORDER BY total_events DESC;


-- -----------------------------------------------
-- KPI 2: Events by city
-- -----------------------------------------------
CREATE OR REPLACE VIEW reporting.kpi_events_by_city AS
SELECT
    v.city,
    v.country,
    COUNT(f.event_sk)                           AS total_events,
    COUNT(DISTINCT v.venue_sk)                  AS unique_venues,
    ROUND(AVG(f.price_avg), 2)                  AS avg_price_eur,
    COUNT(f.event_sk) FILTER (
        WHERE f.event_status = 'cancelled'
    )                                           AS cancelled_events
FROM warehouse.fact_events f
JOIN warehouse.dim_venue v ON v.venue_sk = f.venue_sk
WHERE f.is_deleted = FALSE
GROUP BY v.city, v.country
ORDER BY total_events DESC;


-- -----------------------------------------------
-- KPI 3: Monthly event trend
-- -----------------------------------------------
CREATE OR REPLACE VIEW reporting.kpi_monthly_trend AS
SELECT
    d.year,
    d.month_number,
    d.month_name,
    COUNT(f.event_sk)                           AS total_events,
    COUNT(f.event_sk) FILTER (
        WHERE f.event_status = 'onsale'
    )                                           AS active_events,
    ROUND(AVG(f.price_avg), 2)                  AS avg_price_eur,
    COUNT(DISTINCT f.venue_sk)                  AS unique_venues
FROM warehouse.fact_events f
JOIN warehouse.dim_date d ON d.date_id = f.date_id
WHERE f.is_deleted = FALSE
GROUP BY d.year, d.month_number, d.month_name
ORDER BY d.year, d.month_number;


-- -----------------------------------------------
-- KPI 4: Weekend vs weekday events
-- -----------------------------------------------
CREATE OR REPLACE VIEW reporting.kpi_weekend_vs_weekday AS
SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END    AS day_type,
    COUNT(f.event_sk)                                           AS total_events,
    ROUND(AVG(f.price_avg), 2)                                  AS avg_price_eur,
    ROUND(100.0 * COUNT(f.event_sk) / SUM(COUNT(f.event_sk)) OVER (), 1)
                                                                AS pct_of_total
FROM warehouse.fact_events f
JOIN warehouse.dim_date d ON d.date_id = f.date_id
WHERE f.is_deleted = FALSE
GROUP BY d.is_weekend;


-- -----------------------------------------------
-- KPI 5: Pipeline health dashboard
-- -----------------------------------------------
CREATE OR REPLACE VIEW reporting.kpi_pipeline_health AS
SELECT
    DATE_TRUNC('day', started_at)::DATE         AS run_date,
    source,
    COUNT(*)                                    AS total_runs,
    COUNT(*) FILTER (WHERE status = 'success')  AS successful_runs,
    COUNT(*) FILTER (WHERE status = 'failed')   AS failed_runs,
    SUM(records_loaded)                         AS total_records_loaded,
    ROUND(AVG(
        EXTRACT(EPOCH FROM (finished_at - started_at))
    ), 1)                                       AS avg_duration_seconds
FROM raw.ingestion_log
GROUP BY DATE_TRUNC('day', started_at)::DATE, source
ORDER BY run_date DESC;


-- -----------------------------------------------
-- KPI 6: Data quality summary
-- -----------------------------------------------
CREATE OR REPLACE VIEW reporting.kpi_data_quality AS
SELECT
    check_name,
    table_name,
    ROUND(AVG(pass_rate), 2)                    AS avg_pass_rate,
    COUNT(*) FILTER (WHERE status = 'failed')   AS times_failed,
    COUNT(*) FILTER (WHERE status = 'warning')  AS times_warned,
    MAX(checked_at)                             AS last_checked
FROM raw.quality_log
GROUP BY check_name, table_name
ORDER BY avg_pass_rate ASC;
