# Data Model Documentation

## Overview

This warehouse uses a **star schema** — optimised for analytical queries and BI tool integration.

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │─────────────────│
                    │ date_id (PK)    │
                    │ full_date       │
                    │ day_of_week     │
                    │ week_number     │
                    │ month_name      │
                    │ quarter         │
                    │ year            │
                    │ is_weekend      │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│   dim_venue     │          │          │  dim_category   │
│─────────────────│          │          │─────────────────│
│ venue_sk (PK)   │          │          │ category_sk (PK)│
│ venue_nk        │          │          │ category_name   │
│ venue_name      │◄─────────┼─────────►│ subcategory     │
│ city            │          │          └─────────────────┘
│ country         │    ┌─────┴──────┐
│ latitude        │    │ fact_events│
│ longitude       │    │────────────│
│ is_current (SCD)│    │ event_sk   │
└─────────────────┘    │ event_nk   │
                        │ date_id    │   ┌─────────────────┐
                        │ venue_sk   │   │   dim_source    │
                        │ category_sk│   │─────────────────│
                        │ source_sk  │──►│ source_sk (PK)  │
                        │ event_name │   │ source_name     │
                        │ event_url  │   │ source_type     │
                        │ event_status│  └─────────────────┘
                        │ price_min  │
                        │ price_max  │
                        │ price_avg  │
                        │ price_min_eur│
                        │ currency   │
                        └────────────┘
```

## Layer Architecture

| Layer | Schema | Purpose |
|-------|--------|---------|
| Landing | `raw` | Raw data exactly as received from source |
| Staging | `transform` | SQL views that clean and normalise raw data |
| Warehouse | `warehouse` | Star schema: dimensions + facts |
| Reporting | `reporting` | KPI views for BI tools / Metabase |

## Key Design Decisions

**SCD Type 2 on dim_venue**
Venues can change names or relocate. The `valid_from / valid_to / is_current` columns allow historical accuracy — a fact row always points to the venue as it was at event time.

**Deduplication via ON CONFLICT**
Both raw ingestion and fact loading use `ON CONFLICT DO NOTHING / DO UPDATE` to make the pipeline idempotent. Re-running it won't create duplicates.

**Price normalisation to EUR**
`price_min_eur` provides a common currency for cross-market KPIs. In production, this would reference a live FX rate table.

**Quality log**
Every pipeline run logs pass/fail rates for 5 checks. This gives stakeholders visibility into data health without needing a separate monitoring tool.

## KPI Views Available

| View | Description |
|------|-------------|
| `reporting.kpi_events_by_category` | Event volumes and pricing by category |
| `reporting.kpi_events_by_city` | Geographic distribution |
| `reporting.kpi_monthly_trend` | Month-over-month event trends |
| `reporting.kpi_weekend_vs_weekday` | Day-type split |
| `reporting.kpi_pipeline_health` | Pipeline run success rates |
| `reporting.kpi_data_quality` | Quality check history |
