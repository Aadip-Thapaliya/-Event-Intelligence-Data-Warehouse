# 🎉 Event Intelligence Data Warehouse

A standalone ELT data warehouse that ingests live event data from the Ticketmaster API, transforms and models it using a star schema in PostgreSQL, runs automated data quality checks, and exposes KPI views ready for BI tools like Metabase.

Built as a portfolio project to demonstrate real-world data engineering skills: pipeline design, ETL/ELT, dimensional modelling, data governance, and warehouse architecture.

---

## Architecture

```
[ Ticketmaster API / CSV ]
          ↓
[ Ingestion Layer ]  →  raw.events  (PostgreSQL)
          ↓
[ Transform Layer ]  →  Staging views, dimension tables
          ↓
[ Warehouse Layer ]  →  Star schema (fact + dimensions)
          ↓
[ Reporting Layer ]  →  KPI views → Metabase / any BI tool
```

---

## Features

- **ELT Pipeline** — Extract from API, land raw data, transform in-database with SQL
- **Star Schema** — `fact_events` with 4 dimension tables (date, venue, category, source)
- **SCD Type 2** — Slowly Changing Dimensions on venue table for historical accuracy
- **Idempotent Loads** — `ON CONFLICT` logic means re-running never creates duplicates
- **Automated Data Quality** — 5 checks logged on every run (null fields, bad dates, price consistency, duplicates, orphan facts)
- **KPI Views** — 6 reporting views ready to plug into Metabase
- **Scheduler** — Daily pipeline runs + hourly quality checks
- **CSV Fallback** — Run fully offline with 500 synthetic events (no API key needed)
- **Dockerised** — One command spins up PostgreSQL 15 + pgAdmin

---

## Project Structure

```
event_warehouse/
├── ingestion/
│   ├── api_ingestor.py       # Ticketmaster API → raw.events
│   ├── csv_ingestor.py       # CSV fallback + synthetic data generator
│   └── schema_raw.sql        # Raw landing schema + ingestion/quality logs
├── transform/
│   ├── transform_events.sql  # Staging views, dimension population
│   └── data_quality.sql      # 5 automated quality checks
├── warehouse/
│   ├── schema_star.sql       # Star schema DDL (fact + 4 dims)
│   └── load_facts.sql        # Idempotent fact table load + mark processed
├── dashboard/
│   └── kpi_queries.sql       # 6 KPI views for reporting layer
├── scripts/
│   ├── run_pipeline.py       # Full ELT orchestrator (CLI)
│   └── scheduler.py          # Automated daily + hourly runs
├── docs/
│   └── data_model.md         # Schema diagrams + design decisions
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Database | PostgreSQL 15 |
| Ingestion | Python, Requests, Ticketmaster API |
| Transform | SQL (views, CTEs, window functions) |
| Orchestration | Python, Schedule |
| Infrastructure | Docker, pgAdmin |
| Quality | Custom SQL checks logged to DB |

---

## Quick Start

### Prerequisites
- Python 3.10+
- Docker Desktop

### 1. Clone the repo
```bash
git clone https://github.com/yourusername/event-warehouse.git
cd event-warehouse
```

### 2. Configure environment
```bash
cp .env.example .env
```
Edit `.env`:
```
DB_USER=warehouse_user
DB_PASSWORD=yourpassword
DB_NAME=event_warehouse
DB_HOST=localhost
DB_PORT=5432
TICKETMASTER_API_KEY=your_key_here   # optional
```

### 3. Start the database
```bash
docker-compose up -d
```

### 4. Install dependencies
```bash
pip install -r requirements.txt
```

### 5. Run the pipeline

**With Ticketmaster API (real data):**
```bash
python scripts/run_pipeline.py --source api
```

**Without API key (synthetic data, works instantly):**
```bash
python scripts/run_pipeline.py --source csv --generate-sample
```

### 6. Explore in pgAdmin
Open `http://localhost:5050` → login with `admin@event.com` / `admin`

---

## KPI Views Available

| View | Description |
|------|-------------|
| `reporting.kpi_events_by_category` | Volume + avg pricing by event category |
| `reporting.kpi_events_by_city` | Geographic distribution across cities |
| `reporting.kpi_monthly_trend` | Month-over-month event trends |
| `reporting.kpi_weekend_vs_weekday` | Day-type split with % of total |
| `reporting.kpi_pipeline_health` | Pipeline run success rates over time |
| `reporting.kpi_data_quality` | Quality check history + pass rates |

---

## Data Model

```
         dim_date          dim_category
            │                   │
            └────┬──────────────┘
                 │
            fact_events ──── dim_venue (SCD Type 2)
                 │
            dim_source
```

Full schema documentation → [`docs/data_model.md`](docs/data_model.md)

---

## CLI Options

```bash
python scripts/run_pipeline.py --help

  --source api          Pull from Ticketmaster API
  --source csv          Load from local CSV file
  --csv-path PATH       Path to CSV (default: sample_events.csv)
  --generate-sample     Auto-generate 500 synthetic events
  --skip-ingest         Run transform/load/quality steps only
```

---

## Roadmap

- [ ] dbt integration for transform layer
- [ ] Live FX rate table for currency normalisation
- [ ] Metabase dashboard screenshots
- [ ] Airflow DAG for production scheduling
- [ ] Additional API sources (Eventbrite, Meetup)

---

## Get a Free Ticketmaster API Key

1. Go to [developer.ticketmaster.com](https://developer.ticketmaster.com)
2. Click **Get Your API Key**
3. Sign up and copy your key into `.env`

Free tier: 5,000 calls/day — more than enough for development.

---

## Author

**Aadip Thapaliya**  
Data Science Student @ University of Europe for Applied Sciences  
[LinkedIn](https://www.linkedin.com/in/aadipthapaliya/) · [GitHub](https://github.com/Aadip)
