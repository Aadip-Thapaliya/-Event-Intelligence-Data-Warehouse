"""
scripts/run_pipeline.py
=======================
Orchestrates the full ELT pipeline:

  1. EXTRACT  → Pull from Ticketmaster API (or CSV fallback)
  2. LOAD     → Land raw data in PostgreSQL raw schema
  3. TRANSFORM → Clean, normalise, build dimensions
  4. LOAD FACTS → Populate fact_events star schema table
  5. QUALITY  → Run automated data quality checks
  6. REPORT   → Log summary to console

Usage:
  python scripts/run_pipeline.py                    # API mode
  python scripts/run_pipeline.py --source csv       # CSV fallback mode
  python scripts/run_pipeline.py --source csv --generate-sample
"""

import os
import sys
import argparse
import psycopg2
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

# Make imports work from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from api_ingestor import TicketmasterIngestor
from csv_ingestor import CsvIngestor, generate_sample_csv

load_dotenv()

# ── Helpers ────────────────────────────────────────────────────────────────────

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


def run_sql_file(conn, sql_path: str):
    """Execute a .sql file against the connected database."""
    with open(sql_path, "r") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    logger.info(f"Executed: {sql_path}")


def create_transform_schema(conn):
    """Ensure transform schema exists (needed for staging views)."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS transform;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS reporting;")
    conn.commit()


def print_pipeline_summary(conn):
    """Print a quick summary of warehouse state after pipeline run."""
    queries = {
        "Total raw events":         "SELECT COUNT(*) FROM raw.events",
        "Unprocessed raw events":   "SELECT COUNT(*) FROM raw.events WHERE is_processed = FALSE",
        "Fact events loaded":       "SELECT COUNT(*) FROM warehouse.fact_events",
        "Unique venues":            "SELECT COUNT(*) FROM warehouse.dim_venue",
        "Unique categories":        "SELECT COUNT(*) FROM warehouse.dim_category",
        "Quality checks run":       "SELECT COUNT(*) FROM raw.quality_log",
        "Failed quality checks":    "SELECT COUNT(*) FROM raw.quality_log WHERE status = 'failed'",
    }
    logger.info("=" * 55)
    logger.info("  PIPELINE SUMMARY")
    logger.info("=" * 55)
    with conn.cursor() as cur:
        for label, query in queries.items():
            try:
                cur.execute(query)
                result = cur.fetchone()[0]
                logger.info(f"  {label:<30} {result:>10,}")
            except Exception as e:
                logger.warning(f"  {label:<30} ERROR: {e}")
    logger.info("=" * 55)


# ── Pipeline Steps ─────────────────────────────────────────────────────────────

def step_extract_load(source: str, csv_path: str = None) -> int:
    """Step 1-2: Extract from source and load to raw schema."""
    logger.info(f"▶ STEP 1-2: Extract & Load | source={source}")

    if source == "api":
        ingestor = TicketmasterIngestor()
        return ingestor.run(country_code="DE", max_pages=5)
    elif source == "csv":
        ingestor = CsvIngestor(csv_path)
        return ingestor.run()
    else:
        raise ValueError(f"Unknown source: {source}")


def step_transform(conn):
    """Step 3: Clean data and build dimension tables."""
    logger.info("▶ STEP 3: Transform & build dimensions")
    base = Path(__file__).parent

    create_transform_schema(conn)
    run_sql_file(conn, base / "transform" / "transform_events.sql")
    logger.success("Transform complete")


def step_load_facts(conn):
    """Step 4: Populate fact_events table."""
    logger.info("▶ STEP 4: Load fact table")
    base = Path(__file__).parent
    run_sql_file(conn, base / "files" / "load_facts.sql")
    logger.success("Fact load complete")


def step_quality_checks(conn):
    """Step 5: Run automated data quality checks."""
    logger.info("▶ STEP 5: Data quality checks")
    base = Path(__file__).parent
    run_sql_file(conn, base / "files" / "data_quality.sql")
    logger.success("Quality checks complete")


def step_create_kpi_views(conn):
    """Step 6: Create/refresh KPI reporting views."""
    logger.info("▶ STEP 6: Create KPI views")
    base = Path(__file__).parent
    run_sql_file(conn, base / "kpi_queries.sql")
    logger.success("KPI views ready")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Event Warehouse ELT Pipeline")
    parser.add_argument("--source", choices=["api", "csv"], default="api",
                        help="Data source: api (Ticketmaster) or csv (local file)")
    parser.add_argument("--csv-path", default="sample_events.csv",
                        help="Path to CSV file (only used with --source csv)")
    parser.add_argument("--generate-sample", action="store_true",
                        help="Generate synthetic CSV data before loading")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip ingestion and only run transform/load steps")
    args = parser.parse_args()

    logger.info("🎉 Event Intelligence Data Warehouse — Pipeline Starting")
    logger.info(f"   Source: {args.source}")

    # Generate sample CSV if requested
    if args.generate_sample:
        args.csv_path = generate_sample_csv(args.csv_path)

    # Step 1-2: Extract & Load
    if not args.skip_ingest:
        records = step_extract_load(args.source, args.csv_path)
        logger.info(f"   Records ingested: {records}")

    # Steps 3-6: Transform, Load, Quality, KPIs
    conn = get_db_connection()
    try:
        step_transform(conn)
        step_load_facts(conn)
        step_quality_checks(conn)
        step_create_kpi_views(conn)
        print_pipeline_summary(conn)
        logger.success("✅ Pipeline completed successfully!")
    except Exception as e:
        logger.error(f"Pipeline failed at: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
