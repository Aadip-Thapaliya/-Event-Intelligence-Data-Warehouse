"""
scripts/scheduler.py
====================
Runs the ELT pipeline on a schedule.
Uses the lightweight `schedule` library — no Airflow needed for MVP.

Schedules:
  - Full pipeline: every day at 06:00
  - Quality checks only: every hour

Usage:
  python scripts/scheduler.py

For production, replace with cron, Airflow, or Make (Integromat) trigger.
"""

import schedule
import time
import subprocess
import sys
from loguru import logger
from datetime import datetime


def run_full_pipeline():
    logger.info(f"⏰ Scheduled run starting at {datetime.now()}")
    result = subprocess.run(
        [sys.executable, "scripts/run_pipeline.py", "--source", "api"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        logger.success("Scheduled pipeline run completed successfully")
    else:
        logger.error(f"Scheduled pipeline run failed:\n{result.stderr}")


def run_quality_only():
    logger.info(f"🔍 Quality check run at {datetime.now()}")
    result = subprocess.run(
        [sys.executable, "scripts/run_pipeline.py", "--skip-ingest"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        logger.success("Quality check completed")
    else:
        logger.error(f"Quality check failed:\n{result.stderr}")


# ── Schedule ───────────────────────────────────────────────────────────────────

# Full pipeline once per day at 06:00
schedule.every().day.at("06:00").do(run_full_pipeline)

# Quality checks every hour
schedule.every().hour.do(run_quality_only)

logger.info("📅 Scheduler started")
logger.info("   Full pipeline: daily at 06:00")
logger.info("   Quality checks: every hour")
logger.info("   Press Ctrl+C to stop")

while True:
    schedule.run_pending()
    time.sleep(60)
