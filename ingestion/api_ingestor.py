"""
ingestion/api_ingestor.py
=========================
Pulls event data from the Ticketmaster Discovery API and lands it
in the raw.events PostgreSQL table.

Ticketmaster API docs: https://developer.ticketmaster.com/products-and-docs/apis/discovery-api/v2/
Free tier: 5000 API calls/day — more than enough for dev/demo.
"""

import os
import json
import uuid
import requests
import psycopg2
import psycopg2.extras
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

load_dotenv()

# ── DB Connection ──────────────────────────────────────────────────────────────

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


# ── API Fetcher ────────────────────────────────────────────────────────────────

class TicketmasterIngestor:
    BASE_URL = "https://app.ticketmaster.com/discovery/v2/events.json"

    def __init__(self):
        self.api_key = os.getenv("TICKETMASTER_API_KEY")
        self.batch_size = int(os.getenv("BATCH_SIZE", 100))
        if not self.api_key:
            raise ValueError("TICKETMASTER_API_KEY not set in .env")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def fetch_events(self, country_code: str = "DE", page: int = 0) -> dict:
        """Fetch one page of events from Ticketmaster API."""
        params = {
            "apikey": self.api_key,
            "countryCode": country_code,
            "size": self.batch_size,
            "page": page,
            "sort": "date,asc"
        }
        response = requests.get(self.BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        return response.json()

    def parse_event(self, event: dict) -> dict:
        """Flatten a Ticketmaster event JSON into a clean row dict."""
        # Safely drill into nested JSON
        embedded = event.get("_embedded", {})
        venues = embedded.get("venues", [{}])
        venue = venues[0] if venues else {}
        classifications = event.get("classifications", [{}])
        classification = classifications[0] if classifications else {}
        price_ranges = event.get("priceRanges", [{}])
        price = price_ranges[0] if price_ranges else {}
        dates = event.get("dates", {}).get("start", {})
        location = venue.get("location", {})

        return {
            "source": "ticketmaster",
            "raw_event_id": event.get("id"),
            "raw_payload": json.dumps(event),
            "event_name": event.get("name"),
            "event_date": dates.get("localDate"),
            "event_time": dates.get("localTime"),
            "venue_name": venue.get("name"),
            "venue_city": venue.get("city", {}).get("name"),
            "venue_country": venue.get("country", {}).get("name"),
            "venue_lat": location.get("latitude"),
            "venue_lon": location.get("longitude"),
            "category": classification.get("segment", {}).get("name"),
            "subcategory": classification.get("genre", {}).get("name"),
            "price_min": str(price.get("min", "")),
            "price_max": str(price.get("max", "")),
            "currency": price.get("currency"),
            "url": event.get("url"),
            "status": event.get("dates", {}).get("status", {}).get("code"),
        }

    def load_to_raw(self, conn, rows: list[dict], run_id: str) -> int:
        """Bulk insert parsed rows into raw.events, skip duplicates."""
        if not rows:
            return 0

        insert_sql = """
            INSERT INTO raw.events (
                source, raw_event_id, raw_payload, event_name, event_date,
                event_time, venue_name, venue_city, venue_country,
                venue_lat, venue_lon, category, subcategory,
                price_min, price_max, currency, url, status
            ) VALUES (
                %(source)s, %(raw_event_id)s, %(raw_payload)s, %(event_name)s,
                %(event_date)s, %(event_time)s, %(venue_name)s, %(venue_city)s,
                %(venue_country)s, %(venue_lat)s, %(venue_lon)s, %(category)s,
                %(subcategory)s, %(price_min)s, %(price_max)s, %(currency)s,
                %(url)s, %(status)s
            )
            ON CONFLICT DO NOTHING
        """
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=50)
        conn.commit()
        return len(rows)

    def run(self, country_code: str = "DE", max_pages: int = 5):
        """Full ingestion run: fetch → parse → load."""
        run_id = str(uuid.uuid4())
        conn = get_db_connection()
        total_loaded = 0

        # Log start
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw.ingestion_log (run_id, source, status)
                VALUES (%s, 'ticketmaster', 'running')
            """, (run_id,))
        conn.commit()

        try:
            for page in range(max_pages):
                logger.info(f"Fetching page {page} | country={country_code}")
                data = self.fetch_events(country_code=country_code, page=page)

                events = data.get("_embedded", {}).get("events", [])
                if not events:
                    logger.info("No more events found, stopping early.")
                    break

                rows = [self.parse_event(e) for e in events]
                loaded = self.load_to_raw(conn, rows, run_id)
                total_loaded += loaded
                logger.success(f"Page {page}: loaded {loaded} records")

                # Check if we've hit the last page
                page_info = data.get("page", {})
                if page >= page_info.get("totalPages", 1) - 1:
                    break

            # Update log: success
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE raw.ingestion_log
                    SET finished_at = NOW(), records_loaded = %s, status = 'success'
                    WHERE run_id = %s
                """, (total_loaded, run_id))
            conn.commit()
            logger.success(f"Ingestion complete. Total records: {total_loaded}")

        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE raw.ingestion_log
                    SET finished_at = NOW(), status = 'failed', error_message = %s
                    WHERE run_id = %s
                """, (str(e), run_id))
            conn.commit()
            raise

        finally:
            conn.close()

        return total_loaded


if __name__ == "__main__":
    ingestor = TicketmasterIngestor()
    ingestor.run(country_code="DE", max_pages=5)
