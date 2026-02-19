"""
ingestion/csv_ingestor.py
=========================
Loads event data from a CSV file into raw.events.
Use this for:
  - Offline development (no API key needed)
  - Backfilling historical data
  - Testing the pipeline with synthetic data

Expected CSV columns:
  event_id, event_name, event_date, event_time, venue_name,
  venue_city, venue_country, venue_lat, venue_lon,
  category, subcategory, price_min, price_max, currency, url, status
"""

import os
import uuid
import pandas as pd
import psycopg2
import psycopg2.extras
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", 5432),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


def generate_sample_csv(output_path: str = "sample_events.csv"):
    """Generate synthetic event data for testing when no API key available."""
    import random
    from datetime import datetime, timedelta

    categories = ["Music", "Sports", "Arts & Theatre", "Comedy", "Family"]
    cities = ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne", "Leipzig"]
    venues = {
        "Berlin": ("Mercedes-Benz Arena", 52.5024, 13.4413),
        "Munich": ("Olympiahalle", 48.1736, 11.5461),
        "Hamburg": ("Barclays Arena", 53.5876, 9.9014),
        "Frankfurt": ("Festhalle", 50.1109, 8.6569),
        "Cologne": ("Lanxess Arena", 50.9635, 6.9751),
        "Leipzig": ("Quarterback Immobilien Arena", 51.4189, 12.3915),
    }

    rows = []
    for i in range(500):
        city = random.choice(cities)
        venue_name, lat, lon = venues[city]
        event_date = datetime.today() + timedelta(days=random.randint(1, 180))
        price_min = round(random.uniform(15, 80), 2)
        price_max = round(price_min + random.uniform(10, 100), 2)
        rows.append({
            "event_id": f"MOCK_{i:05d}",
            "event_name": f"Event {i} - {random.choice(categories)} Night",
            "event_date": event_date.strftime("%Y-%m-%d"),
            "event_time": f"{random.randint(18,22):02d}:00:00",
            "venue_name": venue_name,
            "venue_city": city,
            "venue_country": "Germany",
            "venue_lat": lat + random.uniform(-0.001, 0.001),
            "venue_lon": lon + random.uniform(-0.001, 0.001),
            "category": random.choice(categories),
            "subcategory": "General",
            "price_min": price_min,
            "price_max": price_max,
            "currency": "EUR",
            "url": f"https://example.com/event/{i}",
            "status": random.choice(["onsale", "onsale", "onsale", "offsale", "cancelled"]),
        })

    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)
    logger.success(f"Sample CSV written to {output_path} ({len(df)} rows)")
    return output_path


class CsvIngestor:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path

    def run(self) -> int:
        run_id = str(uuid.uuid4())
        logger.info(f"Loading CSV: {self.csv_path}")

        df = pd.read_csv(self.csv_path)
        df = df.where(pd.notnull(df), None)  # Convert NaN → None for psycopg2

        rows = []
        for _, row in df.iterrows():
            rows.append({
                "source": "csv",
                "raw_event_id": str(row.get("event_id", "")),
                "raw_payload": None,
                "event_name": row.get("event_name"),
                "event_date": str(row.get("event_date", "")),
                "event_time": str(row.get("event_time", "")),
                "venue_name": row.get("venue_name"),
                "venue_city": row.get("venue_city"),
                "venue_country": row.get("venue_country"),
                "venue_lat": row.get("venue_lat"),
                "venue_lon": row.get("venue_lon"),
                "category": row.get("category"),
                "subcategory": row.get("subcategory"),
                "price_min": str(row.get("price_min", "")),
                "price_max": str(row.get("price_max", "")),
                "currency": row.get("currency"),
                "url": row.get("url"),
                "status": row.get("status"),
            })

        conn = get_db_connection()
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
            psycopg2.extras.execute_batch(cur, insert_sql, rows, page_size=100)

        # Log run
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO raw.ingestion_log
                    (run_id, source, records_fetched, records_loaded, status, finished_at)
                VALUES (%s, 'csv', %s, %s, 'success', NOW())
            """, (run_id, len(rows), len(rows)))

        conn.commit()
        conn.close()
        logger.success(f"CSV ingestion complete: {len(rows)} records loaded")
        return len(rows)


if __name__ == "__main__":
    # Generate sample data and load it
    path = generate_sample_csv("sample_events.csv")
    ingestor = CsvIngestor(path)
    ingestor.run()
