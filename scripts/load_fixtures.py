#!/usr/bin/env python3
"""
Load fixture data into the database for development/testing.

Usage:
    python -m scripts.load_fixtures

This script:
1. Loads the MeteoSwiss fixture (Bern station historical data)
2. Loads the SBB fixture (one day of train data)
"""

import os
import shutil
import logging
from pathlib import Path

from scripts.db_utils import get_db_connection
from scripts.load_meteo import parse_meteo_csv
from scripts.sbb_parser import parse_sbb_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

FIXTURES_DIR = Path(__file__).parent.parent / "tests" / "fixtures"
METEO_FIXTURE = FIXTURES_DIR / "ogd-smn_ber_t_historical_2020-2029.csv"
SBB_FIXTURE = FIXTURES_DIR / "2024-01-01_istdaten.csv"


def load_meteo_fixture():
    """Load MeteoSwiss fixture data."""
    logger.info("Loading MeteoSwiss fixture...")

    if not METEO_FIXTURE.exists():
        logger.error(f"MeteoSwiss fixture not found: {METEO_FIXTURE}")
        return 0

    try:
        with open(METEO_FIXTURE, 'rb') as f:
            csv_content = f.read()

        conn = get_db_connection()
        cursor = conn.cursor()

        # Parse the fixture
        records = parse_meteo_csv(csv_content, "BER")

        if not records:
            logger.warning("No records parsed from MeteoSwiss fixture")
            conn.close()
            return 0

        # Insert records
        rows_inserted = 0
        for record in records:
            cursor.execute("""
                INSERT INTO precipitation_10min (station_abbr, city, measured_at, precip_mm)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (station_abbr, measured_at) DO NOTHING
            """, (
                record["station_abbr"],
                record["city"],
                record["measured_at"],
                record["precip_mm"]
            ))
            if cursor.rowcount > 0:
                rows_inserted += 1

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✓ Loaded {rows_inserted} MeteoSwiss records from fixture")
        return rows_inserted

    except Exception as e:
        logger.error(f"Failed to load MeteoSwiss fixture: {e}")
        return 0


def load_sbb_fixture():
    """Load SBB fixture data."""
    logger.info("Loading SBB fixture...")

    if not SBB_FIXTURE.exists():
        logger.error(f"SBB fixture not found: {SBB_FIXTURE}")
        return 0

    try:
        with open(SBB_FIXTURE, 'r', encoding='utf-8') as f:
            csv_content = f.read()

        conn = get_db_connection()
        cursor = conn.cursor()

        # Parse the fixture
        records = parse_sbb_csv(csv_content, "2024-01")

        if not records:
            logger.warning("No records parsed from SBB fixture")
            conn.close()
            return 0

        # Load precipitation cache for enrichment
        from scripts.precipitation import load_precipitation_cache, get_median_precipitation_cached
        precip_cache = load_precipitation_cache(conn)

        # Insert records with precipitation enrichment
        rows_inserted = 0
        for record in records:
            median_precip = get_median_precipitation_cached(
                precip_cache,
                record["destination_city"],
                record["origin_departure_scheduled"] if record.get("origin_departure_scheduled") else record["scheduled_arrival"],
                record["scheduled_arrival"]
            )

            cursor.execute("""
                INSERT INTO train_connections (
                    betriebstag, fahrt_bezeichner, destination_station, destination_city,
                    scheduled_arrival, actual_arrival, arrival_delay_min,
                    origin_station, origin_departure_scheduled, trip_duration_min,
                    median_precip_mm, source_month
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fahrt_bezeichner, scheduled_arrival) DO NOTHING
            """, (
                record["betriebstag"],
                record["fahrt_bezeichner"],
                record["destination_station"],
                record["destination_city"],
                record["scheduled_arrival"],
                record["actual_arrival"],
                record["arrival_delay_min"],
                record.get("origin_station"),
                record.get("origin_departure_scheduled"),
                record.get("trip_duration_min"),
                median_precip,
                record["source_month"]
            ))
            if cursor.rowcount > 0:
                rows_inserted += 1

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✓ Loaded {rows_inserted} SBB records from fixture")
        return rows_inserted

    except Exception as e:
        logger.error(f"Failed to load SBB fixture: {e}")
        return 0


def main():
    """Load all fixtures."""
    logger.info("=" * 60)
    logger.info("Loading fixture data for development")
    logger.info("=" * 60)

    meteo_rows = load_meteo_fixture()
    sbb_rows = load_sbb_fixture()

    logger.info("=" * 60)
    logger.info(f"Total rows loaded:")
    logger.info(f"  Precipitation: {meteo_rows}")
    logger.info(f"  Train connections: {sbb_rows}")
    logger.info("=" * 60)
    logger.info("✓ Fixture loading complete!")
    logger.info("You can now develop on this data")
    logger.info("=" * 60)


if __name__ == "__main__":
    import sys
    sys.exit(main())

