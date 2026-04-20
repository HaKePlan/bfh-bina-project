#!/usr/bin/env python3
"""
CLI script to download MeteoSwiss precipitation data and populate precipitation_10min.

Usage:
    python load_meteo.py [--debug]
"""

import argparse
import io
import logging
import os
import shutil
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

from scripts.db_utils import get_db_connection, log_processing_result

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
METEO_STATIONS = {
    "SMA": "Zürich",
    "BAS": "Basel",
    "BER": "Bern",
}

METEO_URLS = {
    "SMA": "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/sma/ogd-smn_sma_t_historical_2020-2029.csv",
    "BAS": "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/bas/ogd-smn_bas_t_historical_2020-2029.csv",
    "BER": "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ber/ogd-smn_ber_t_historical_2020-2029.csv",
}

RAW_DIR = "raw/meteo"
MIN_YEAR = 2024
MAX_YEAR = 2025


def download_station_data(station_abbr: str) -> bytes:
    """Download MeteoSwiss CSV for a station."""
    url = METEO_URLS.get(station_abbr)
    if not url:
        raise ValueError(f"Unknown station: {station_abbr}")

    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        chunk_size = 8192

        with tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc=f"Downloading {station_abbr}",
            leave=False
        ) as pbar:
            data = io.BytesIO()
            for chunk in response.iter_content(chunk_size):
                data.write(chunk)
                pbar.update(len(chunk))

        return data.getvalue()

    except requests.RequestException as e:
        logger.error(f"Failed to download {station_abbr}: {e}")
        raise


def parse_meteo_csv(csv_content: bytes, station_abbr: str) -> list:
    """
    Parse MeteoSwiss CSV and return list of dicts for insertion.

    Filters to years 2024-2025 and extracts:
    - reference_timestamp (DD.MM.YYYY HH:MM) → measured_at (UTC TIMESTAMP)
    - rre150z0 → precip_mm (10-minute precipitation in mm)
    """
    try:
        df = pd.read_csv(io.BytesIO(csv_content), sep=";", dtype=str)
    except Exception as e:
        logger.error(f"Failed to parse CSV for {station_abbr}: {e}")
        return []

    if "reference_timestamp" not in df.columns or "rre150z0" not in df.columns:
        logger.error(f"Missing required columns in {station_abbr} CSV")
        return []

    results = []
    city = METEO_STATIONS.get(station_abbr)

    for _, row in df.iterrows():
        try:
            # Parse timestamp: DD.MM.YYYY HH:MM
            ts_str = row["reference_timestamp"]
            measured_at = datetime.strptime(ts_str, "%d.%m.%Y %H:%M")

            # Filter by year
            if measured_at.year < MIN_YEAR or measured_at.year > MAX_YEAR:
                continue

            # Parse precipitation (nullable)
            precip_str = row["rre150z0"]
            precip_mm = None
            if pd.notna(precip_str) and precip_str.strip():
                try:
                    precip_mm = float(precip_str)
                except ValueError:
                    # Keep as None for invalid values
                    pass

            results.append({
                "station_abbr": station_abbr,
                "city": city,
                "measured_at": measured_at,
                "precip_mm": precip_mm,
            })

        except Exception as e:
            logger.warning(f"Failed to parse row in {station_abbr}: {e}")
            continue

    return results


def insert_precipitation_data(conn, station_abbr: str, records: list) -> int:
    """
    Insert precipitation records into precipitation_10min table.

    Uses upsert (INSERT ... ON CONFLICT) to handle duplicate timestamps.
    Returns count of actually inserted rows (conflicts don't count).
    """
    if not records:
        return 0

    try:
        cursor = conn.cursor()
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
            # Only count as inserted if rowcount is 1 (conflict returns 0)
            if cursor.rowcount > 0:
                rows_inserted += 1

        conn.commit()
        cursor.close()

        logger.info(f"Inserted {rows_inserted} rows for {station_abbr}")
        return rows_inserted

    except Exception as e:
        logger.error(f"Failed to insert data for {station_abbr}: {e}")
        return 0


def process_station(conn, station_abbr: str, debug: bool = False) -> int:
    """
    Download, parse, and insert precipitation data for a station.

    Returns count of inserted rows.
    """
    try:
        # Download
        logger.info(f"Downloading {station_abbr}...")
        csv_data = download_station_data(station_abbr)

        # Parse
        logger.info(f"Parsing {station_abbr}...")
        records = parse_meteo_csv(csv_data, station_abbr)

        if not records:
            logger.warning(f"No records parsed for {station_abbr}")
            log_processing_result(conn, "meteo", station_abbr, "success", rows_inserted=0)
            return 0

        # Insert
        logger.info(f"Inserting {station_abbr}...")
        rows_inserted = insert_precipitation_data(conn, station_abbr, records)

        # Log result
        log_processing_result(conn, "meteo", station_abbr, "success", rows_inserted=rows_inserted)

        return rows_inserted

    except Exception as e:
        logger.error(f"Failed to process {station_abbr}: {e}")
        log_processing_result(conn, "meteo", station_abbr, "error", error_msg=str(e))
        return 0


def main():
    """Main entry point for load_meteo.py."""
    parser = argparse.ArgumentParser(
        description="Download and load MeteoSwiss precipitation data."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Keep downloaded files for debugging"
    )

    args = parser.parse_args()

    logger.info("Starting MeteoSwiss precipitation data load")

    try:
        # Create output directory
        os.makedirs(RAW_DIR, exist_ok=True)

        conn = get_db_connection()
        total_rows = 0

        # Process each station
        for station_abbr in sorted(METEO_STATIONS.keys()):
            rows = process_station(conn, station_abbr, debug=args.debug)
            total_rows += rows

        # Print summary
        print(f"\n---\nTotal rows inserted: {total_rows}")

        conn.close()

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    finally:
        # Cleanup if not debug mode
        if not args.debug and os.path.exists(RAW_DIR):
            shutil.rmtree(RAW_DIR)


if __name__ == "__main__":
    import sys
    sys.exit(main())

