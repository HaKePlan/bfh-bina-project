#!/usr/bin/env python3
"""
CLI script to download SBB monthly archives, extract daily CSVs, and populate train_connections.

Usage:
    python collect_sbb.py --start-year 2024 --end-year 2025 [--months 1,2,3] [--debug]
"""

import argparse
import io
import logging
import os
import shutil
from zipfile import ZipFile

import requests
from tqdm import tqdm

from scripts.db_utils import check_processing_log, get_db_connection, log_processing_result
from scripts.precipitation import load_precipitation_cache, get_median_precipitation_cached
from scripts.sbb_parser import parse_sbb_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://archive.opentransportdata.swiss/istdaten"
RAW_DIR = "raw/sbb"
TARGET_STATIONS = {"Zürich HB", "Basel SBB", "Bern"}


def download_month(year: int, month: int):
    """Download SBB monthly ZIP archive."""
    url = f"{BASE_URL}/{year}/ist-daten-{year:04d}-{month:02d}.zip"

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))
        chunk_size = 8192

        with tqdm(total=total_size, unit="B", unit_scale=True, desc=f"Downloading {year}-{month:02d}") as pbar:
            data = io.BytesIO()
            for chunk in response.iter_content(chunk_size):
                data.write(chunk)
                pbar.update(len(chunk))

        return data.getvalue()

    except requests.RequestException as e:
        logger.error(f"Failed to download {year}-{month:02d}: {e}")
        return None


def process_month(year: int, month: int, conn, debug: bool = False) -> int:
    """Download and process a single month of SBB data."""
    period = f"{year:04d}-{month:02d}"

    # Check if already processed
    if check_processing_log(conn, "sbb", period):
        logger.info(f"Skipping {period}: already processed")
        return 0

    # Create output directory
    os.makedirs(RAW_DIR, exist_ok=True)

    # Download ZIP
    logger.info(f"Downloading {period}...")
    zip_data = download_month(year, month)
    if zip_data is None:
        log_processing_result(conn, "sbb", period, "error", error_msg="Download failed")
        return 0

    try:
        all_records = []

        # Extract and process daily CSVs
        with ZipFile(io.BytesIO(zip_data)) as zf:
            csv_files = [f for f in zf.namelist() if f.endswith("_istdaten.csv")]

            for csv_file in tqdm(csv_files, desc=f"Processing {period}", leave=False):
                try:
                    csv_content = zf.read(csv_file).decode("utf-8")
                    records = parse_sbb_csv(csv_content, period)
                    all_records.extend(records)
                except Exception as e:
                    logger.warning(f"Failed to process {csv_file}: {e}")
                    continue

        # Load precipitation cache once (much faster than per-record queries)
        logger.info(f"Loading precipitation data for {period}...")
        precip_cache = load_precipitation_cache(conn)

        # Enrich with precipitation and insert
        if all_records:
            cursor = conn.cursor()
            rows_inserted = 0

            for record in all_records:
                try:
                    # Get median precipitation from cache (no DB query!)
                    median_precip = get_median_precipitation_cached(
                        precip_cache,
                        record["destination_city"],
                        record["origin_departure_scheduled"] if record.get("origin_departure_scheduled") else record["scheduled_arrival"],
                        record["scheduled_arrival"]
                    )

                    # Insert with upsert
                    cursor.execute("""
                        INSERT INTO train_connections (
                            betriebstag, fahrt_bezeichner, destination_station, destination_city,
                            scheduled_arrival, actual_arrival, arrival_delay_min,
                            origin_station, origin_departure_scheduled, trip_duration_min,
                            median_precip_mm, source_month
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (fahrt_bezeichner, scheduled_arrival) DO UPDATE SET
                            arrival_delay_min = EXCLUDED.arrival_delay_min,
                            median_precip_mm = EXCLUDED.median_precip_mm
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
                    rows_inserted += 1

                except Exception as e:
                    logger.warning(f"Failed to insert record: {e}")
                    continue

            conn.commit()
            cursor.close()

            log_processing_result(conn, "sbb", period, "success", rows_inserted=rows_inserted)
            logger.info(f"Processed {period}: {rows_inserted} records inserted")
            return rows_inserted
        else:
            log_processing_result(conn, "sbb", period, "success", rows_inserted=0)
            logger.info(f"Processed {period}: no records found")
            return 0

    except Exception as e:
        logger.error(f"Failed to process {period}: {e}")
        log_processing_result(conn, "sbb", period, "error", error_msg=str(e))
        return 0
    finally:
        # Cleanup if not debug mode
        if not debug and os.path.exists(RAW_DIR):
            shutil.rmtree(RAW_DIR)


def main():
    """Main entry point for collect_sbb.py."""
    parser = argparse.ArgumentParser(
        description="Download and process SBB train data for specified months and years."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        required=True,
        help="First year to process (e.g. 2024)"
    )
    parser.add_argument(
        "--end-year",
        type=int,
        required=True,
        help="Last year to process (e.g. 2025)"
    )
    parser.add_argument(
        "--months",
        type=str,
        help="Comma-separated months to process (e.g. 1,2,3). If omitted, all 12 months."
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Keep downloaded files for debugging"
    )

    args = parser.parse_args()

    # Parse months
    if args.months:
        months = [int(m.strip()) for m in args.months.split(",")]
    else:
        months = list(range(1, 13))

    # Generate year-month pairs
    year_months = []
    for year in range(args.start_year, args.end_year + 1):
        for month in months:
            year_months.append((year, month))

    logger.info(f"Processing {len(year_months)} month(s): {args.start_year}-{args.end_year}")

    try:
        conn = get_db_connection()
        total_rows = 0

        with tqdm(total=len(year_months), desc="Months", position=0, leave=True) as months_bar:
            for year, month in year_months:
                rows = process_month(year, month, conn, debug=args.debug)
                total_rows += rows
                months_bar.update(1)

        print(f"\n---\nTotal rows inserted: {total_rows}")
        conn.close()

        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

