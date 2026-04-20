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
import sys
import time
from datetime import datetime
from zipfile import ZipFile

import requests
from tqdm import tqdm

from scripts.db_utils import check_processing_log, get_db_connection, log_processing_result
from scripts.precipitation import load_precipitation_cache, get_median_precipitation_cached
from scripts.sbb_parser import parse_sbb_csv

# Configuration
BASE_URL = "https://archive.opentransportdata.swiss/istdaten"
RAW_DIR = "raw/sbb"
LOGS_DIR = "logs"
TARGET_STATIONS = {"Zürich HB", "Basel SBB", "Bern"}
MIN_DISK_SPACE_GB = 10

# Create logs directory
os.makedirs(LOGS_DIR, exist_ok=True)

# Setup logging to file and console
log_filename = os.path.join(LOGS_DIR, f"collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def check_disk_space(path: str = ".") -> float:
    """Check available disk space in GB."""
    stat = shutil.disk_usage(path)
    return stat.free / (1024**3)


def download_month(year: int, month: int, max_retries: int = 3):
    """Download SBB monthly ZIP archive with retry logic.

    Retries up to 3 times with exponential backoff: 10s, 30s, 90s.
    Deletes partial ZIPs before retrying.
    Returns binary data on success, None on failure.
    """
    url = f"{BASE_URL}/{year}/ist-daten-{year:04d}-{month:02d}.zip"
    period = f"{year:04d}-{month:02d}"
    backoff_times = [10, 30, 90]

    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading {period} (attempt {attempt + 1}/{max_retries})")
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))
            chunk_size = 8192

            with tqdm(total=total_size, unit="B", unit_scale=True, desc=f"Downloading {period}") as pbar:
                data = io.BytesIO()
                for chunk in response.iter_content(chunk_size):
                    data.write(chunk)
                    pbar.update(len(chunk))

            return data.getvalue()

        except requests.RequestException as e:
            logger.error(f"Download attempt {attempt + 1}/{max_retries} failed for {period}: {e}")

            if attempt < max_retries - 1:
                wait_time = backoff_times[attempt]
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} download attempts failed for {period}")
                return None

    return None


def process_month(year: int, month: int, debug: bool = False) -> int:
    """Download and process a single month of SBB data.

    Opens a fresh DB connection for this month to avoid timeout issues
    on long-running operations.
    """
    period = f"{year:04d}-{month:02d}"

    # Get a fresh connection for this month
    conn = get_db_connection()

    try:
        # Check if already processed
        if check_processing_log(conn, "sbb", period):
            logger.info(f"Skipping {period}: already processed")
            return 0

        # Check disk space before download
        available_gb = check_disk_space(RAW_DIR if os.path.exists(RAW_DIR) else ".")
        if available_gb < MIN_DISK_SPACE_GB:
            error_msg = f"Insufficient disk space: {available_gb:.2f} GB available, {MIN_DISK_SPACE_GB} GB required"
            logger.error(error_msg)
            log_processing_result(conn, "sbb", period, "error", error_msg=error_msg)
            return 0

        # Create output directory
        os.makedirs(RAW_DIR, exist_ok=True)

        # Download ZIP with retry logic
        zip_data = download_month(year, month)
        if zip_data is None:
            log_processing_result(conn, "sbb", period, "error", error_msg="Download failed after all retries")
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

    finally:
        # Always close the connection for this month
        conn.close()


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
    logger.info(f"Log file: {log_filename}")

    try:
        total_rows = 0

        with tqdm(total=len(year_months), desc="Months", position=0, leave=True) as months_bar:
            for year, month in year_months:
                # Each month opens its own connection (no reuse across months)
                rows = process_month(year, month, debug=args.debug)
                total_rows += rows
                months_bar.update(1)

        logger.info(f"---\nTotal rows inserted: {total_rows}")
        return 0

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

