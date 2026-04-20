"""SBB CSV parsing and data transformation utilities."""

import io
import logging
from datetime import datetime
from zipfile import ZipFile

import pandas as pd

logger = logging.getLogger(__name__)

# Target stations and mappings
TARGET_STATIONS = {"Zürich HB", "Basel SBB", "Bern"}
STATION_TO_CITY = {
    "Zürich HB": "Zürich",
    "Basel SBB": "Basel",
    "Bern": "Bern",
}


def parse_sbb_datetime(date_str: str, time_str: str) -> datetime:
    """
    Parse SBB date and time strings.
    Handles both formats:
    - Separate date and time: date_str=DD.MM.YYYY, time_str=HH:MM
    - Combined datetime: time_str=DD.MM.YYYY HH:MM
    
    Returns datetime in UTC (SBB uses Swiss local time, convert to UTC).
    Note: For this version, we store as naive datetime.
    """
    try:
        # Check if time_str contains a full datetime (with date)
        if " " in time_str and len(time_str) > 5:
            # time_str is actually a full datetime: DD.MM.YYYY HH:MM
            return datetime.strptime(time_str, "%d.%m.%Y %H:%M")
        else:
            # time_str is just HH:MM, combine with date_str
            date_part = datetime.strptime(date_str, "%d.%m.%Y").date()
            time_part = datetime.strptime(time_str, "%H:%M").time()
            return datetime.combine(date_part, time_part)
    except ValueError as e:
        logger.error(f"Failed to parse date/time: {date_str} {time_str}: {e}")
        raise


def _build_trip_origins_map(daily_df: pd.DataFrame) -> dict:
    """
    Build a dict mapping fahrt_bezeichner -> origin info for all trips in a daily CSV.
    This is much faster than looking up each trip individually.
    """
    origins = {}
    
    # Group by trip ID
    for fahrt_id, trip_df in daily_df.groupby("FAHRT_BEZEICHNER"):
        # Find first stop (no ANKUNFTSZEIT)
        origin_rows = trip_df[trip_df["ANKUNFTSZEIT"].isna()]
        
        if origin_rows.empty:
            origin_row = trip_df.iloc[0]
        else:
            origin_row = origin_rows.iloc[0]
        
        try:
            origin_station = origin_row.get("HALTESTELLEN_NAME")
            origin_departure_str = origin_row.get("ABFAHRTSZEIT")
            
            if pd.isna(origin_departure_str):
                origins[fahrt_id] = {
                    "origin_station": origin_station,
                    "origin_departure_scheduled": None
                }
            else:
                origin_departure_scheduled = parse_sbb_datetime(
                    origin_row["BETRIEBSTAG"],
                    origin_departure_str
                )
                origins[fahrt_id] = {
                    "origin_station": origin_station,
                    "origin_departure_scheduled": origin_departure_scheduled
                }
        except Exception as e:
            logger.warning(f"Failed to find trip origin for {fahrt_id}: {e}")
            origins[fahrt_id] = {
                "origin_station": None,
                "origin_departure_scheduled": None
            }
    
    return origins


def parse_sbb_csv(csv_content: str, source_month: str) -> list:
    """
    Parse SBB daily CSV and return list of qualifying train arrivals.

    Filters:
    - PRODUKT_ID == 'Zug'
    - AN_PROGNOSE_STATUS == 'REAL'
    - HALTESTELLEN_NAME in TARGET_STATIONS
    - FAELLT_AUS_TF == 'false'

    Returns list of dicts with parsed data, enriched with trip origin and duration.
    """
    try:
        df = pd.read_csv(io.StringIO(csv_content), sep=";", dtype=str)
    except Exception as e:
        logger.error(f"Failed to parse CSV: {e}")
        return []

    # Build trip origins map (once, for all trips in the day)
    trip_origins = _build_trip_origins_map(df)

    # Filter rows
    df = df[df["PRODUKT_ID"] == "Zug"]
    df = df[df["AN_PROGNOSE_STATUS"] == "REAL"]
    df = df[df["HALTESTELLEN_NAME"].isin(TARGET_STATIONS)]
    df = df[df["FAELLT_AUS_TF"] == "false"]

    # Drop rows without ANKUNFTSZEIT (origin stops)
    df = df[df["ANKUNFTSZEIT"].notna()]

    results = []
    for _, row in df.iterrows():
        try:
            betriebstag = datetime.strptime(row["BETRIEBSTAG"], "%d.%m.%Y").date()
            scheduled_arrival = parse_sbb_datetime(row["BETRIEBSTAG"], row["ANKUNFTSZEIT"])
            actual_arrival = datetime.strptime(row["AN_PROGNOSE"], "%d.%m.%Y %H:%M:%S")

            arrival_delay_min = (actual_arrival - scheduled_arrival).total_seconds() / 60

            # Look up trip origin from pre-computed map
            fahrt_bezeichner = row["FAHRT_BEZEICHNER"]
            origin_info = trip_origins.get(fahrt_bezeichner, {
                "origin_station": None,
                "origin_departure_scheduled": None
            })

            # Compute trip_duration_min if both times are available
            trip_duration_min = None
            if origin_info["origin_departure_scheduled"] is not None:
                trip_duration_min = (scheduled_arrival - origin_info["origin_departure_scheduled"]).total_seconds() / 60

            results.append({
                "betriebstag": betriebstag,
                "fahrt_bezeichner": fahrt_bezeichner,
                "destination_station": row["HALTESTELLEN_NAME"],
                "destination_city": STATION_TO_CITY[row["HALTESTELLEN_NAME"]],
                "scheduled_arrival": scheduled_arrival,
                "actual_arrival": actual_arrival,
                "arrival_delay_min": arrival_delay_min,
                "origin_station": origin_info["origin_station"],
                "origin_departure_scheduled": origin_info["origin_departure_scheduled"],
                "trip_duration_min": trip_duration_min,
                "source_month": source_month,
            })
        except Exception as e:
            logger.warning(f"Failed to parse row: {e}")
            continue

    return results


def find_trip_origin(daily_df: pd.DataFrame, fahrt_bezeichner: str, destination_row_idx: int) -> dict:
    """
    Find the origin of a trip by looking for the stop with no ANKUNFTSZEIT
    or earliest ABFAHRTSZEIT.

    Returns dict with origin_station, origin_departure_scheduled, trip_duration_min.
    """
    trip_rows = daily_df[daily_df["FAHRT_BEZEICHNER"] == fahrt_bezeichner].copy()

    if trip_rows.empty:
        return {"origin_station": None, "origin_departure_scheduled": None, "trip_duration_min": None}

    # Find first stop (no ANKUNFTSZEIT)
    origin_rows = trip_rows[trip_rows["ANKUNFTSZEIT"].isna()]

    if origin_rows.empty:
        # No origin found, use first row
        origin_row = trip_rows.iloc[0]
    else:
        origin_row = origin_rows.iloc[0]

    try:
        origin_station = origin_row.get("HALTESTELLEN_NAME")
        origin_departure_str = origin_row.get("ABFAHRTSZEIT")

        if pd.isna(origin_departure_str):
            return {"origin_station": origin_station, "origin_departure_scheduled": None, "trip_duration_min": None}

        origin_departure_scheduled = parse_sbb_datetime(
            origin_row["BETRIEBSTAG"],
            origin_departure_str
        )

        return {
            "origin_station": origin_station,
            "origin_departure_scheduled": origin_departure_scheduled,
            "trip_duration_min": None,  # Will be computed later
        }
    except Exception as e:
        logger.warning(f"Failed to find trip origin: {e}")
        return {"origin_station": None, "origin_departure_scheduled": None, "trip_duration_min": None}


def extract_csv_from_zip(zip_bytes: bytes, betriebstag: str) -> str:
    """
    Extract specific daily CSV from ZIP archive.
    betriebstag format: YYYY-MM-DD
    """
    try:
        with ZipFile(io.BytesIO(zip_bytes)) as zf:
            # Look for CSV matching the date
            filename = f"{betriebstag}_istdaten.csv"
            if filename in zf.namelist():
                return zf.read(filename).decode("utf-8")
            else:
                return None
    except Exception as e:
        logger.error(f"Failed to extract CSV from ZIP: {e}")
        return None

