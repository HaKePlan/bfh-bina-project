"""Precipitation data enrichment utilities."""

import logging

import psycopg2

logger = logging.getLogger(__name__)


def load_precipitation_cache(conn):
    """
    Load all precipitation data into memory for fast lookup.
    Returns a dict: city -> list of (measured_at, precip_mm) tuples.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT city, measured_at, precip_mm
            FROM precipitation_10min
            ORDER BY city, measured_at
        """)

        rows = cursor.fetchall()
        cursor.close()

        # Build cache: city -> sorted list of (measured_at, precip_mm)
        cache = {}
        for city, measured_at, precip_mm in rows:
            if city not in cache:
                cache[city] = []
            cache[city].append((measured_at, precip_mm))

        logger.info(f"Loaded precipitation cache: {sum(len(v) for v in cache.values())} records for {len(cache)} cities")
        return cache

    except psycopg2.Error as e:
        logger.error(f"Failed to load precipitation cache: {e}")
        return {}


def get_median_precipitation_cached(precip_cache, city: str, start_time, end_time):
    """
    Calculate median precipitation from in-memory cache.
    Much faster than querying the database for every record.
    """
    if start_time is None or end_time is None:
        return None

    if city not in precip_cache:
        return None

    precip_list = precip_cache[city]

    # Find all records in the window
    precip_values = [
        precip_mm
        for measured_at, precip_mm in precip_list
        if start_time <= measured_at <= end_time and precip_mm is not None
    ]

    if not precip_values:
        # All values are NULL, try forward-fill and backward-fill
        # Forward fill: nearest value after end_time
        forward_value = None
        for measured_at, precip_mm in precip_list:
            if measured_at > end_time and precip_mm is not None:
                forward_value = precip_mm
                break

        if forward_value is not None:
            return forward_value

        # Backward fill: nearest value before start_time
        backward_value = None
        for measured_at, precip_mm in reversed(precip_list):
            if measured_at < start_time and precip_mm is not None:
                backward_value = precip_mm
                break

        return backward_value

    # Calculate median
    precip_values.sort()
    n = len(precip_values)
    if n % 2 == 1:
        return precip_values[n // 2]
    else:
        return (precip_values[n // 2 - 1] + precip_values[n // 2]) / 2

