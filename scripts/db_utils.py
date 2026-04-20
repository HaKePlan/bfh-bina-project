"""Database connection and query utilities."""

import logging
import os
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


def get_db_connection():
    """Create and return a PostgreSQL database connection from .env credentials."""
    load_dotenv()

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")

    if not name:
        raise ValueError("DB_NAME environment variable is required.")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=name,
            user=user,
            password=password
        )
        logger.info(f"Connected to database '{name}' on {host}:{port}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


@contextmanager
def get_db_cursor(conn):
    """Context manager for database cursor."""
    cursor = conn.cursor()
    try:
        yield cursor
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        cursor.close()


def log_processing_result(conn, run_type, period, status, rows_inserted=None, error_msg=None):
    """Log a processing run to the processing_log table."""
    with get_db_cursor(conn) as cursor:
        cursor.execute(
            """
            INSERT INTO processing_log (run_type, period, status, rows_inserted, error_msg)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (run_type, period, status, rows_inserted, error_msg)
        )
    logger.info(f"Logged {run_type} {period}: {status} ({rows_inserted} rows)")


def check_processing_log(conn, run_type, period):
    """Check if a processing run has already succeeded."""
    with get_db_cursor(conn) as cursor:
        cursor.execute(
            """
            SELECT status FROM processing_log
            WHERE run_type = %s AND period = %s
            ORDER BY run_at DESC
            LIMIT 1
            """,
            (run_type, period)
        )
        result = cursor.fetchone()
        return result is not None and result[0] == "success"

