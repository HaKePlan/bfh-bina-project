#!/usr/bin/env python3
"""
CLI script to reset (drop and recreate) a PostgreSQL database schema.

Usage:
    python reset_db.py --database <name> [--yes]
"""

import argparse
import logging
import sys

import psycopg2
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def reset_database(db_name, force=False):
    """Drop all views and tables, then recreate schema from init.sql."""
    load_dotenv()

    import os
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")

    # Get confirmation if --yes not provided
    if not force:
        print(f"\nThis will permanently delete all data in database '{db_name}'.")
        confirm = input(f"Type the database name to confirm: ")
        if confirm != db_name:
            print("Confirmation failed. Aborting.")
            return False

    try:
        # Connect to postgres (default database) with autocommit mode
        conn = psycopg2.connect(
            host=host,
            port=port,
            database="postgres",
            user=user,
            password=password
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Terminate all connections to target database
        cursor.execute(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{db_name}'
            AND pid <> pg_backend_pid();
        """)

        # Drop and recreate database
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name};")
        cursor.execute(f"CREATE DATABASE {db_name};")
        cursor.close()
        conn.close()

        # Reconnect to new database and apply schema
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=db_name,
            user=user,
            password=password
        )
        conn.autocommit = False
        cursor = conn.cursor()

        # Read and execute init.sql
        with open("db/init.sql", "r") as f:
            schema = f.read()
        cursor.execute(schema)
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Database '{db_name}' reset successfully.")
        return True

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return False


def main():
    """Main entry point for reset_db.py."""
    parser = argparse.ArgumentParser(
        description="Reset a PostgreSQL database schema."
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Name of the database to reset (required)"
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt"
    )

    args = parser.parse_args()

    if not args.database:
        logger.error("--database argument is required.")
        return 1

    success = reset_database(args.database, force=args.yes)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())

