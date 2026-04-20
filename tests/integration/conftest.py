"""Integration test configuration and fixtures."""

import os
import subprocess

import pytest
from dotenv import load_dotenv


@pytest.fixture(scope="session", autouse=True)
def reset_test_db():
    """Reset the test database before running integration tests."""
    load_dotenv(".env.test")

    db_name = os.getenv("DB_NAME", "sbb_precipitation_test")

    # Ensure we're using the test database
    if db_name != "sbb_precipitation_test":
        raise ValueError(
            f"Integration tests must use DB_NAME='sbb_precipitation_test', "
            f"not '{db_name}'. Check .env.test."
        )

    try:
        # Run reset_db.py with --yes flag
        result = subprocess.run(
            ["python", "scripts/reset_db.py", "--database", db_name, "--yes"],
            capture_output=True,
            text=True,
            timeout=30
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to reset test database: {result.stderr}")
        print(f"Test database '{db_name}' reset successfully.")
    except Exception as e:
        raise RuntimeError(f"Integration test database setup failed: {e}")

    yield


@pytest.fixture
def test_db_connection():
    """Provide a database connection for integration tests."""
    import psycopg2

    load_dotenv(".env.test")

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "sbb_precipitation_test")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")

    if name != "sbb_precipitation_test":
        raise ValueError(f"Test fixture must use sbb_precipitation_test, not {name}")

    conn = psycopg2.connect(
        host=host,
        port=port,
        database=name,
        user=user,
        password=password
    )

    yield conn

    conn.close()

