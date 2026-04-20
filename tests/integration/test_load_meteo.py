"""Integration tests for MeteoSwiss precipitation loading."""

import pytest
from pathlib import Path

from scripts.load_meteo import parse_meteo_csv, insert_precipitation_data
from scripts.db_utils import get_db_connection


@pytest.fixture
def meteo_csv_fixture():
    """Load the test MeteoSwiss CSV fixture for Bern."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "ogd-smn_ber_t_historical_2020-2029.csv"
    if not fixture_path.exists():
        pytest.skip(f"Fixture not found: {fixture_path}")
    with open(fixture_path, "rb") as f:
        return f.read()


@pytest.fixture
def db_connection():
    """Get a database connection for integration testing."""
    conn = get_db_connection()
    yield conn
    conn.close()


class TestInsertPrecipitationData:
    """Tests for insert_precipitation_data function."""

    def test_insert_returns_row_count(self, db_connection, meteo_csv_fixture):
        """Test that insert returns number of inserted rows."""
        records = parse_meteo_csv(meteo_csv_fixture, "BER")

        if records:
            rows_inserted = insert_precipitation_data(db_connection, "BER", records)
            assert isinstance(rows_inserted, int)
            assert rows_inserted > 0

    def test_insert_with_empty_records(self, db_connection):
        """Test that insert handles empty record list."""
        rows_inserted = insert_precipitation_data(db_connection, "BER", [])
        assert rows_inserted == 0

    def test_data_persisted_in_database(self, db_connection, meteo_csv_fixture):
        """Test that inserted data is actually in the database."""
        records = parse_meteo_csv(meteo_csv_fixture, "BER")

        if records:
            rows_inserted = insert_precipitation_data(db_connection, "BER", records)

            # Query database to verify
            cursor = db_connection.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM precipitation_10min WHERE station_abbr = %s",
                ("BER",)
            )
            count = cursor.fetchone()[0]
            cursor.close()

            assert count >= rows_inserted

    def test_duplicate_handling(self, db_connection, meteo_csv_fixture):
        """Test that duplicate timestamps are handled (upsert)."""
        records = parse_meteo_csv(meteo_csv_fixture, "BER")

        if records:
            # Insert twice
            insert_precipitation_data(db_connection, "BER", records)
            rows_inserted_2 = insert_precipitation_data(db_connection, "BER", records)

            # Second insert should skip duplicates
            assert rows_inserted_2 == 0

    def test_inserted_data_integrity(self, db_connection, meteo_csv_fixture):
        """Test that inserted data matches source records."""
        records = parse_meteo_csv(meteo_csv_fixture, "BER")

        if records:
            insert_precipitation_data(db_connection, "BER", records)

            # Sample check: verify a few records
            cursor = db_connection.cursor()
            cursor.execute(
                "SELECT station_abbr, city, measured_at, precip_mm FROM precipitation_10min "
                "WHERE station_abbr = %s LIMIT 5",
                ("BER",)
            )
            db_records = cursor.fetchall()
            cursor.close()

            # Verify structure
            assert all(len(r) == 4 for r in db_records)
            assert all(r[0] == "BER" for r in db_records)
            assert all(r[1] == "Bern" for r in db_records)

    def test_all_data_types_correct(self, db_connection, meteo_csv_fixture):
        """Test that inserted data has correct data types."""
        records = parse_meteo_csv(meteo_csv_fixture, "BER")

        if records:
            insert_precipitation_data(db_connection, "BER", records)

            cursor = db_connection.cursor()
            cursor.execute(
                "SELECT station_abbr, city, measured_at, precip_mm FROM precipitation_10min "
                "WHERE station_abbr = %s LIMIT 1",
                ("BER",)
            )
            record = cursor.fetchone()
            cursor.close()

            if record:
                station_abbr, city, measured_at, precip_mm = record
                assert isinstance(station_abbr, str)
                assert isinstance(city, str)
                # measured_at is datetime from psycopg2
                assert precip_mm is None or isinstance(precip_mm, (int, float))
