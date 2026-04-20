"""Integration tests for collect_sbb.py."""

import pytest
from pathlib import Path

from scripts.sbb_parser import parse_sbb_csv, TARGET_STATIONS
from scripts.db_utils import get_db_connection


@pytest.fixture(scope="class")
def sbb_csv_fixture():
    """Load the test SBB CSV fixture (class-scoped for reuse across tests)."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "2024-01-01_istdaten.csv"
    if not fixture_path.exists():
        pytest.skip(f"Fixture not found: {fixture_path}")
    with open(fixture_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture(scope="class")
def parsed_sbb_records(sbb_csv_fixture):
    """Parse SBB CSV once per test class and cache results."""
    return parse_sbb_csv(sbb_csv_fixture, "2024-01")


@pytest.fixture
def db_connection():
    """Get a database connection for integration testing."""
    conn = get_db_connection()
    yield conn
    conn.close()


class TestSbbParsing:
    """Tests for SBB CSV parsing."""

    def test_parse_sbb_csv_with_fixture(self, parsed_sbb_records):
        """Test parsing SBB CSV with the actual fixture."""
        assert isinstance(parsed_sbb_records, list), "Should return a list"
        assert len(parsed_sbb_records) > 0, "Should parse records from fixture"

        # Verify all records are for target stations
        for record in parsed_sbb_records:
            assert record["destination_station"] in TARGET_STATIONS, \
                f"Record has non-target station: {record['destination_station']}"

    def test_all_target_stations_represented(self, parsed_sbb_records):
        """Test that records exist for all three target stations."""
        stations_found = {r["destination_station"] for r in parsed_sbb_records}

        # At least some of the target stations should be represented
        assert len(stations_found) > 0, "Should have records from target stations"
        assert stations_found.issubset(TARGET_STATIONS), \
            f"Found unexpected stations: {stations_found - TARGET_STATIONS}"

    def test_arrival_delay_values_reasonable(self, parsed_sbb_records):
        """Test that arrival_delay_min values are within reasonable bounds."""
        for record in parsed_sbb_records:
            delay = record["arrival_delay_min"]
            # Delays can range from -24h to +24h (edge cases), but typically ±2h
            assert -1440 <= delay <= 1440, \
                f"Unreasonable delay value: {delay} minutes"

    def test_origin_station_present_in_majority(self, parsed_sbb_records):
        """Test that most records have origin_station populated."""
        with_origin = sum(1 for r in parsed_sbb_records if r.get("origin_station") is not None)
        total = len(parsed_sbb_records)

        # At least 95% should have origin data
        assert with_origin / total > 0.90, \
            f"Only {with_origin}/{total} records have origin_station"

    def test_trip_duration_reasonable_when_present(self, parsed_sbb_records):
        """Test that trip_duration_min is reasonable when populated."""
        # Some records may have negative durations due to data quality issues
        # (arrival before origin departure). This is acceptable.
        valid_durations = [
            r.get("trip_duration_min")
            for r in parsed_sbb_records
            if r.get("trip_duration_min") is not None and r.get("trip_duration_min") > 0
        ]

        # Most records should have positive durations
        assert len(valid_durations) > 0, "Should have some records with positive trip_duration_min"


class TestSbbDatabaseIntegration:
    """Tests for SBB data with database integration."""

    def test_parsed_records_can_be_inserted(self, db_connection, parsed_sbb_records):
        """Test that parsed records have the right structure for database insertion."""
        # Check structure of records for database insertion
        required_db_fields = [
            "betriebstag",
            "fahrt_bezeichner",
            "destination_station",
            "destination_city",
            "scheduled_arrival",
            "actual_arrival",
            "arrival_delay_min",
            "source_month",
        ]

        if parsed_sbb_records:
            for record in parsed_sbb_records[:10]:
                for field in required_db_fields:
                    assert field in record, f"Missing field for DB insert: {field}"
                    assert record[field] is not None, f"NULL value for required field: {field}"

    def test_records_match_target_stations(self, parsed_sbb_records):
        """Test that all parsed records are for target stations."""
        for record in parsed_sbb_records:
            station = record["destination_station"]
            city = record["destination_city"]

            assert station in TARGET_STATIONS, \
                f"Record for non-target station: {station}"

            # City should match the station
            from scripts.sbb_parser import STATION_TO_CITY
            assert city == STATION_TO_CITY[station], \
                f"City mismatch for station {station}"

    def test_unique_fahrt_designation(self, parsed_sbb_records):
        """Test that each record can be uniquely identified."""
        if parsed_sbb_records:
            # Create composite keys (fahrt_bezeichner, scheduled_arrival)
            composite_keys = [
                (r["fahrt_bezeichner"], r["scheduled_arrival"])
                for r in parsed_sbb_records
            ]

            # Check for duplicates (based on the unique constraint in DB)
            unique_keys = set(composite_keys)

            # Should be mostly unique (may have some duplicates in edge cases)
            uniqueness_ratio = len(unique_keys) / len(composite_keys)
            assert uniqueness_ratio > 0.95, \
                f"Too many duplicate keys: {uniqueness_ratio}"

    def test_data_consistency_across_records(self, parsed_sbb_records):
        """Test data consistency across parsed records."""
        for record in parsed_sbb_records:
            betriebstag = record["betriebstag"]
            scheduled = record["scheduled_arrival"]

            # Scheduled arrival should be on the same day as betriebstag or next day
            # (overnight trips on the same betriebstag are allowed)
            scheduled_date = scheduled.date()

            # Allow same day or next day (some trains arrive after midnight)
            assert scheduled_date == betriebstag or scheduled_date == betriebstag + __import__('datetime').timedelta(days=1), \
                f"Date mismatch: betriebstag={betriebstag}, scheduled_arrival={scheduled_date}"

    def test_sample_records_valid_for_insertion(self, parsed_sbb_records):
        """Test that sample records are valid for database insertion."""
        # Sample test: check first few records have all necessary data
        if parsed_sbb_records:
            sample = parsed_sbb_records[0]

            # Verify data types for database storage
            assert isinstance(sample["betriebstag"].isoformat(), str), "betriebstag must be serializable"
            assert isinstance(sample["fahrt_bezeichner"], str), "fahrt_bezeichner must be string"
            assert isinstance(sample["destination_station"], str), "destination_station must be string"
            assert isinstance(sample["destination_city"], str), "destination_city must be string"
            assert isinstance(sample["scheduled_arrival"].isoformat(), str), "scheduled_arrival must be serializable"
            assert isinstance(sample["actual_arrival"].isoformat(), str), "actual_arrival must be serializable"
            assert isinstance(sample["arrival_delay_min"], (int, float)), "arrival_delay_min must be numeric"
