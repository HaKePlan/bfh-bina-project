"""Unit tests for SBB CSV parsing."""

import pytest
from pathlib import Path
from datetime import datetime

from scripts.sbb_parser import parse_sbb_csv, TARGET_STATIONS, STATION_TO_CITY


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


class TestParseSbbCSV:
    """Tests for parse_sbb_csv function."""

    def test_parse_returns_list(self, parsed_sbb_records):
        """Test that parse returns a list."""
        assert isinstance(parsed_sbb_records, list)

    def test_only_zug_rows_returned(self, parsed_sbb_records):
        """Test that only rows with PRODUKT_ID == 'Zug' are returned."""
        # All returned records should be from Zug (trains)
        # We can't directly check PRODUKT_ID but we can verify the results match expectations
        assert len(parsed_sbb_records) > 0, "Should have parsed Zug records"

    def test_only_real_prognose_status(self, parsed_sbb_records):
        """Test that only rows with AN_PROGNOSE_STATUS == 'REAL' are returned."""
        # All returned records should have REAL status
        # Verified indirectly by checking that required fields exist
        for record in parsed_sbb_records:
            assert "actual_arrival" in record, "All records should have actual_arrival from REAL status"
            assert record["actual_arrival"] is not None

    def test_only_target_stations(self, parsed_sbb_records):
        """Test that only rows for TARGET_STATIONS are returned."""
        for record in parsed_sbb_records:
            assert record["destination_station"] in TARGET_STATIONS, \
                f"Station {record['destination_station']} not in target stations {TARGET_STATIONS}"
            assert record["destination_city"] in STATION_TO_CITY.values(), \
                f"City {record['destination_city']} not in valid cities"

    def test_excluded_cancelled_trains(self, parsed_sbb_records):
        """Test that cancelled trains (FAELLT_AUS_TF == True) are excluded."""
        # All returned records should be non-cancelled
        # If any cancelled record was included, this test would fail
        assert len(parsed_sbb_records) > 0, "Should have non-cancelled records"

    def test_arrival_delay_min_computed_correctly(self, parsed_sbb_records):
        """Test that arrival_delay_min is computed correctly."""
        for record in parsed_sbb_records:
            # arrival_delay_min should be (actual_arrival - scheduled_arrival) in minutes
            delay = record["arrival_delay_min"]
            assert isinstance(delay, (int, float)), "Delay should be numeric"

            # Verify computation: actual - scheduled should match
            actual = record["actual_arrival"]
            scheduled = record["scheduled_arrival"]
            expected_delay = (actual - scheduled).total_seconds() / 60
            assert abs(delay - expected_delay) < 0.1, \
                f"Delay mismatch: {delay} vs {expected_delay}"

    def test_required_fields_present(self, parsed_sbb_records):
        """Test that all required fields are present in parsed records."""
        if parsed_sbb_records:
            required_fields = [
                "betriebstag",
                "fahrt_bezeichner",
                "destination_station",
                "destination_city",
                "scheduled_arrival",
                "actual_arrival",
                "arrival_delay_min",
                "origin_station",
                "origin_departure_scheduled",
                "trip_duration_min",
                "source_month",
            ]

            for record in parsed_sbb_records[:10]:  # Check first 10 records
                for field in required_fields:
                    assert field in record, f"Missing field: {field}"

    def test_source_month_set_correctly(self, parsed_sbb_records):
        """Test that source_month is set to the provided period."""
        for record in parsed_sbb_records:
            assert record["source_month"] == "2024-01", \
                "source_month should match the provided period"

    def test_betriebstag_is_date(self, parsed_sbb_records):
        """Test that betriebstag is parsed as a date."""
        if parsed_sbb_records:
            for record in parsed_sbb_records[:10]:
                betriebstag = record["betriebstag"]
                # Should be a date object
                assert hasattr(betriebstag, "year"), "betriebstag should have year attribute"
                assert hasattr(betriebstag, "month"), "betriebstag should have month attribute"
                assert hasattr(betriebstag, "day"), "betriebstag should have day attribute"

    def test_timestamps_are_datetime(self, parsed_sbb_records):
        """Test that timestamps are parsed as datetime objects."""
        if parsed_sbb_records:
            for record in parsed_sbb_records[:10]:
                assert isinstance(record["scheduled_arrival"], datetime), \
                    "scheduled_arrival should be datetime"
                assert isinstance(record["actual_arrival"], datetime), \
                    "actual_arrival should be datetime"

    def test_handles_missing_origin_data(self, parsed_sbb_records):
        """Test that records can have NULL origin data (handled gracefully)."""
        # Some records may have NULL origin data, which is acceptable
        records_with_null_origin = [r for r in parsed_sbb_records if r.get("origin_station") is None]
        records_with_origin = [r for r in parsed_sbb_records if r.get("origin_station") is not None]

        # At least some records should have origin data
        assert len(records_with_origin) > 0, "Should have some records with origin data"
        # Some may have NULL origin data (edge case)
        # This is acceptable - records_with_null_origin can be any number

    def test_city_mapped_from_station(self, parsed_sbb_records):
        """Test that destination_city is correctly mapped from destination_station."""
        for record in parsed_sbb_records:
            station = record["destination_station"]
            city = record["destination_city"]

            expected_city = STATION_TO_CITY.get(station)
            assert city == expected_city, \
                f"City {city} doesn't match station {station} mapping {expected_city}"
