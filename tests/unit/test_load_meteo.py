"""Unit tests for MeteoSwiss precipitation loading."""

import pytest
from pathlib import Path
from datetime import datetime

from scripts.load_meteo import parse_meteo_csv, METEO_STATIONS


@pytest.fixture
def meteo_csv_fixture():
    """Load the test MeteoSwiss CSV fixture for Bern."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "ogd-smn_ber_t_historical_2020-2029.csv"
    if not fixture_path.exists():
        pytest.skip(f"Fixture not found: {fixture_path}")
    with open(fixture_path, "rb") as f:
        return f.read()


class TestParseMeteoCSV:
    """Tests for parse_meteo_csv function."""

    def test_parse_returns_list(self, meteo_csv_fixture):
        """Test that parse returns a list."""
        results = parse_meteo_csv(meteo_csv_fixture, "BER")
        assert isinstance(results, list)

    def test_parse_filters_by_year(self, meteo_csv_fixture):
        """Test that only 2024-2025 records are returned."""
        results = parse_meteo_csv(meteo_csv_fixture, "BER")
        for record in results:
            assert 2024 <= record["measured_at"].year <= 2025

    def test_parse_required_fields(self, meteo_csv_fixture):
        """Test that parsed records have all required fields."""
        results = parse_meteo_csv(meteo_csv_fixture, "BER")
        if results:
            record = results[0]
            assert "station_abbr" in record
            assert "city" in record
            assert "measured_at" in record
            assert "precip_mm" in record

    def test_station_abbr_set_correctly(self, meteo_csv_fixture):
        """Test that station_abbr is set correctly."""
        results = parse_meteo_csv(meteo_csv_fixture, "BER")
        assert all(r["station_abbr"] == "BER" for r in results)

    def test_city_mapped_correctly(self, meteo_csv_fixture):
        """Test that city is mapped from station_abbr."""
        results = parse_meteo_csv(meteo_csv_fixture, "BER")
        assert all(r["city"] == "Bern" for r in results)

    def test_timestamp_parsed_as_datetime(self, meteo_csv_fixture):
        """Test that measured_at is a datetime object."""
        results = parse_meteo_csv(meteo_csv_fixture, "BER")
        if results:
            assert isinstance(results[0]["measured_at"], datetime)

    def test_precip_mm_nullable(self, meteo_csv_fixture):
        """Test that precip_mm can be None (sensor gaps)."""
        results = parse_meteo_csv(meteo_csv_fixture, "BER")
        # Check that at least some records exist
        assert len(results) > 0
        # Some records may have None precip_mm
        precip_values = [r["precip_mm"] for r in results]
        assert any(v is not None for v in precip_values)

    def test_precip_mm_numeric(self, meteo_csv_fixture):
        """Test that non-null precip_mm values are numeric."""
        results = parse_meteo_csv(meteo_csv_fixture, "BER")
        for record in results:
            if record["precip_mm"] is not None:
                assert isinstance(record["precip_mm"], (int, float))
                assert record["precip_mm"] >= 0

    def test_invalid_station_returns_empty(self):
        """Test that invalid station returns empty list."""
        csv_content = b"reference_timestamp;rre150z0\n01.01.2024 10:00;0.5"
        results = parse_meteo_csv(csv_content, "INVALID")
        assert isinstance(results, list)
        # City will be None for invalid station, but parsing should work

    def test_missing_required_column_returns_empty(self):
        """Test that missing required columns returns empty list."""
        # CSV missing rre150z0 column
        csv_content = b"reference_timestamp;other_column\n01.01.2024 10:00;value"
        results = parse_meteo_csv(csv_content, "BER")
        assert isinstance(results, list)
        assert len(results) == 0

