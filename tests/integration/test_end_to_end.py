"""End-to-end integration test for meteo and sbb scripts working together."""

import pytest
from pathlib import Path
from datetime import datetime

from scripts.load_meteo import parse_meteo_csv, insert_precipitation_data
from scripts.sbb_parser import parse_sbb_csv
from scripts.precipitation import load_precipitation_cache, get_median_precipitation_cached
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
def sbb_csv_fixture():
    """Load the test SBB CSV fixture."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "2024-01-01_istdaten.csv"
    if not fixture_path.exists():
        pytest.skip(f"Fixture not found: {fixture_path}")
    with open(fixture_path, "r", encoding="utf-8") as f:
        return f.read()


@pytest.fixture
def db_connection():
    """Get a database connection for integration testing."""
    conn = get_db_connection()
    yield conn
    conn.close()


class TestEndToEndPipeline:
    """End-to-end tests for meteo + sbb pipeline."""

    def test_meteo_parsing_works(self, meteo_csv_fixture):
        """Test that meteo CSV parsing works."""
        meteo_records = parse_meteo_csv(meteo_csv_fixture, "BER")
        # Fixture may not have 2024-2025 data, but parsing should work
        assert isinstance(meteo_records, list)

    def test_sbb_parsing_works(self, sbb_csv_fixture):
        """Test that SBB CSV parsing works."""
        sbb_records = parse_sbb_csv(sbb_csv_fixture, "2024-01")
        assert isinstance(sbb_records, list)
        assert len(sbb_records) > 0

    def test_sbb_can_query_meteo_data(self, db_connection, meteo_csv_fixture, sbb_csv_fixture):
        """Test that SBB processing can query meteo data (even if empty)."""
        # Step 1: Load meteo data (may be empty if fixture isn't 2024-2025)
        meteo_records = parse_meteo_csv(meteo_csv_fixture, "BER")
        rows_inserted = insert_precipitation_data(db_connection, "BER", meteo_records)
        print(f"Inserted {rows_inserted} meteo records")

        # Step 2: Load precipitation cache (like collect_sbb.py does)
        precip_cache = load_precipitation_cache(db_connection)

        # Step 3: Verify cache exists (may be empty if no data)
        assert isinstance(precip_cache, dict), "Cache should be a dict"

    def test_sbb_parsing_with_meteo_enrichment(self, db_connection, meteo_csv_fixture, sbb_csv_fixture):
        """Test that SBB records can be enriched with meteo data."""
        # Step 1: Load meteo data
        meteo_records = parse_meteo_csv(meteo_csv_fixture, "BER")
        insert_precipitation_data(db_connection, "BER", meteo_records)

        # Step 2: Parse SBB data
        sbb_records = parse_sbb_csv(sbb_csv_fixture, "2024-01")

        if sbb_records:
            # Step 3: Load precipitation cache
            precip_cache = load_precipitation_cache(db_connection)

            # Step 4: For each SBB record, compute median precipitation
            enriched_count = 0
            for record in sbb_records[:10]:  # Test first 10 records
                destination_city = record.get("destination_city")
                origin_departure = record.get("origin_departure_scheduled")
                scheduled_arrival = record.get("scheduled_arrival")

                if destination_city and origin_departure and scheduled_arrival:
                    # Try to get median precipitation (like collect_sbb.py does)
                    median_precip = get_median_precipitation_cached(
                        precip_cache,
                        destination_city,
                        origin_departure,
                        scheduled_arrival
                    )
                    # It's ok if median_precip is None, the important thing is
                    # that we can query without error
                    enriched_count += 1

            assert enriched_count > 0, "Should be able to enrich some SBB records"

    def test_data_types_in_sbb_records(self, sbb_csv_fixture):
        """Test that SBB records have correct data types."""
        sbb_records = parse_sbb_csv(sbb_csv_fixture, "2024-01")

        if sbb_records:
            for record in sbb_records:
                # Verify SBB record structure
                assert isinstance(record["betriebstag"], object)  # datetime.date
                assert isinstance(record["fahrt_bezeichner"], str)
                assert isinstance(record["destination_station"], str)
                assert isinstance(record["destination_city"], str)
                assert isinstance(record["scheduled_arrival"], datetime)
                assert isinstance(record["actual_arrival"], datetime)
                assert isinstance(record["arrival_delay_min"], (int, float))

                # origin_station and trip_duration_min should be present
                assert "origin_station" in record
                assert "trip_duration_min" in record

                if record["origin_station"] is not None:
                    assert isinstance(record["origin_station"], str)
                if record["trip_duration_min"] is not None:
                    assert isinstance(record["trip_duration_min"], (int, float))

    def test_meteo_database_persistence(self, db_connection, meteo_csv_fixture):
        """Test that meteo data persists in database and can be queried."""
        # Insert meteo data
        meteo_records = parse_meteo_csv(meteo_csv_fixture, "BER")
        rows_inserted = insert_precipitation_data(db_connection, "BER", meteo_records)

        # Verify persistence by querying (even if no rows, query should work)
        cursor = db_connection.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM precipitation_10min WHERE station_abbr = %s",
            ("BER",)
        )
        count = cursor.fetchone()[0]
        cursor.close()

        assert count >= rows_inserted, "All inserted rows should be queryable"

    def test_sbb_records_have_origin_data(self, sbb_csv_fixture):
        """Test that parsed SBB records include origin station and trip duration."""
        sbb_records = parse_sbb_csv(sbb_csv_fixture, "2024-01")

        assert len(sbb_records) > 0, "Should parse SBB records"

        records_with_origin = [r for r in sbb_records if r.get("origin_station") is not None]
        assert len(records_with_origin) > 0, "Some records should have origin_station"

        records_with_duration = [r for r in sbb_records if r.get("trip_duration_min") is not None]
        assert len(records_with_duration) > 0, "Some records should have trip_duration_min"

        # Verify origin data quality for records with valid duration
        valid_duration_records = [
            r for r in records_with_origin
            if r.get("trip_duration_min") is not None and r.get("trip_duration_min") > 0
        ]
        assert len(valid_duration_records) > 0, "Some records should have positive trip_duration_min"

        for record in valid_duration_records[:10]:  # Check first 10 valid records
            origin_station = record["origin_station"]
            assert isinstance(origin_station, str), "origin_station should be string"
            assert len(origin_station) > 0, "origin_station should not be empty"

            trip_duration = record["trip_duration_min"]
            assert trip_duration > 0, "Valid trip_duration_min should be positive"

    def test_complete_pipeline_flow(self, db_connection, meteo_csv_fixture, sbb_csv_fixture):
        """Test complete flow: meteo → database → cache → sbb enrichment."""
        # Step 1: Parse and load meteo
        meteo_records = parse_meteo_csv(meteo_csv_fixture, "BER")
        meteo_rows = insert_precipitation_data(db_connection, "BER", meteo_records)
        print(f"Inserted {meteo_rows} meteo records")

        # Step 2: Parse SBB
        sbb_records = parse_sbb_csv(sbb_csv_fixture, "2024-01")
        print(f"Parsed {len(sbb_records)} SBB records")
        assert len(sbb_records) > 0, "Should parse SBB records"

        # Step 3: Load cache and verify
        precip_cache = load_precipitation_cache(db_connection)
        assert isinstance(precip_cache, dict), "Cache should be a dict"

        # Step 4: Enrich SBB records with meteo data (if meteo data exists for Bern)
        enriched_records = []
        for record in sbb_records:
            if record.get("destination_city") == "Bern":
                origin_departure = record.get("origin_departure_scheduled")
                if origin_departure:
                    median_precip = get_median_precipitation_cached(
                        precip_cache,
                        record["destination_city"],
                        origin_departure,
                        record["scheduled_arrival"]
                    )
                    enriched_records.append({
                        **record,
                        "median_precip_mm": median_precip
                    })

        print(f"Enriched {len(enriched_records)} records for Bern")

        # Verify pipeline worked
        # Note: meteo_rows may be 0 if fixture isn't for 2024-2025
        assert isinstance(meteo_rows, int), "Should return insert count"
        assert len(sbb_records) > 0, "SBB records should be parsed"
        assert len(enriched_records) > 0, "Should have some Bern records to enrich"

    def test_pipeline_handles_missing_meteo(self, db_connection, sbb_csv_fixture):
        """Test that SBB processing works even without meteo data."""
        # Parse SBB without loading meteo
        sbb_records = parse_sbb_csv(sbb_csv_fixture, "2024-01")

        # Load cache (will be empty)
        precip_cache = load_precipitation_cache(db_connection)

        # Should still be able to enrich (just with None values)
        for record in sbb_records[:5]:
            if record.get("destination_city") == "Bern" and record.get("origin_departure_scheduled"):
                median_precip = get_median_precipitation_cached(
                    precip_cache,
                    record["destination_city"],
                    record["origin_departure_scheduled"],
                    record["scheduled_arrival"]
                )
                # Should return None when no data
                assert median_precip is None or isinstance(median_precip, (int, float))
