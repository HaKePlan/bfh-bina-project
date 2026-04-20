"""Pytest configuration for test fixtures.
Auto-downloads large test files if they don't exist.
"""
import pytest
from pathlib import Path
def pytest_configure(config):
    """Pytest hook called before test collection.
    This ensures fixtures are available before tests run.
    """
    fixture_dir = Path(__file__).parent
    sbb_fixture = fixture_dir / "2024-01-01_istdaten.csv"
    # If the SBB fixture is missing, try to download it
    if not sbb_fixture.exists():
        try:
            from tests.fixtures.download_fixture import prepare_fixtures
            prepare_fixtures()
        except Exception:
            # If download fails, tests will skip gracefully
            pass
