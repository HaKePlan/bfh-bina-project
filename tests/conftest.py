"""Pytest configuration and shared fixtures."""

import pytest


@pytest.fixture(scope="session")
def test_data_dir():
    """Return the path to the test data directory."""
    from pathlib import Path
    return Path(__file__).parent / "fixtures"

