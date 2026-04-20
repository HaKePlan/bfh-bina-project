#!/usr/bin/env python
"""Download test fixtures that are too large for GitHub.
This script downloads SBB CSV fixtures required for unit and integration tests.
"""
import sys
from pathlib import Path
import urllib.request
import zipfile
import io
SBB_FIXTURE_URL = "https://archive.opentransportdata.swiss/istdaten/2024/ist-daten-2024-01.zip"
SBB_FIXTURE_NAME = "2024-01-01_istdaten.csv"
def print_instructions():
    """Print setup instructions."""
    fixture_dir = Path(__file__).parent
    fixture_path = fixture_dir / SBB_FIXTURE_NAME
    print("\n" + "=" * 70)
    print("SBB Test Fixture Setup")
    print("=" * 70)
    print(f"\nThe file {SBB_FIXTURE_NAME} is required for tests.")
    print(f"Download: {SBB_FIXTURE_URL}")
    print(f"Save to:  {fixture_path}")
    print("=" * 70 + "\n")
def prepare_fixtures():
    """Download test fixtures."""
    fixture_dir = Path(__file__).parent
    sbb_fixture_path = fixture_dir / SBB_FIXTURE_NAME
    if sbb_fixture_path.exists():
        print(f"✓ {SBB_FIXTURE_NAME} already exists")
        return True
    print(f"Downloading {SBB_FIXTURE_NAME}...")
    try:
        with urllib.request.urlopen(SBB_FIXTURE_URL) as response:
            total_size = int(response.headers.get('Content-Length', 0))
            chunks = []
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                chunks.append(chunk)
            zip_data = b''.join(chunks)
        print(f"Extracting {SBB_FIXTURE_NAME}...")
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_ref:
            matching = [f for f in zip_ref.namelist() if f.endswith(SBB_FIXTURE_NAME)]
            if matching:
                with open(sbb_fixture_path, 'wb') as f:
                    f.write(zip_ref.read(matching[0]))
                print(f"✓ Saved to: {sbb_fixture_path}")
                return True
            else:
                print(f"✗ File not found in ZIP")
                return False
    except Exception as e:
        print(f"✗ Download failed: {e}")
        print_instructions()
        return False
if __name__ == "__main__":
    success = prepare_fixtures()
    sys.exit(0 if success else 1)
