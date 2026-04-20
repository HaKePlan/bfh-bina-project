# Test Fixtures

## Overview

This project uses two test fixtures with different handling approaches.

### Large Fixtures (Auto-Downloaded)

**File:** `tests/fixtures/2024-01-01_istdaten.csv` (SBB train data)
- **Size:** 313MB (extracted)
- **Status:** NOT committed to Git (exceeds GitHub's 100MB limit)
- **Handling:** Auto-downloaded on first test run, cached locally
- **Source:** [OpenTransportData](https://archive.opentransportdata.swiss/istdaten/2024/)
- **Manual Download:** `python tests/fixtures/download_fixture.py`

### Committed Fixtures

**File:** `tests/fixtures/ogd-smn_ber_t_historical_2020-2029.csv` (MeteoSwiss precipitation)
- **Size:** 41MB
- **Status:** COMMITTED to Git (within 100MB limit)
- **Handling:** Available immediately after `git clone`
- **Source:** [MeteoSwiss](https://data.geo.admin.ch/)

## Auto-Download Mechanism

The pytest hook `tests/fixtures/conftest.py` automatically:
1. Checks if `2024-01-01_istdaten.csv` exists before tests run
2. Downloads and extracts from OpenTransportData if missing
3. Caches the file locally for reuse
4. Allows tests to proceed normally

## Manual Setup

If automatic download fails:

```bash
python tests/fixtures/download_fixture.py
```

This script provides detailed instructions and status messages.

