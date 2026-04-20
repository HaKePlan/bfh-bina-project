# SBB Precipitation Study

Investigation of the correlation between precipitation and train delays at three major Swiss railway stations: Zürich HB, Basel SBB, and Bern.

**Study period:** 2024–2025

---

## Project Structure

```
.
├── .github/
│   └── copilot-instructions.md     # Project guidelines
├── db/
│   ├── init.sql                    # Database schema
│   └── 02-init-databases.sql       # Multi-database init script
├── scripts/
│   ├── collect_sbb.py              # Download SBB train data
│   ├── load_meteo.py               # Download MeteoSwiss precipitation
│   ├── reset_db.py                 # Reset database schema
│   ├── db_utils.py                 # Shared database utilities
│   ├── sbb_parser.py               # SBB CSV parsing utilities
│   ├── precipitation.py            # Precipitation data enrichment
│   └── requirements.txt            # Python dependencies
├── notebooks/
│   └── analysis.ipynb              # Jupyter analysis notebook
├── tests/
│   ├── conftest.py                 # Pytest configuration
│   ├── unit/                       # Unit tests (no DB/network)
│   ├── integration/                # Integration tests (requires DB)
│   └── fixtures/                   # Test data
├── docker-compose.yml              # PostgreSQL + pgAdmin
├── .env.example                    # Environment template
├── .env.test.example               # Test environment template
└── .gitignore
```

---

## Quick Start

### 1. Clone and set up environment

```bash
git clone <repo>
cd bfh-bina-project-wt1

# Copy environment templates
cp .env.example .env
cp .env.test.example .env.test

# Edit .env with your local database credentials if needed
```

### 2. Start PostgreSQL and pgAdmin

```bash
docker-compose up -d
```

This creates:
- PostgreSQL on `localhost:5432` with databases `sbb_precipitation` and `sbb_precipitation_test`
- pgAdmin on `http://localhost:5050`

### 3. Install Python dependencies

```bash
pip install -r scripts/requirements.txt
```

### 4. Download and process data

```bash
# Load MeteoSwiss precipitation data
python scripts/load_meteo.py

# Download and process SBB train data for 2024-2025
python scripts/collect_sbb.py --start-year 2024 --end-year 2025

# Or for a quick test run (Jan-Feb 2024)
python scripts/collect_sbb.py --start-year 2024 --end-year 2024 --months 1,2
```

### 5. Run analysis notebook

```bash
jupyter notebook notebooks/analysis.ipynb
```

---

## Database Schema

The database contains three main tables:

- **`precipitation_10min`**: 10-minute precipitation readings from MeteoSwiss
- **`train_connections`**: Qualified train arrivals with computed delays and trip origins
- **`processing_log`**: Processing run history for crash recovery
- **`analysis` (view)**: Denormalized view for notebook queries

See `db/init.sql` for the complete schema definition.

**Key fields populated by the collection pipeline:**
- `origin_station`: Starting station of the trip
- `origin_departure_scheduled`: Scheduled departure time from origin
- `trip_duration_min`: Total trip duration in minutes (origin departure to destination arrival)
- `median_precip_mm`: Median precipitation during the trip window

---

## CLI Scripts

### `load_meteo.py`

Download MeteoSwiss precipitation data (SMA, BAS, BER stations):

```bash
python scripts/load_meteo.py [--debug]
```

- `--debug`: Keep CSV files in `raw/meteo/` after processing

### `collect_sbb.py`

Download and process SBB monthly archives:

```bash
python scripts/collect_sbb.py --start-year YYYY --end-year YYYY [--months M,M] [--debug]
```

- `--start-year`: First year (e.g. 2024)
- `--end-year`: Last year (e.g. 2025)
- `--months`: Comma-separated months to process (default: all 12)
- `--debug`: Keep ZIP and CSV files in `raw/sbb/` after processing

**Examples:**

```bash
# Full 2024-2025 range
python scripts/collect_sbb.py --start-year 2024 --end-year 2025

# Jan-Feb 2024 only
python scripts/collect_sbb.py --start-year 2024 --end-year 2024 --months 1,2

# Oct-Nov both years, keep files
python scripts/collect_sbb.py --start-year 2024 --end-year 2025 --months 10,11 --debug
```

### `reset_db.py`

Drop and recreate database schema from `db/init.sql`:

```bash
python scripts/reset_db.py --database <name> [--yes]
```

- `--database`: Database name (required, no default)
- `--yes`: Skip confirmation prompt (required for non-interactive use)

**Examples:**

```bash
# Interactive reset
python scripts/reset_db.py --database sbb_precipitation

# Non-interactive reset (used by tests)
python scripts/reset_db.py --database sbb_precipitation_test --yes
```

---

## Testing

### Test Fixtures

Two test fixtures are used with different handling:

**SBB Train Data** — Auto-downloaded on first test run
- File: `tests/fixtures/2024-01-01_istdaten.csv` (313MB)
- Not committed to Git (exceeds GitHub's 100MB limit)
- Auto-downloads from OpenTransportData when needed
- Cached locally for reuse

**MeteoSwiss Precipitation** — Committed to repository
- File: `tests/fixtures/ogd-smn_ber_t_historical_2020-2029.csv` (41MB)
- Committed to Git (within 100MB limit)
- Available immediately after clone

See `.github/FIXTURES.md` for detailed fixture documentation.

### Unit tests (no DB or network required)

```bash
pytest tests/unit/
```

### Integration tests (requires running PostgreSQL)

```bash
pytest tests/integration/
```

### All tests

```bash
pytest tests/
```

---

## Development

### Code style

Follow PEP 8 and the Zen of Python. See `.github/copilot-instructions.md` for detailed conventions.

### File organization

- No file should exceed ~150 lines of code (excluding comments)
- Split logic into small, single-purpose functions
- Use `snake_case` for functions, variables, files
- Use `UPPER_SNAKE_CASE` for constants

### Database operations

- Always use parameterized queries (never string formatting for SQL)
- Load credentials from `.env` via `python-dotenv`
- Always close connections explicitly or use context managers
- Log all database errors at appropriate levels

### Error handling

- Use Python's `logging` module for diagnostics
- Log at `INFO` for progress, `WARNING` for skips, `ERROR` for recoverable failures
- If a single daily CSV fails, log and continue; don't abort the entire month
- All exceptions must be caught and either logged/handled or re-raised

---

## Performance Notes

**Data collection speed:**
- MeteoSwiss load: ~1-2 minutes per run
- SBB collection: ~10-16 minutes per month (optimized with in-memory precipitation cache)
- Full 2024-2025 range: ~4-5 hours total

**Optimizations:**
- Precipitation data is loaded into memory once per month (not queried per-record)
- Trip origins computed once per daily CSV using pandas groupby
- Database upsert operations batch-committed for efficiency

---

## Known Limitations

- Single predictor (precipitation) has inherently low explanatory power
- Confounding variables not modelled: day of week, holidays, season, incidents, infrastructure works
- MeteoSwiss station location may not precisely align with train route
- Overnight trips (crossing midnight) are excluded
- Data quality depends on SBB data availability and MeteoSwiss sensor coverage

---

## Troubleshooting

**Connection refused on port 5432:**
- Ensure `docker-compose up -d` has completed
- Check logs: `docker-compose logs postgres`

**"DB_NAME is required" error:**
- Copy `.env.example` to `.env` and update as needed

**Integration tests fail:**
- Ensure `.env.test` is configured with correct credentials
- Run `python scripts/reset_db.py --database sbb_precipitation_test --yes` manually

**Memory issues with large data:**
- Use `--months` to process smaller date ranges at a time
- Check available disk space in `raw/` directory

**Import errors for dotenv:**
- Ensure dependencies are installed: `pip install -r scripts/requirements.txt`
- The `python-dotenv` package is required to load `.env` credentials

---

## Further Reading

- See `.github/copilot-instructions.md` for complete project specifications and API guidelines
- See `notebooks/analysis.ipynb` for methodology, analysis, and findings

