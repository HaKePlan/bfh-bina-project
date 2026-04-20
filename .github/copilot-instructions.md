# GitHub Copilot Instructions

## 1. Project Overview

This project is a Business Intelligence study that investigates the correlation
between precipitation and train delays at three major Swiss railway stations:
Zürich HB, Basel SBB, and Bern.

The study covers the period 2024–2025 and is structured in two independent parts:

**Data Collection (`scripts/`)**
Two standalone CLI scripts written in Python that populate a PostgreSQL database.
These scripts are run once from the terminal before any analysis begins.
They have no dependency on the notebook and are never imported by it.

**Analysis (`notebooks/`)**
A Jupyter notebook that connects to the already-populated database and covers:
- Data validation and quality checks
- Correlation analysis between precipitation and arrival delay
- Predictive model: delay in minutes as a function of precipitation
- Documentation of methodology, limitations, and findings

The low explanatory power of precipitation as a sole predictor is acknowledged
as an expected limitation of the study. Confounding variables (day of week,
public holidays, season, incidents, infrastructure works) are discussed in the
notebook but not modelled.

---

## 2. Repository Structure

```
.
├── .github/
│   └── copilot-instructions.md
├── .gitignore                        # raw/ and .env must always be listed here
├── docker-compose.yml                # PostgreSQL + pgAdmin
├── db/
│   └── init.sql                      # Schema: all CREATE TABLE / CREATE VIEW / CREATE INDEX
├── scripts/
│   ├── collect_sbb.py                # CLI: download and process SBB monthly archives
│   ├── load_meteo.py                 # CLI: download and load MeteoSwiss precipitation files
│   ├── reset_db.py                   # CLI: drop and recreate all tables in a target database
│   ├── requirements.txt              # All Python dependencies for scripts
│   └── <module files>               # Supporting modules imported by the scripts
├── notebooks/
│   └── analysis.ipynb               # The single Jupyter notebook for analysis
├── tests/
│   ├── unit/                        # Pure function tests, no DB, no network
│   ├── integration/                 # Tests that require a running DB
│   └── fixtures/
│       └── 2024-01-01_istdaten.csv  # Real SBB CSV used for parser unit tests
├── raw/                             # Temp working directory — auto-created, never committed
│   ├── sbb/                         # SBB ZIPs and extracted CSVs (deleted after processing)
│   └── meteo/                       # MeteoSwiss CSVs (deleted after processing)
└── logs/                            # Log files from collection runs — auto-created, never committed
```

**Rules Copilot must always follow for the repository structure:**
- `raw/` is never committed. It is created automatically by the scripts at runtime.
- `.env` is never committed. It holds local DB credentials.
- `raw/` and `.env` must always be present in `.gitignore`.
- Schema changes always go into `db/init.sql`, never inline in scripts or notebooks.
- The notebook never imports from `scripts/`. It only connects to the database.

---

## 3. Data Model

The database name is `sbb_precipitation`. All tables are created by `db/init.sql`.

### Table: `precipitation_10min`

Stores the raw 10-minute precipitation readings loaded from the MeteoSwiss CSV files.
One row per station per 10-minute interval.

```sql
CREATE TABLE precipitation_10min (
    id             SERIAL PRIMARY KEY,
    station_abbr   VARCHAR(10)  NOT NULL,          -- 'SMA', 'BAS', 'BER'
    city           VARCHAR(20)  NOT NULL,           -- 'Zürich', 'Basel', 'Bern'
    measured_at    TIMESTAMP    NOT NULL,           -- parsed from reference_timestamp (UTC)
    precip_mm      FLOAT                            -- rre150z0, nullable (sensor gaps)
);

CREATE UNIQUE INDEX idx_precip_station_time
    ON precipitation_10min (station_abbr, measured_at);

CREATE INDEX idx_precip_city_time
    ON precipitation_10min (city, measured_at);
```

### Table: `train_connections`

One row per qualifying train arrival at a target station.
A qualifying arrival is a stop at Zürich HB, Basel SBB, or Bern where:
- `PRODUKT_ID` is `Zug`
- `AN_PROGNOSE_STATUS` is `REAL`
- The row is not the origin of the trip (there is an `ANKUNFTSZEIT`)

```sql
CREATE TABLE train_connections (
    id                          SERIAL PRIMARY KEY,
    betriebstag                 DATE          NOT NULL,
    fahrt_bezeichner            VARCHAR(100)  NOT NULL,
    destination_station         VARCHAR(50)   NOT NULL,  -- 'Zürich HB', 'Basel SBB', 'Bern'
    destination_city            VARCHAR(20)   NOT NULL,  -- 'Zürich', 'Basel', 'Bern'
    scheduled_arrival           TIMESTAMP     NOT NULL,  -- parsed from ANKUNFTSZEIT
    actual_arrival              TIMESTAMP     NOT NULL,  -- parsed from AN_PROGNOSE
    arrival_delay_min           FLOAT         NOT NULL,  -- actual_arrival - scheduled_arrival in minutes
    origin_station              VARCHAR(100),            -- HALTESTELLEN_NAME of the first stop of the trip
    origin_departure_scheduled  TIMESTAMP,               -- ABFAHRTSZEIT of the first stop of the trip
    trip_duration_min           FLOAT,                   -- scheduled_arrival - origin_departure_scheduled in minutes
    median_precip_mm            FLOAT,                   -- median of precipitation_10min over the trip window
    source_month                VARCHAR(7)    NOT NULL   -- 'YYYY-MM' for traceability
);

CREATE UNIQUE INDEX idx_connections_fahrt_arrival
    ON train_connections (fahrt_bezeichner, scheduled_arrival);

CREATE INDEX idx_connections_date_station
    ON train_connections (betriebstag, destination_station);
```

### Table: `processing_log`

Tracks completed processing runs so the scripts are safe to restart and resume.

```sql
CREATE TABLE processing_log (
    id             SERIAL PRIMARY KEY,
    run_type       VARCHAR(20)   NOT NULL,  -- 'sbb' or 'meteo'
    period         VARCHAR(10)   NOT NULL,  -- 'YYYY-MM' for SBB, station abbr for meteo
    status         VARCHAR(20)   NOT NULL,  -- 'success', 'error', 'partial'
    rows_inserted  INT,
    error_msg      TEXT,
    run_at         TIMESTAMPTZ   DEFAULT NOW()
);
```

### View: `analysis`

A denormalized view joining train connections with their precipitation context.
This is the primary view used by the notebook for all queries.

```sql
CREATE VIEW analysis AS
SELECT
    tc.betriebstag,
    tc.destination_station,
    tc.destination_city,
    tc.scheduled_arrival,
    tc.arrival_delay_min,
    tc.trip_duration_min,
    tc.median_precip_mm,
    CASE
        WHEN tc.median_precip_mm IS NULL  THEN 'unknown'
        WHEN tc.median_precip_mm = 0      THEN 'dry'
        WHEN tc.median_precip_mm < 0.5    THEN 'light'
        WHEN tc.median_precip_mm < 2.0    THEN 'moderate'
        ELSE                                   'heavy'
    END AS precip_category,
    EXTRACT(DOW  FROM tc.betriebstag) AS day_of_week,   -- 0=Sunday
    EXTRACT(MONTH FROM tc.betriebstag) AS month
FROM train_connections tc;
```

### Precipitation NULL handling

When computing `median_precip_mm` for a trip window, if some 10-minute intervals
within the window have `precip_mm = NULL` (sensor gap), they are excluded from
the median calculation. If ALL intervals in the window are NULL, the nearest
non-null value is used (forward-fill, then backward-fill). If no non-null value
exists for the entire day, `median_precip_mm` is stored as NULL.

### Overnight trip handling

Trips that cross midnight (origin departure on day X, arrival on day X+1) are
skipped. They are identified by comparing `betriebstag` of the destination stop
with the parsed date of `origin_departure_scheduled`.

---

## 4. Data Sources

### SBB Istdaten

**URL pattern:**
```
https://archive.opentransportdata.swiss/istdaten/{YYYY}/ist-daten-{YYYY}-{MM}.zip
```
Example: `https://archive.opentransportdata.swiss/istdaten/2024/ist-daten-2024-01.zip`

Each ZIP contains one CSV per operating day, named `YYYY-MM-DD_istdaten.csv`.
Each CSV uses `;` as separator and contains all train and bus movements in
Switzerland for that day.

**Columns used (all others are ignored):**

| Column | Description |
|---|---|
| `BETRIEBSTAG` | Operating day, format `DD.MM.YYYY` |
| `FAHRT_BEZEICHNER` | Trip ID — unique per trip per day |
| `PRODUKT_ID` | Transport mode — keep only `Zug` |
| `HALTESTELLEN_NAME` | Station name |
| `ANKUNFTSZEIT` | Scheduled arrival, format `DD.MM.YYYY HH:MM` |
| `AN_PROGNOSE` | Actual arrival, format `DD.MM.YYYY HH:MM:SS` |
| `AN_PROGNOSE_STATUS` | Keep only rows where this equals `REAL` |
| `ABFAHRTSZEIT` | Scheduled departure, format `DD.MM.YYYY HH:MM` |
| `FAELLT_AUS_TF` | Cancelled flag — `true`/`false` |

**Target stations:**
```python
TARGET_STATIONS = {"Zürich HB", "Basel SBB", "Bern"}
```

**Station to city mapping:**
```python
STATION_TO_CITY = {
    "Zürich HB": "Zürich",
    "Basel SBB": "Basel",
    "Bern":      "Bern",
}
```

### MeteoSwiss Precipitation

**Download URLs (one file per station, 10-min interval, 2020–2029):**
```
https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/sma/ogd-smn_sma_t_historical_2020-2029.csv
https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/bas/ogd-smn_bas_t_historical_2020-2029.csv
https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ber/ogd-smn_ber_t_historical_2020-2029.csv
```

**Station to city mapping:**
```python
METEO_STATIONS = {
    "SMA": "Zürich",
    "BAS": "Basel",
    "BER": "Bern",
}
```

**File format:** `;` separated. The timestamp column is named `reference_timestamp`
in historical files (note: it is `REFERENCE_TS` in recent files — this project
only uses historical files).

**Column used for precipitation:** `rre150z0` — 10-minute precipitation sum in mm.
All other columns in the file are ignored.

**Timestamp parsing:** `reference_timestamp` is in the format `DD.MM.YYYY HH:MM`.
Parse as UTC. Store as `TIMESTAMP` in PostgreSQL (no timezone).

---

## 5. Pipeline: `load_meteo.py`

**Purpose:** Download the three MeteoSwiss CSV files, load the relevant columns
into `precipitation_10min`, then delete the files (unless `--debug`).

**CLI flags:**
```
--debug    Keep downloaded CSV files in raw/meteo/ after loading
```

**Processing steps:**

1. Create `raw/meteo/` if it does not exist.
2. For each of the three stations (SMA, BAS, BER):
   a. Download the CSV from the known URL to `raw/meteo/{station_lower}_historical.csv`.
   b. Read only `station_abbr`, `reference_timestamp`, and `rre150z0`.
   c. Derive `city` from `station_abbr` using `METEO_STATIONS`.
   d. Parse `reference_timestamp` to a Python `datetime`.
   e. Rename `rre150z0` to `precip_mm`.
   f. Filter to rows where the year is between 2024 and 2025 inclusive.
   g. Bulk-insert into `precipitation_10min` using upsert on `(station_abbr, measured_at)`.
   h. Log the result to `processing_log`.
   i. Delete the CSV file unless `--debug`.
3. Print a summary: rows inserted per station.

---

## 6. Pipeline: `collect_sbb.py`

**Purpose:** Download SBB monthly ZIP archives, extract per-day CSVs, parse
qualifying train arrivals with their trip origin, enrich with precipitation data
from the database, insert into `train_connections`, then clean up.

**CLI flags:**
```
--start-year  INT           First year to process (e.g. 2024)
--end-year    INT           Last year to process (e.g. 2025)
--months      INT[,INT...]  Comma-separated list of months to process (e.g. 1,2 or 10,11,12)
                            If omitted, all 12 months are processed for each year.
--debug       Keep downloaded ZIP files and extracted CSVs in raw/sbb/ after processing
```

**Example invocations:**
```bash
# Process Jan–Feb 2024 only (for testing)
python collect_sbb.py --start-year 2024 --end-year 2024 --months 1,2

# Process full 2024–2025 range
python collect_sbb.py --start-year 2024 --end-year 2025

# Process only October and November across both years, keep files
python collect_sbb.py --start-year 2024 --end-year 2025 --months 10,11 --debug
```

**Processing steps per month:**

1. Check `processing_log`: skip if this month already has a `success` entry.
2. Check available disk space in `raw/sbb/`. If less than 10 GB free, abort
   immediately with a clear error message. Do not attempt the download.
3. Download `ist-daten-{YYYY}-{MM}.zip` to `raw/sbb/` with tqdm progress bar.
   - If a partial ZIP already exists from a previous failed run, delete it first.
   - Retry failed downloads up to 3 times with exponential backoff
     (wait 10s, 30s, 90s between attempts).
   - If all 3 retries fail, log the error, write an `error` entry to
     `processing_log`, and continue with the next month.
4. For each daily CSV inside the ZIP:
   a. Read only the required columns (see Section 4).
   b. Filter to `PRODUKT_ID == 'Zug'` and `HALTESTELLEN_NAME` in `TARGET_STATIONS`
      and `AN_PROGNOSE_STATUS == 'REAL'` and `FAELLT_AUS_TF == False`.
   c. For each qualifying stop, find the origin of the same `FAHRT_BEZEICHNER`
      within the same daily file: the stop with no `ANKUNFTSZEIT` (first stop),
      or the stop with the earliest `ABFAHRTSZEIT`.
   d. Skip trips where the parsed date of `origin_departure_scheduled` differs
      from `betriebstag` (overnight trips).
   e. Compute `arrival_delay_min` and `trip_duration_min`.
   f. Compute `median_precip_mm` by querying `precipitation_10min` for the
      matching city over the window `[origin_departure_scheduled, scheduled_arrival]`.
      Apply NULL fill strategy as defined in Section 3.
   g. Collect all rows for the month, then bulk-insert into `train_connections`
      using upsert on `(fahrt_bezeichner, scheduled_arrival)`.
5. Open a fresh database connection for each month's insert. Do not reuse a
   connection across months — long-running connections can time out over a
   multi-hour run.
6. Log the result to `processing_log`.
7. Delete the ZIP and any extracted files unless `--debug`.

**stdout layout:**

```
Months:  [████████████████████] 2/2

<blank line>
Downloading ist-daten-2024-01.zip: [██████████] 1.2GB/1.2GB
Processing 2024-01:                [██████████] 31/31 days
<blank line>
Downloading ist-daten-2024-02.zip: [██████████] 1.1GB/1.1GB
Processing 2024-02:                [██████████] 28/28 days
<blank line>
---
Total months configured: 2
Completed months:        2
Rows in train_connections:    180594
Rows in precipitation_10min:  262800
```

**tqdm configuration:**
- The months bar uses `position=0` and `leave=True` so it stays fixed at the top.
- The download and processing bars use `position=1` and `leave=False` so they
  are replaced by the next bar when done.
- A blank line is printed before and after each month block using `tqdm.write()`.

**Log file:**
- In addition to stdout, write all log output to `logs/collection_YYYYMMDD_HHMMSS.log`
  where the timestamp is the moment the script started.
- Create the `logs/` directory automatically if it does not exist.
- `logs/` must be added to `.gitignore`.
- The log file captures everything the `logging` module emits (INFO and above).
  tqdm output is not duplicated into the log file.

**Running unattended on a remote VM:**
- The README must include instructions for running the script detached via `tmux`:
  ```bash
  tmux new -s sbb_collection
  python collect_sbb.py --start-year 2024 --end-year 2025
  # Detach with Ctrl+B then D
  # Reattach later with: tmux attach -t sbb_collection
  ```
- The README must note that if the process is interrupted, it can be safely
  restarted with the same flags — completed months will be skipped automatically
  via `processing_log`.

---

## 7. Script: `reset_db.py`

**Purpose:** Drop all tables and views in a target database and recreate them
from `db/init.sql`. Used to wipe the production database before a full re-run,
and used automatically by the integration test suite to reset the test database.

**CLI flags:**
```
--database  Name of the database to reset (required, no default — must be explicit)
--yes       Skip the confirmation prompt (required for non-interactive use in tests)
```

**Behavior:**
- Without `--yes`, print a confirmation prompt:
  ```
  This will permanently delete all data in database 'sbb_precipitation'.
  Type the database name to confirm:
  ```
  Abort if the user input does not match exactly.
- With `--yes`, skip the prompt entirely (used by integration tests).
- Drop all views first, then all tables, in dependency order:
  1. DROP VIEW IF EXISTS `analysis`
  2. DROP TABLE IF EXISTS `train_connections`
  3. DROP TABLE IF EXISTS `precipitation_10min`
  4. DROP TABLE IF EXISTS `processing_log`
- Re-apply `db/init.sql` to recreate the full schema.
- Print a confirmation line on success: `Database '{name}' reset successfully.`

**Example invocations:**
```bash
# Interactive reset of the production database
python reset_db.py --database sbb_precipitation

# Non-interactive reset of the test database (used in test fixtures)
python reset_db.py --database sbb_precipitation_test --yes
```

**Safety rules Copilot must always enforce for this script:**
- `--database` has no default value. The script must always fail with a clear
  error if `--database` is not provided.
- The script must never fall back to a hardcoded database name.
- The script must refuse to run if `--yes` is passed without `--database`.

---

## 8. Jupyter Notebook Structure

The notebook is `notebooks/analysis.ipynb`. It connects to the database and
assumes it is already fully populated by the collection scripts.

The notebook must contain the following sections in order, each as a Markdown
heading with code cells beneath it:

### 0. Setup
- Import all libraries
- Load DB connection from environment variables (same `.env` as the scripts)
- Print row counts from `train_connections`, `precipitation_10min`, and `processing_log`
  as a sanity check

### 1. Data Validation
- Check for unexpected NULLs in critical columns
- Distribution of `arrival_delay_min` (histogram): identify and discuss outliers
- Distribution of `median_precip_mm`
- Check coverage: how many connections have a non-null `median_precip_mm`
- Check date range completeness: are there any missing months?
- Flag and document any data quality issues found

### 2. Exploratory Analysis
- Arrival delay by station (boxplot per station)
- Arrival delay by month (seasonality check)
- Arrival delay by day of week
- Precipitation distribution per city per month
- Scatter plot: `median_precip_mm` vs `arrival_delay_min` per station

### 3. Correlation Analysis
- Pearson and Spearman correlation between `median_precip_mm` and `arrival_delay_min`
  computed per station and overall
- Correlation by precipitation category (dry / light / moderate / heavy)
- Statistical significance (p-values reported for each correlation)
- Discussion of findings and limitations

### 4. Predictive Model
- Feature: `median_precip_mm`
- Target: `arrival_delay_min`
- Model: Linear Regression as baseline; Random Forest as comparison
- Train/test split: 80/20, split by date (not random) to avoid data leakage
- Evaluation metrics: MAE, RMSE, R²
- Residual plot and prediction vs. actual plot
- Discussion of model performance and limitations

### 5. Conclusion
- Summary of findings
- Acknowledged limitations: single feature, confounders not modelled,
  data quality caveats, MeteoSwiss station proximity to actual train route
- Suggestions for further work (additional features, longer time range)

---

## 9. Conventions

### Python Style
Follow the Zen of Python (`import this`) in all code written for this project.
Specifically:
- Explicit is better than implicit — no magic values, no undocumented behavior.
- Simple is better than complex — prefer the straightforward solution.
- Flat is better than nested — reduce nesting with early returns and guard clauses.
- Readability counts — variable and function names must be self-explanatory.
- Errors should never pass silently — all exceptions must be caught, logged,
  and re-raised or handled explicitly.

### File Size and Modularity
- No file should contain more than ~150 lines of code (excluding docstrings/comments).
- Split logic into small, single-purpose functions that can be tested in isolation.
- Group related functions into separate module files imported by the main scripts.
- A function that does more than one thing should be split into two functions.

### Naming Conventions
- Files and modules: `snake_case.py`
- Functions and variables: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- No abbreviations unless universally understood (e.g. `db`, `csv`, `url`)

### Timezone Handling
- All timestamps are stored and processed as UTC with no timezone offset.
- The SBB data uses Swiss local time (CET/CEST). Convert to UTC on ingestion.
- MeteoSwiss `reference_timestamp` is already in UTC. Do not convert.
- Never compare SBB and MeteoSwiss timestamps without first ensuring both are UTC.

### Database Access
- All DB connections use the credentials from `.env` via `python-dotenv`.
- Never hardcode credentials.
- Always use parameterized queries. Never use string formatting for SQL.
- Always close connections explicitly or use context managers.

### Error Handling and Logging
- Use Python's `logging` module. Never use bare `print()` for diagnostic output
  (only `tqdm.write()` for tqdm-compatible output and the final summary print).
- Log at `INFO` for normal progress, `WARNING` for skipped records, `ERROR` for
  recoverable failures, `CRITICAL` for unrecoverable ones.
- If a single daily CSV fails to parse, log the error and continue with the next day.
  Do not abort the entire month.
- If a monthly download fails (HTTP error, network timeout), log the error, write
  an `error` entry to `processing_log`, and continue with the next month.

### Testing

**General rules:**
- All tests live in `tests/`.
- Unit tests live in `tests/unit/`. They must have no external dependencies
  (no database, no network, no file system access outside `tests/fixtures/`).
- Integration tests live in `tests/integration/`. They require a running
  PostgreSQL instance (the Docker container).
- Use `pytest` as the test runner.
- Every function that parses, transforms, or computes data must have at least
  one unit test.

**Test database:**
- Integration tests never use the production database `sbb_precipitation`.
- Integration tests use a dedicated database named `sbb_precipitation_test`.
- This database must be created in the Docker PostgreSQL instance alongside
  the production database. Add it to `docker-compose.yml` via the
  `POSTGRES_MULTIPLE_DATABASES` pattern or a second init script.
- A `session`-scoped pytest fixture in `tests/integration/conftest.py` must
  call `reset_db.py --database sbb_precipitation_test --yes` before the
  integration test session begins, ensuring a clean schema for every run.
- Integration tests must read DB credentials from a `.env.test` file, not
  `.env`. `.env.test` must also be listed in `.gitignore`.
- The `DB_NAME` value in `.env.test` must always be `sbb_precipitation_test`.
  Copilot must never allow integration tests to connect to `sbb_precipitation`.

**Unit tests — SBB CSV parsing:**
- The SBB CSV parsing function must be tested using
  `tests/fixtures/2024-01-01_istdaten.csv` as input. Tests must assert:
  - Only rows with `PRODUKT_ID == 'Zug'` are returned
  - Only rows with `AN_PROGNOSE_STATUS == 'REAL'` are returned
  - Only rows for `TARGET_STATIONS` are returned
  - `arrival_delay_min` is computed correctly for a known row
  - Cancelled trains (`FAELLT_AUS_TF == True`) are excluded

**Integration tests — end-to-end:**
- The integration test suite must include at least one end-to-end test for
  `collect_sbb.py` that:
  - Resets `sbb_precipitation_test` via the session fixture
  - Downloads exactly one monthly ZIP
  - Processes it against the test database
  - Queries the test database and asserts that `train_connections` contains
    rows for all three target stations with valid `arrival_delay_min` values

### `.gitignore` (mandatory entries)
```
raw/
logs/
.env
.env.test
__pycache__/
*.pyc
.pytest_cache/
*.egg-info/
dist/
.ipynb_checkpoints/
```