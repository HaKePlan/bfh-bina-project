# GitHub Copilot Instructions

## 1. Project Overview

This project is a Business Intelligence study that investigates the correlation
between precipitation and train delays at three major Swiss railway stations:
Zürich HB, Basel SBB, and Bern.

The study covers the period 2022–2025 and is structured in three independent parts:

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

**Phase 5 — Web Application (`app/`)**
A Streamlit application that loads the trained model and fetches the MeteoSwiss
7-day precipitation forecast via the Open-Meteo API. It allows users to explore
expected arrival delays per station based on real forecast data and manual input.
The app has no database dependency — it only needs `models/model_all_data.pkl`
and an internet connection. It is packaged as a Docker container.

---

## Current Project State

**Phases 1–4 are complete.** Do not modify, regenerate, or overwrite any of the
following unless explicitly asked:
- All scripts in `scripts/` (collect_sbb.py, load_meteo.py, reset_db.py, db_utils.py,
  sbb_parser.py, precipitation.py)
- The database schema in `db/init.sql`
- The notebook `notebooks/analysis.ipynb`
- All tests in `tests/`

**Phase 5 is the current focus.** All new work happens inside `app/`.

**Key facts about the completed analysis (do not contradict these):**
- Dataset: 4,100,349 train connections, 45 months (January 2022 – March 2025)
- Stations: Zürich HB, Basel SBB, Bern
- Overall Spearman ρ = 0.074 (precipitation vs. arrival delay)
- Winning model: Random Forest, RMSE=6.610, MAE=1.462, R²=–0.000
- Saved model file: `models/model_all_data.pkl`
- The precipitation effect is a threshold effect (dry vs. wet), not linear
- The model takes `median_precip_mm` (10-min equivalent) as its single feature


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
├── models/
│   └── .gitkeep                     # Trained model saved here after notebook Section 4 runs
├── app/
│   ├── app.py                       # Streamlit application entry point
│   ├── requirements.txt             # App-specific Python dependencies
│   ├── Dockerfile                   # Container definition for the Streamlit app
│   └── <module files>              # Supporting modules imported by app.py
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
- `.env` is never committed. It holds local dev DB credentials.
- `.env.prod` is never committed. It holds production DB credentials used by the notebook.
- `.env.test` is never committed. It holds test DB credentials used by integration tests.
- `raw/`, `.env`, `.env.prod`, and `.env.test` must always be present in `.gitignore`.
- Schema changes always go into `db/init.sql`, never inline in scripts or notebooks.
- The notebook connects to the production DB via `.env.prod` — never `.env`.
- The app (`app/`) never connects to any database.

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

**General rules for all sections:**
- Every code block must be preceded by a narrative markdown cell explaining
  what is being done and why — this is a study document, not just code.
- Every chart must be followed by an interpretation markdown cell explaining
  what the chart shows and what conclusion can be drawn.
- All charts must have a title, labeled axes, and a legend where applicable.
- Use seaborn for all statistical plots (boxplots, distributions, heatmaps)
  with matplotlib for layout and customization.
- Use the style set in Section 0 (`seaborn-v0_8-darkgrid`, `husl` palette)
  consistently across all charts.
- Assign a consistent color per station across all charts:
  Zürich HB = blue, Basel SBB = red, Bern = green.
- The notebook must run correctly against any amount of data — one month or
  24 months — without any code changes.
- All sections must be fully implemented with no TODOs left.
- Commit the notebook with output cells cleared.

The notebook must contain the following sections in order:

### 0. Setup
- Import all libraries: `pandas`, `numpy`, `matplotlib`, `seaborn`,
  `scipy.stats`, `sklearn`, `joblib`, `dotenv`, `sqlalchemy`
- Load DB connection from environment variables via `.env`
- Create SQLAlchemy engine
- Print row counts from `train_connections`, `precipitation_10min`, and
  `processing_log` as a sanity check

### 1. Data Validation

**NULL check:**
- Check for NULLs in `arrival_delay_min`, `median_precip_mm`, `trip_duration_min`
- Print counts and percentages

**Arrival delay distribution:**
- Plot a histogram of `arrival_delay_min` showing the full distribution
- Do not cap or clip the values
- Follow with an interpretation markdown cell that:
  - States the mean, median, and 95th percentile delay
  - Identifies what counts as an outlier (e.g. delays > 60 min) and how many
    there are as a percentage of total
  - Notes that outliers are kept in the dataset as they are valid measurements

**Precipitation distribution:**
- Plot a histogram of `median_precip_mm` (excluding NULLs)
- Print coverage: percentage of connections with non-null `median_precip_mm`

**Date range completeness:**
- Query `processing_log` to show which months have been successfully loaded
- Print a table of loaded months per station
- Add a markdown note: "If months are missing, re-run `collect_sbb.py` for
  those months. Results below are based on available data only."

**Data quality summary:**
- End Section 1 with a markdown cell summarising all findings and flagging
  any issues to be aware of in the subsequent analysis

### 2. Exploratory Analysis

All plots use the consistent station color scheme (Zürich=blue, Basel=red,
Bern=green) and are followed by an interpretation markdown cell.

- **Delay by station**: boxplot of `arrival_delay_min` per `destination_station`
- **Delay by month**: line plot of median monthly delay per station
  (only rendered if more than one month of data is available)
- **Delay by day of week**: boxplot of `arrival_delay_min` per `day_of_week`
- **Precipitation by city per month**: bar chart of mean `median_precip_mm`
  per city per month (only rendered if more than one month is available)
- **Scatter plot**: `median_precip_mm` vs `arrival_delay_min`, one subplot
  per station, with a regression line overlay

### 3. Correlation Analysis

- **Pearson and Spearman correlation table**: compute both coefficients and
  their p-values using `scipy.stats` for each station and overall. Present
  as a formatted pandas DataFrame table.
- **Correlation heatmap**: seaborn heatmap of the correlation matrix. Annotate
  cells with the coefficient value.
- **Correlation by precipitation category**: compute mean delay per
  `precip_category` (dry / light / moderate / heavy) per station. Present
  as both a bar chart and a printed table.
- **Interpretation markdown**: discuss statistical significance (p < 0.05
  threshold), direction and strength of correlation, and any differences
  between stations.
- **Limitations markdown**: note that correlation does not imply causation,
  that confounding variables are not controlled for, and that the single
  predictor limits explanatory power.

### 4. Predictive Model

**Data sufficiency check (must run before any modelling):**
- Count the number of distinct months in `train_connections`
- If fewer than 6 months are available, print the following warning and
  skip all modelling code in this section entirely:
  ```
  ⚠️  Insufficient data for reliable model training.
      Only {n} month(s) loaded — minimum 6 required.
      Re-run this section after the full dataset is collected.
  ```
- If 6 or more months are available, proceed with the steps below.

**Feature and target:**
- Feature: `median_precip_mm` (drop NULLs before modelling)
- Target: `arrival_delay_min`

**Train/test split:**
- Split 80/20 by date (temporal split — sort by `betriebstag`, take first
  80% as train, last 20% as test). Never use random split to avoid data leakage.

**Models:**
- Linear Regression (scikit-learn) as baseline
- Random Forest Regressor (scikit-learn) as comparison
- Train both on the training set

**Evaluation:**
- Compute MAE, RMSE, and R² for both models on the test set
- Print a comparison table of metrics for both models
- Plot residuals for both models
- Plot predicted vs actual for both models

**Model selection and persistence:**
- Automatically select the model with the lower RMSE on the test set
- Save the winning model to `models/model.pkl` using `joblib.dump`
- Save the model metadata (model type, RMSE, MAE, R², training date,
  number of training samples) to `models/model_metadata.json`
- Create the `models/` directory automatically if it does not exist
- Print: `✓ Model saved: {model_type} (RMSE: {rmse:.3f})`
- `models/*.pkl` and `models/*.json` must be added to `.gitignore`
  so the trained model is not committed but the empty `models/` folder is
  (add a `.gitkeep` file to `models/`)

**Discussion markdown:**
- Interpret the metrics in plain language
- Acknowledge that R² will likely be low due to the single predictor
- Note that the model is saved and ready for use in Phase 5

### 5. Conclusion
- Summary of findings from Sections 2, 3, and 4
- Acknowledged limitations: single feature, confounders not modelled,
  data quality caveats, MeteoSwiss station proximity to actual train route
- Suggestions for further work (additional features, longer time range)
- This section must be written as prose markdown cells, not code
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
models/*.pkl
models/*.json
.env
.env.prod
.env.test
__pycache__/
*.pyc
.pytest_cache/
*.egg-info/
dist/
.ipynb_checkpoints/
```

---

## 10. Phase 5 — Streamlit Web Application

### Purpose

A Streamlit app that presents the trained delay prediction model to an end user.
It fetches the real MeteoSwiss 7-day hourly precipitation forecast via the
Open-Meteo API and converts it into predicted arrival delays per station.

The app has **no database dependency**. It requires only:
- `models/model_all_data.pkl` — the trained scikit-learn model
- An internet connection to call the Open-Meteo API

### Station Configuration

```python
STATIONS = {
    "Zürich HB":  {"city": "Zürich",  "lat": 47.3779, "lon": 8.5403},
    "Basel SBB":  {"city": "Basel",   "lat": 47.5476, "lon": 7.5898},
    "Bern":       {"city": "Bern",    "lat": 46.9481, "lon": 7.4474},
}
```

### Precipitation Category Scale

Used for alert colour coding throughout the app. These thresholds match
`db/init.sql` exactly and must not be changed:

| Category | `median_precip_mm` range | Alert |
|---|---|---|
| dry      | = 0.000 mm               | 🟢    |
| light    | > 0.000 and < 0.500 mm   | 🟡    |
| moderate | ≥ 0.500 and < 2.000 mm   | 🟠    |
| heavy    | ≥ 2.000 mm               | 🔴    |

### Model Architecture Note

The model takes `median_precip_mm` as its **single feature** — it has no
station feature. This means the model returns the same predicted delay for
the same precipitation value regardless of station. Station differentiation
in the app comes from different weather forecasts per location (different
lat/lon → different precipitation from Open-Meteo), not from different
model behaviour per station.

### Precipitation Unit Conversion

The model was trained on `median_precip_mm` — the median of 10-minute
precipitation sums over a trip window.

The Open-Meteo API returns hourly precipitation (`precipitation` in mm/h).

**Conversion:** divide the hourly value by 6 to obtain an approximate
10-minute equivalent before passing it to the model.

```python
precip_10min = hourly_precip_mm_per_hour / 6.0
```

**Approximation note:** this divides a 1-hour sum by 6 to get an average
10-minute portion. The training data used the *median* of actual 10-minute
readings, not an average of an hourly total. This is a known approximation —
there is no way to get 10-minute forecast granularity from Open-Meteo.

This conversion must be applied consistently in both the forecast section
and the manual input section. The UI must display both the raw hourly
value (mm/h) and the converted value passed to the model (mm/10min)
so the user understands the transformation.

### Open-Meteo API

**Endpoint:** `https://api.open-meteo.com/v1/forecast`

**Parameters:**
```
latitude        = station latitude
longitude       = station longitude
hourly          = precipitation
models          = meteoswiss_icon_ch2   (⚠️ verify this value — see implementation note below)
forecast_days   = 7
timezone        = Europe/Zurich
```

**Implementation prerequisite — verify model parameter:**
Before writing any code, manually test the API call with
`models=meteoswiss_icon_ch2`. If it returns an error, try `icon_ch` or omit
the `models` parameter entirely. Store the working value as a `FORECAST_MODEL`
constant in `forecast.py` so it is easy to change.

**Response structure:**
```json
{
  "hourly": {
    "time": ["2024-01-01T00:00", "2024-01-01T01:00", ...],
    "precipitation": [0.0, 0.1, ...]
  }
}
```

The `time` values are ISO 8601 strings in the specified timezone.
Parse them with `pd.to_datetime()`.

**Error handling:** if the API call fails (network error, HTTP error, timeout),
display a clear error message in the app and do not crash. Always wrap API
calls in try/except.

**Caching:** cache the API response for 1 hour using `@st.cache_data(ttl=3600)`
to avoid unnecessary API calls on every user interaction.

### App Structure

The app has two sections rendered in order on a single page.
Both sections use the **sidebar-selected station** — the station selector
applies globally to the entire app.

---

#### Section 1 — 7-Day Forecast View

**Purpose:** show the user what delays to expect based on the real MeteoSwiss
forecast for their planned arrival day and time.

**User inputs (in the sidebar):**
- Station selector: `st.selectbox` with options Zürich HB, Basel SBB, Bern
  (applies to both Section 1 and Section 2)
- Planned arrival time: `st.time_input` (HH:MM, default 08:00)

**Processing:**
1. Fetch 7-day hourly forecast for the selected station's coordinates.
2. For each of the 7 forecast days, extract the hourly precipitation value
   at the hour matching the user's planned arrival time.
3. Convert to 10-min equivalent (÷ 6).
4. Pass to model to get predicted delay in minutes.
5. Derive precipitation category and alert emoji from the thresholds above.

**Display:** a table or card layout showing one row per day:

| Date | Day | Precip (mm/h) | Model input (mm/10min) | Alert | Predicted delay |
|---|---|---|---|---|---|
| 2024-01-15 | Mon | 1.2 mm/h | 0.20 mm/10min | 🟡 | +0.9 min |

- Dates must be formatted as `DD.MM.YYYY` with the day name in German
  (Mo, Di, Mi, Do, Fr, Sa, So).
- Predicted delay must be shown as `+X.X min` with a `+` prefix.
- The alert emoji column must be large and visually prominent.
- Today's row must be visually highlighted (e.g. bold or background colour).

---

#### Section 2 — Manual Precipitation Input

**Purpose:** let the user explore "what if" scenarios by entering their own
precipitation value and seeing the predicted delay for the selected station.

**User input:**
- A slider or number input for precipitation intensity in **mm/h**
  (range 0.0 to 20.0, step 0.1, default 0.0).
- Label: `Precipitation intensity (mm/h)`
- Below the input, show a helper text explaining the category scale:
  ```
  0 mm/h = dry  |  < 3 mm/h = light  |  3–12 mm/h = moderate  |  > 12 mm/h = heavy
  ```
  (These are the hourly equivalents of the 10-min thresholds × 6.)

**Processing:**
1. Convert user input to 10-min equivalent (÷ 6).
2. Pass to model for the sidebar-selected station.
3. Derive category and alert.

**Display:** a single `st.metric` card (not a table) showing:
- The alert emoji and precipitation category
- The converted model input (mm/10min)
- The predicted delay as `+X.X min`

This is for the sidebar-selected station only. The model has no station
feature, so showing all three stations would display identical values.

---

#### Footer / Disclaimer

At the bottom of the page, always display:

```
⚠️ This prediction is based on a single-predictor model (R² ≈ 0.000).
Precipitation explains less than 0.25% of delay variance.
This tool indicates a general trend only — not a precise per-trip forecast.
Hourly forecast precipitation is divided by 6 as an approximation of the
10-minute input the model expects.
Data source: MeteoSwiss ICON CH2 via Open-Meteo (open-meteo.com).
Model trained on SBB Istdaten 2024–2025.
```

---

### Model Loading

Load the model once at startup using `@st.cache_resource`:

```python
@st.cache_resource
def load_model():
    import joblib
    from pathlib import Path
    model_path = Path(__file__).parent.parent / "models" / "model_all_data.pkl"
    return joblib.load(model_path)
```

**Important:** the app must be run from the project root directory:
```bash
streamlit run app/app.py
```
Running `cd app && streamlit run app.py` will fail because the model path
resolves relative to `app.py`'s location. This is documented in the README.

If the model file does not exist, display a clear error:
```
Model file not found at models/model_all_data.pkl.
Run the analysis notebook (Section 4) first to train and save the model.
```

### Import Strategy

Because the entry point is named `app.py` inside an `app/` package, Streamlit's
runtime causes the filename to shadow the package name. To support both
Streamlit runtime and pytest (which resolves `app` as a package), all cross-module
imports use a try/except pattern:

```python
try:
    from app.prediction import convert_hourly_to_10min   # pytest
except ImportError:
    from prediction import convert_hourly_to_10min        # streamlit run
```

Additionally, `app.py` injects its own directory onto `sys.path` at startup:
```python
sys.path.insert(0, str(Path(__file__).parent))
```

### Null Precipitation in Forecasts

The Open-Meteo API returns `null` for precipitation values in the tail end of
the 7-day forecast window (typically the last ~48 hours). These are converted
to `0.0` via `fillna(0.0)` in `fetch_forecast()`. The `build_forecast_table()`
function also coerces values with `float(value or 0.0)` as a safety measure.

### File Size and Modularity

Follow the same modularity rules as the scripts (Section 9):
- `app.py` — Streamlit layout only, no business logic
- `forecast.py` — Open-Meteo API call, caching, response parsing
- `prediction.py` — model loading, precipitation conversion, delay prediction,
  category classification
- No file should exceed ~150 lines

### Testing (TDD)

All app business logic must be developed using test-driven development
(red-green-refactor). Write failing tests first, then implement.

App tests live in `app/tests/` (not `tests/unit/`) so they can be run
independently from the pipeline tests which take several minutes:

```bash
# App tests only (~0.3s)
pytest app/tests/

# Pipeline tests only (~90s)
pytest tests/unit/

# All tests
pytest tests/ app/tests/
```

**`app/tests/test_prediction.py`** — tests for `app/prediction.py`:
- `convert_hourly_to_10min()` — verify division by 6 for known values
- `classify_precip_category()` — verify all 4 categories and boundary values
  (0.0, 0.499, 0.5, 1.999, 2.0)
- `predict_delay()` — mock the model, verify it calls `model.predict()`
  with the correct shaped input and returns a float
- `load_model()` — verify `FileNotFoundError` when model file is missing

**`app/tests/test_forecast.py`** — tests for `app/forecast.py`:
- `fetch_forecast()` — mock `requests.get` with a sample JSON response,
  verify returned DataFrame has `time` and `precipitation` columns
- `extract_precip_at_hour()` — create a synthetic DataFrame with 168 hourly
  rows (7 days × 24 hours), verify filtering to 7 rows at the target hour
- `build_forecast_table()` — mock model + synthetic forecast data, verify
  output DataFrame has expected columns (date, day name, precip, alert, delay)

All tests must have no external dependencies (no network, no model file on
disk). Use `unittest.mock` or `pytest` fixtures for mocking.

### Docker

**`app/Dockerfile`:**
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ ./app/
COPY models/model_all_data.pkl ./models/
EXPOSE 8501
CMD ["streamlit", "run", "app/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]
```

**Build and run:**
```bash
# Build
docker build -f app/Dockerfile -t sbb-delay-app .

# Run
docker run -p 8501:8501 sbb-delay-app
```

The app is then accessible at `http://localhost:8501`.

**Important:** the `models/model_all_data.pkl` file must exist before building
the Docker image. The Dockerfile copies it from the project root's `models/`
directory. The README must document this prerequisite clearly.

### README additions

The README must include a **Phase 5 — Web Application** section with:
1. Prerequisites: model file must be trained first (run notebook Section 4)
2. Local run without Docker: `streamlit run app/app.py`
3. Docker build and run commands
4. Screenshot or description of what the app shows
5. Disclaimer about model limitations (same text as the in-app footer)

### `.gitignore` addition

`models/model_all_data.pkl` is already gitignored via `models/*.pkl`.
No additional entries needed for the app.

### Implementation Plan (ordered steps)

1. **Verify Open-Meteo API model parameter.** `curl` or browser-test
   `models=meteoswiss_icon_ch2`. If it fails, use `icon_ch` or omit.
   Store result as `FORECAST_MODEL` constant.

2. **TDD: `app/prediction.py`** — write `tests/unit/test_prediction.py`
   first (red), then implement (green), then refactor. Functions:
   `convert_hourly_to_10min`, `classify_precip_category`, `predict_delay`,
   `load_model`.

3. **TDD: `app/forecast.py`** — write `tests/unit/test_forecast.py`
   first (red), then implement (green), then refactor. Functions:
   `fetch_forecast`, `extract_precip_at_hour`, `build_forecast_table`.
   Constants: `STATIONS`, `FORECAST_MODEL`, `API_ENDPOINT`.

4. **Create `app/app.py`** — Streamlit layout wiring only (~100 lines).
   Sidebar inputs, Section 1 forecast table, Section 2 metric card,
   footer disclaimer.

5. **Create `app/requirements.txt`** — `streamlit`, `pandas`, `requests`,
   `joblib`, `scikit-learn`, `numpy`.

6. **Create `app/Dockerfile`** — per spec above.

7. **Update `README.md`** — add Phase 5 section with prerequisites, local
   run command (from project root), Docker build/run, description, disclaimer.
