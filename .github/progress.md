# Project Progress: SBB Precipitation Study

**Last Updated:** 2026-04-20  
**Project Status:** 🟡 In Development (Data Collection & Analysis Pipeline Built)

---

## Overview

This document tracks the progress of the Business Intelligence study investigating the correlation between precipitation and train delays at three major Swiss railway stations (Zürich HB, Basel SBB, and Bern) for the period 2024–2025.

The project is divided into two independent phases:
1. **Data Collection** (Scripts-based, one-time setup)
2. **Analysis** (Jupyter notebook, iterative analysis)

---

## Phase 1: Infrastructure & Database Setup

### Status: ✅ COMPLETE

- [x] Repository structure established
- [x] Docker Compose setup with PostgreSQL + pgAdmin
- [x] Database schema created (`db/init.sql`)
- [x] Environment configuration (`.env` template, `.gitignore`)
- [x] Python project setup (`pyproject.toml`, `requirements.txt`)
- [x] pytest configuration

### Deliverables
- **Docker Compose**: Includes PostgreSQL 15 and pgAdmin for local development
- **Database Schema**: 
  - `precipitation_10min` — 10-minute MeteoSwiss precipitation readings
  - `train_connections` — SBB train arrivals with delay and precipitation context
  - `processing_log` — Audit trail of script executions
  - `analysis` view — Denormalized view for notebook queries
- **Dependencies**: Python 3.8+, PostgreSQL 12+, required packages in `scripts/requirements.txt`

---

## Phase 2: Data Collection Pipeline

### Status: ✅ COMPLETE

#### 2.1 SBB Data Collection (`scripts/collect_sbb.py`)

**Completed Features:**
- [x] CLI argument parsing (--start-year, --end-year, --months, --debug)
- [x] ZIP download with streaming and progress bars (tqdm)
- [x] Per-day CSV extraction and parsing
- [x] Train arrival filtering (PRODUKT_ID='Zug', AN_PROGNOSE_STATUS='REAL', target stations only)
- [x] Trip origin tracking (first stop of journey)
- [x] Overnight trip filtering (skip trips crossing midnight)
- [x] Arrival delay calculation (actual - scheduled in minutes)
- [x] Trip duration calculation (scheduled arrival - origin departure in minutes)
- [x] Precipitation enrichment (median from 10-min readings over trip window)
- [x] NULL fill strategy for missing precipitation data (forward-fill, backward-fill)
- [x] Upsert into `train_connections` (deduplication by fahrt_bezeichner + scheduled_arrival)
- [x] Processing log for crash recovery and resume capability
- [x] Error handling and logging

**Module Architecture:**
- `collect_sbb.py` — Main CLI entry point
- `sbb_parser.py` — CSV parsing and row filtering logic
- `db_utils.py` — Database connection, upsert, logging utilities
- `precipitation.py` — Precipitation cache and median calculation

#### 2.2 MeteoSwiss Precipitation Loading (`scripts/load_meteo.py`)

**Completed Features:**
- [x] CLI argument parsing (--debug flag)
- [x] Download from three MeteoSwiss sources (SMA, BAS, BER)
- [x] CSV parsing with selective column loading
- [x] UTC timestamp handling (reference_timestamp format parsing)
- [x] Year filtering (2024–2025 only)
- [x] Upsert into `precipitation_10min` (deduplication by station_abbr + measured_at)
- [x] Processing log entries
- [x] Error handling and logging
- [x] Optional file retention for debugging (--debug)

#### 2.3 Database Reset Utility (`scripts/reset_db.py`)

**Completed Features:**
- [x] CLI argument parsing (--database required, --yes flag)
- [x] Confirmation prompt (interactive or skipped with --yes)
- [x] Dependency-aware DROP VIEW/TABLE order
- [x] Schema recreation from `db/init.sql`
- [x] Safety rails (no default DB, must be explicit)
- [x] Integration test compatibility

---

## Phase 3: Testing Infrastructure

### Status: ⚠️ PARTIAL (Unit tests present, integration tests need expansion)

#### 3.1 Unit Tests

**Completed:**
- [x] `tests/unit/test_sbb_parser.py` — CSV parsing, row filtering, delay calculation
- [x] `tests/unit/test_load_meteo.py` — Timestamp parsing, UTC handling
- [x] Test fixtures available:
  - `tests/fixtures/2024-01-01_istdaten.csv` — Real SBB data for parser validation
  - `tests/fixtures/ogd-smn_ber_t_historical_2020-2029.csv` — MeteoSwiss sample data

**Coverage:**
- SBB parser: PRODUKT_ID filtering, target station filtering, REAL status check, delay calculation
- MeteoSwiss parser: Timestamp parsing, timezone handling, year filtering

#### 3.2 Integration Tests

**Status:** 🟡 Partial
- [x] Test database fixture setup (`tests/integration/conftest.py`)
- [x] Test database initialization (sbb_precipitation_test)
- [x] Basic `collect_sbb.py` integration tests
- [x] Basic `load_meteo.py` integration tests
- [ ] End-to-end test coverage could be expanded

**Configuration:**
- Test DB: `sbb_precipitation_test` (isolated from production)
- Credentials: `.env.test` (not committed, isolated from `.env`)
- Fixture setup: Automatic database reset before test session

---

## Phase 4: Jupyter Notebook Analysis

### Status: 🟡 SKELETON COMPLETE (Ready for execution against populated DB)

The notebook (`notebooks/analysis.ipynb`) is structured with all required sections. Content needs population after data collection is complete.

**Notebook Sections (Planned):**

1. **0. Setup** ✅
   - Library imports (pandas, numpy, matplotlib, seaborn)
   - Database connection via SQLAlchemy
   - Row count sanity checks

2. **1. Data Validation** 🟡
   - NULL value checks in critical columns
   - Delay distribution histogram (outlier identification)
   - Precipitation distribution
   - Coverage metrics (% with non-null precipitation)
   - Date range completeness check
   - Data quality flags and documentation

3. **2. Exploratory Analysis** 🟡
   - Delay by station (boxplot)
   - Delay by month (seasonality)
   - Delay by day of week
   - Precipitation distribution per city per month
   - Scatter plot: precipitation vs. delay (per station)

4. **3. Correlation Analysis** 🟡
   - Pearson & Spearman correlation (overall + per station)
   - Correlation by precipitation category (dry/light/moderate/heavy)
   - P-values and statistical significance
   - Findings and limitations discussion

5. **4. Predictive Model** 🟡
   - Feature: median_precip_mm
   - Target: arrival_delay_min
   - Models: Linear Regression (baseline) + Random Forest (comparison)
   - Train/test split: 80/20 by date (temporal split, no data leakage)
   - Metrics: MAE, RMSE, R²
   - Residual plots, prediction vs. actual
   - Performance discussion and caveats

6. **5. Conclusion** 🟡
   - Summary of findings
   - Documented limitations (single feature, confounders, data quality, station proximity)
   - Suggestions for future work (additional features, longer time range)

---

## Code Quality & Standards

### Completed
- [x] Zen of Python adherence (explicit, simple, readable, flat)
- [x] Function modularity (~150 lines per file max)
- [x] Snake_case naming conventions
- [x] Docstrings on functions and modules
- [x] Error handling with logging (not bare prints)
- [x] Parameterized SQL queries (no string formatting)
- [x] `.gitignore` includes all required entries (raw/, .env, .env.test)

### Standards Applied
- Python 3.8+ compatibility
- PostgreSQL 12+ compatibility
- No hardcoded credentials (uses `.env`)
- Timezone consistency (all times UTC)
- Database credentials from environment variables

---

## Next Steps

### Priority 1: Execute Data Collection Pipeline
```bash
# 1. Start PostgreSQL
docker-compose up -d

# 2. Load precipitation data
python scripts/load_meteo.py

# 3. Collect SBB data (example: Jan-Feb 2024 for quick test)
python scripts/collect_sbb.py --start-year 2024 --end-year 2024 --months 1,2

# 4. Verify row counts in notebook Setup cell
```

### Priority 2: Complete Analysis Notebook
- Execute cells in order (Setup → Validation → EDA → Correlation → Model → Conclusion)
- Populate each section with actual analysis results
- Document findings and limitations
- Generate charts and visualizations

### Priority 3: Extended Testing
- Add more integration tests for edge cases
- Extend coverage for precipitation enrichment logic
- Add load tests for full 2024–2025 dataset

### Priority 4: Documentation & Deployment
- Update README with execution instructions
- Create runbook for monthly data updates
- Set up CI/CD pipeline for automated testing

---

## Known Limitations & Caveats

1. **Single Predictor**: Precipitation alone has limited explanatory power for delay; confounders (day of week, holidays, season, incidents, infrastructure work) not modelled.

2. **Station Proximity**: MeteoSwiss weather stations may not perfectly align with actual train route weather conditions.

3. **Data Quality**: Sensor gaps in precipitation data (NULL values) are filled forward/backward; if no non-null exists for entire day, median is NULL.

4. **Overnight Trips**: Trips crossing midnight (departure day X, arrival day X+1) are excluded from analysis.

5. **Cancelled Trains**: Rows with FAELLT_AUS_TF=True are excluded from collection.

6. **SBB Data Lag**: Monthly archives are typically available 1–2 days after month end.

---

## File Inventory

### Core Scripts
- `scripts/collect_sbb.py` — SBB data collection CLI
- `scripts/load_meteo.py` — MeteoSwiss precipitation CLI
- `scripts/reset_db.py` — Database reset utility
- `scripts/sbb_parser.py` — SBB CSV parsing logic
- `scripts/precipitation.py` — Precipitation enrichment
- `scripts/db_utils.py` — Database utilities

### Database
- `db/init.sql` — Complete schema (tables, indexes, views)
- `db/02-init-databases.sql` — Docker initialization script

### Notebooks
- `notebooks/analysis.ipynb` — Main analysis notebook (structure complete, awaiting data)

### Tests
- `tests/unit/test_sbb_parser.py` — Parser unit tests
- `tests/unit/test_load_meteo.py` — MeteoSwiss parsing unit tests
- `tests/integration/test_collect_sbb.py` — SBB collection integration tests
- `tests/integration/test_load_meteo.py` — MeteoSwiss loading integration tests
- `tests/integration/test_end_to_end.py` — End-to-end pipeline tests
- `tests/integration/conftest.py` — Integration test fixtures
- `tests/conftest.py` — Root test configuration
- `tests/fixtures/2024-01-01_istdaten.csv` — Real SBB sample data
- `tests/fixtures/ogd-smn_ber_t_historical_2020-2029.csv` — MeteoSwiss sample

### Configuration
- `docker-compose.yml` — PostgreSQL 15 + pgAdmin
- `.github/copilot-instructions.md` — Detailed project specifications
- `.github/progress.md` — This file
- `pyproject.toml` — Python project metadata
- `pytest.ini` — pytest configuration
- `requirements.txt` — Python dependencies
- `.gitignore` — Git ignore rules

---

## Summary Table

| Phase | Task | Status | Notes |
|-------|------|--------|-------|
| 1 | Infrastructure Setup | ✅ | Docker, DB schema, environment ready |
| 2 | SBB Collection Script | ✅ | Full pipeline, tested, crash-safe |
| 2 | MeteoSwiss Collection Script | ✅ | Full pipeline, tested, crash-safe |
| 2 | DB Reset Utility | ✅ | Safe, interactive, test-compatible |
| 3 | Unit Tests | ✅ | Parser, timestamp, filtering |
| 3 | Integration Tests | ⚠️ | Basic coverage, expandable |
| 4 | Analysis Notebook | 🟡 | Skeleton ready, awaits data |
| 5 | Documentation | 🟡 | This file + copilot-instructions.md |
| 6 | Pipeline Execution | ⏳ | Ready to start data collection |

---

## Changelog

### 2026-04-20 (Initial)
- Created this progress tracking document
- Documented completion of Phases 1–3 (Infrastructure, Scripts, Testing)
- Outlined Phase 4 (Analysis) and Phase 5 (Next Steps)
- All code scaffolding complete, awaiting data collection execution

