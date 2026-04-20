-- ============================================================================
-- Database Schema for SBB Precipitation Study
-- ============================================================================
-- This schema is applied to both 'sbb_precipitation' (production)
-- and 'sbb_precipitation_test' (testing) databases.
-- ============================================================================

-- Table: precipitation_10min
-- Stores the raw 10-minute precipitation readings from MeteoSwiss.
CREATE TABLE IF NOT EXISTS precipitation_10min (
    id             SERIAL PRIMARY KEY,
    station_abbr   VARCHAR(10)  NOT NULL,          -- 'SMA', 'BAS', 'BER'
    city           VARCHAR(20)  NOT NULL,          -- 'Zürich', 'Basel', 'Bern'
    measured_at    TIMESTAMP    NOT NULL,          -- UTC timestamp
    precip_mm      FLOAT                           -- nullable for sensor gaps
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_precip_station_time
    ON precipitation_10min (station_abbr, measured_at);

CREATE INDEX IF NOT EXISTS idx_precip_city_time
    ON precipitation_10min (city, measured_at);


-- Table: train_connections
-- One row per qualifying train arrival at a target station.
CREATE TABLE IF NOT EXISTS train_connections (
    id                          SERIAL PRIMARY KEY,
    betriebstag                 DATE          NOT NULL,
    fahrt_bezeichner            VARCHAR(100)  NOT NULL,
    destination_station         VARCHAR(50)   NOT NULL,  -- 'Zürich HB', 'Basel SBB', 'Bern'
    destination_city            VARCHAR(20)   NOT NULL,  -- 'Zürich', 'Basel', 'Bern'
    scheduled_arrival           TIMESTAMP     NOT NULL,
    actual_arrival              TIMESTAMP     NOT NULL,
    arrival_delay_min           FLOAT         NOT NULL,
    origin_station              VARCHAR(100),
    origin_departure_scheduled  TIMESTAMP,
    trip_duration_min           FLOAT,
    median_precip_mm            FLOAT,
    source_month                VARCHAR(7)    NOT NULL   -- 'YYYY-MM'
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_connections_fahrt_arrival
    ON train_connections (fahrt_bezeichner, scheduled_arrival);

CREATE INDEX IF NOT EXISTS idx_connections_date_station
    ON train_connections (betriebstag, destination_station);


-- Table: processing_log
-- Tracks completed processing runs for crash recovery.
CREATE TABLE IF NOT EXISTS processing_log (
    id             SERIAL PRIMARY KEY,
    run_type       VARCHAR(20)   NOT NULL,  -- 'sbb' or 'meteo'
    period         VARCHAR(10)   NOT NULL,  -- 'YYYY-MM' for SBB, station abbr for meteo
    status         VARCHAR(20)   NOT NULL,  -- 'success', 'error', 'partial'
    rows_inserted  INT,
    error_msg      TEXT,
    run_at         TIMESTAMPTZ   DEFAULT NOW()
);


-- View: analysis
-- Denormalized view for notebook analysis.
DROP VIEW IF EXISTS analysis;
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
    EXTRACT(DOW  FROM tc.betriebstag) AS day_of_week,
    EXTRACT(MONTH FROM tc.betriebstag) AS month
FROM train_connections tc;

