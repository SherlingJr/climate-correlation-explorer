-- Raw landing schema for the Climate Correlation Explorer.
-- Run once against your Railway Postgres instance before the first
-- ingestion run. One raw schema per source keeps things easy to reason
-- about when you add USDA/CDC/FBI/iNaturalist later.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.noaa_county_climate_monthly (
    fips_code     CHAR(5)      NOT NULL,   -- 2-digit state + 3-digit county FIPS, zero-padded
    period_year   SMALLINT     NOT NULL,
    period_month  SMALLINT     NOT NULL CHECK (period_month BETWEEN 1 AND 12),
    tmax_f        NUMERIC(6,2),
    tmin_f        NUMERIC(6,2),
    tavg_f        NUMERIC(6,2),
    prcp_in       NUMERIC(6,2),
    source_file   TEXT         NOT NULL,
    ingested_at   DATE         NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (fips_code, period_year, period_month)
);

-- SCOPE NOTE: the project scope shifted from county-level to state-level
-- after the county pipeline was fully built and backfilled (portfolio
-- scope call - state-level was the original intent). The county table
-- below is left in place since it's harmless and cost-free sitting idle,
-- but it is NOT used by the current ingestion/dbt/API pipeline - see
-- raw.noaa_state_climate_monthly instead.

CREATE TABLE IF NOT EXISTS raw.noaa_county_climate_monthly (
    fips_code     CHAR(5)      NOT NULL,   -- 2-digit state + 3-digit county FIPS, zero-padded
    period_year   SMALLINT     NOT NULL,
    period_month  SMALLINT     NOT NULL CHECK (period_month BETWEEN 1 AND 12),
    tmax_f        NUMERIC(6,2),
    tmin_f        NUMERIC(6,2),
    tavg_f        NUMERIC(6,2),
    prcp_in       NUMERIC(6,2),
    source_file   TEXT         NOT NULL,
    ingested_at   DATE         NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (fips_code, period_year, period_month)
);

CREATE INDEX IF NOT EXISTS idx_noaa_county_climate_year_month
    ON raw.noaa_county_climate_monthly (period_year, period_month);

-- Current pipeline: state-level monthly climate data. state_fips is the
-- REAL Census 2-digit state FIPS (already corrected from NOAA's own
-- internal numbering at ingestion time - see ingest_noaa_state_climate.py).
CREATE TABLE IF NOT EXISTS raw.noaa_state_climate_monthly (
    state_fips    CHAR(2)      NOT NULL,
    state_abbr    CHAR(2)      NOT NULL,
    period_year   SMALLINT     NOT NULL,
    period_month  SMALLINT     NOT NULL CHECK (period_month BETWEEN 1 AND 12),
    tmax_f        NUMERIC(6,2),
    tmin_f        NUMERIC(6,2),
    tavg_f        NUMERIC(6,2),
    prcp_in       NUMERIC(6,2),
    source_file   TEXT         NOT NULL,
    ingested_at   DATE         NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (state_fips, period_year, period_month)
);

CREATE INDEX IF NOT EXISTS idx_noaa_state_climate_year_month
    ON raw.noaa_state_climate_monthly (period_year, period_month);

-- Reference table: every other overlay (USDA, CDC, FBI, iNaturalist) will
-- join to climate data on fips_code, so get a canonical county list in
-- early. Census TIGER/Line's national county gazetteer is the standard
-- source and rarely changes (county FIPS additions/mergers are rare and
-- well documented), so this is a one-time load, not a recurring ingestion.
CREATE TABLE IF NOT EXISTS raw.county_fips_reference (
    fips_code    CHAR(5) PRIMARY KEY,
    county_name  TEXT NOT NULL,
    state_abbr   CHAR(2) NOT NULL,
    state_fips   CHAR(2) NOT NULL,
    land_sq_mi   NUMERIC(10,2)
);

CREATE INDEX IF NOT EXISTS idx_county_fips_state
    ON raw.county_fips_reference (state_fips);
