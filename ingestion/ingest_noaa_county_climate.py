"""
ingest_noaa_county_climate.py

Pulls county-level temperature/precipitation data from NOAA's
nClimGrid-Daily product (area averages, county scale) and loads a
monthly aggregate into a raw Postgres table on Railway.

REVISION NOTE: an earlier version of this script pointed at a guessed S3
path (s3://noaa-nclimgrid-daily-pds/EpiNOAA/csv/) based on a JS-rendered
bucket browser page that couldn't actually be verified. Discover mode
correctly caught that it was wrong (0 objects found). This version is
built from NOAA's own nClimGrid-Daily v1.0.0 User Guide, which documents
the real file naming convention directly - see the source note below.

Why nClimGrid-Daily's county area-averages, not GSOD or legacy nClimDiv:
- GSOD is station-level. To get a county value you'd have to pick/interpolate
  stations yourself, and station coverage is uneven across counties.
- The legacy nClimDiv "county" files (climdiv-tmpccy-*) duplicate the same
  climate-division value across every county inside that division - not
  real per-county resolution.
- nClimGrid-Daily's county area-averages are the grid (real spatial
  interpolation from GHCN-D stations) pre-aggregated to actual county
  polygons - real per-county values. Public, free, no API key, covers
  1951-present, updated every 3 days. Already keyed by FIPS.

Source (NOAA nClimGrid-Daily User Guide, section 3b - filenames):
  Web-accessible folder: https://www.ncei.noaa.gov/data/nclimgrid-daily/access/averages/{YYYY}/
  Filename pattern:      {var}-{YYYYMM}-cty-{status}.csv
    var    = tmax | tmin | tavg | prcp
    status = scaled (final, appears ~3 months after the fact)
           | prelim (recent months, before QC scaling)
  One file per variable per month - NOT one combined file. Each file has
  one row per county with a value per day of that month (this is a DAILY
  product), so this script averages (temp) / sums (precip) across the
  days present to get the monthly number our schema stores.

IMPORTANT - confirmed via --discover against a real file (tavg, 2026-07):
this file has NO header row - it's a fixed positional layout:
  col 0:  region type ('cty')
  col 1:  county FIPS code (must be read as text - reading as a number
          drops the leading zero on FIPS 01000-09999)
  col 2:  "ST: County Name" (state postal abbreviation + county name)
  col 3:  year
  col 4:  month
  col 5:  variable name ('TAVG'/'TMAX'/'TMIN'/'PRCP')
  cols 6-36: one column per day of the month (up to 31), values are
          right-padded with leading spaces
Also confirmed: temperature values are in Celsius (28.65 for Autauga
County, AL in July only makes sense as ~83.6F), not Fahrenheit as this
script originally assumed. Precipitation is presumed millimeters (NOAA's
metric-first convention for this product) - converted to inches below.
Both get converted to the US units our schema/frontend expect.

Usage:
    python ingest_noaa_county_climate.py --discover
    python ingest_noaa_county_climate.py --start-year 1991 --end-year 2026
    python ingest_noaa_county_climate.py --start-year 2026 --end-year 2026  # incremental / cron run
"""

import argparse
import io
import logging
import os
import sys
from datetime import date

import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("noaa_ingest")

BASE_URL = "https://www.ncei.noaa.gov/data/nclimgrid-daily/access/averages"
VARIABLES = ["tmax", "tmin", "tavg", "prcp"]
STATUSES = ["scaled", "prelim"]  # try scaled (final) first, fall back to prelim

# No header row - fixed column positions, confirmed via --discover.
COL_REGION_TYPE = 0
COL_FIPS = 1
COL_NAME = 2
COL_YEAR = 3
COL_MONTH = 4
COL_VARIABLE = 5
FIRST_DAY_COL = 6

# Below this, treat a value as a missing-data sentinel rather than a real
# reading - no real daily temp or precip is anywhere close to this.
MISSING_SENTINEL_THRESHOLD = -900


def _file_url(var: str, year: int, month: int, status: str) -> str:
    yyyymm = f"{year}{month:02d}"
    return f"{BASE_URL}/{year}/{var}-{yyyymm}-cty-{status}.csv"


def _fetch_raw_csv(var: str, year: int, month: int) -> tuple[pd.DataFrame, str] | None:
    """Try scaled first, then prelim. Returns (dataframe, url) or None if
    neither status exists yet. header=None + fips forced to string - see
    the no-header/leading-zero note above."""
    for status in STATUSES:
        url = _file_url(var, year, month, status)
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            df = pd.read_csv(io.BytesIO(resp.content), header=None, dtype={COL_FIPS: str})
            return df, url
        if resp.status_code != 404:
            resp.raise_for_status()
    return None


def discover(year: int | None = None, month: int | None = None) -> None:
    """Download one real file and print its shape/columns so we can
    confirm the parsing logic before trusting a full backfill."""
    today = date.today()
    if year is None or month is None:
        probe = today.month - 2
        year = today.year if probe > 0 else today.year - 1
        month = probe if probe > 0 else probe + 12

    log.info("Probing tavg for %d-%02d ...", year, month)
    result = _fetch_raw_csv("tavg", year, month)
    if result is None:
        log.error("Got 404 for both scaled and prelim at %d-%02d. Tried:\n  %s\n  %s",
                   year, month, _file_url("tavg", year, month, "scaled"),
                   _file_url("tavg", year, month, "prelim"))
        return

    df, url = result
    log.info("Fetched: %s", url)
    log.info("Shape: %s", df.shape)
    print(df.head())
    print("Parsed aggregate (Fahrenheit):")
    print(_aggregate_month(df, "tavg").head())


def _aggregate_month(df: pd.DataFrame, var: str) -> pd.DataFrame:
    """Collapse one county-by-day dataframe (fixed positional layout, no
    header - see module docstring) down to one value per county for the
    month, converted to US units. Temperature variables are averaged
    across days; precipitation is summed."""
    day_cols = list(range(FIRST_DAY_COL, df.shape[1]))
    daily = df[day_cols].apply(
        lambda col: pd.to_numeric(col.astype(str).str.strip(), errors="coerce")
    )
    daily = daily.where(daily > MISSING_SENTINEL_THRESHOLD)  # drop sentinel values to NaN

    if var == "prcp":
        monthly_mm = daily.sum(axis=1, min_count=1)
        monthly_value = monthly_mm / 25.4  # mm -> inches
    else:
        monthly_c = daily.mean(axis=1, skipna=True)
        monthly_value = monthly_c * 9 / 5 + 32  # Celsius -> Fahrenheit

    return pd.DataFrame({
        "fips_code": df[COL_FIPS].astype(str).str.zfill(5),
        var: monthly_value,
    })


def fetch_month(year: int, month: int) -> pd.DataFrame | None:
    """Fetch all four variables for one year-month and join into one
    row-per-county dataframe."""
    merged: pd.DataFrame | None = None
    sources = []
    for var in VARIABLES:
        result = _fetch_raw_csv(var, year, month)
        if result is None:
            log.warning("%d-%02d: no file for %s (may not be published yet)", year, month, var)
            continue
        raw_df, url = result
        sources.append(url)
        agg = _aggregate_month(raw_df, var)
        merged = agg if merged is None else merged.merge(agg, on="fips_code", how="outer")

    if merged is None:
        return None

    for var in VARIABLES:
        if var not in merged.columns:
            merged[var] = None

    merged = merged.rename(columns={"tmax": "tmax_f", "tmin": "tmin_f", "tavg": "tavg_f", "prcp": "prcp_in"})
    merged["period_year"] = year
    merged["period_month"] = month
    merged["source_file"] = ";".join(sources)
    return merged


UPSERT_SQL = """
    INSERT INTO raw.noaa_county_climate_monthly
        (fips_code, period_year, period_month, tmax_f, tmin_f, tavg_f, prcp_in, source_file, ingested_at)
    VALUES %s
    ON CONFLICT (fips_code, period_year, period_month)
    DO UPDATE SET
        tmax_f = EXCLUDED.tmax_f,
        tmin_f = EXCLUDED.tmin_f,
        tavg_f = EXCLUDED.tavg_f,
        prcp_in = EXCLUDED.prcp_in,
        source_file = EXCLUDED.source_file,
        ingested_at = EXCLUDED.ingested_at;
"""


def load_to_postgres(df: pd.DataFrame, dsn: str) -> int:
    if df.empty:
        return 0
    rows = [
        (
            r.fips_code, int(r.period_year), int(r.period_month),
            r.tmax_f, r.tmin_f, r.tavg_f, r.prcp_in, r.source_file, date.today(),
        )
        for r in df.itertuples(index=False)
    ]
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, rows, page_size=1000)
        conn.commit()
    return len(rows)


def run(start_year: int, end_year: int, dsn: str) -> None:
    total_rows = 0
    total_months = 0
    today = date.today()
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == today.year and month > today.month:
                continue  # don't request months that haven't happened yet
            df = fetch_month(year, month)
            if df is None:
                log.warning("%d-%02d: nothing fetched for any variable, skipping", year, month)
                continue
            n = load_to_postgres(df, dsn)
            total_rows += n
            total_months += 1
            log.info("%d-%02d -> %d county rows", year, month, n)
    log.info("Done. %d months processed, %d total rows upserted.", total_months, total_rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discover", action="store_true",
                         help="Fetch one real file and print its columns, then exit.")
    parser.add_argument("--discover-year", type=int, default=None)
    parser.add_argument("--discover-month", type=int, default=None)
    parser.add_argument("--start-year", type=int, default=2004)
    parser.add_argument("--end-year", type=int, default=date.today().year)
    args = parser.parse_args()

    if args.discover:
        discover(args.discover_year, args.discover_month)
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL is not set. On Railway this is injected automatically "
                   "when you reference the Postgres plugin's connection string as an env var.")
        sys.exit(1)

    run(args.start_year, args.end_year, dsn)


if __name__ == "__main__":
    main()
