"""
ingest_noaa_state_climate.py

Pulls state-level temperature/precipitation data from NOAA's
nClimGrid-Daily product (area averages, state scale - the 48 contiguous
states) and loads a monthly aggregate into a raw Postgres table on Railway.

REVISION HISTORY:
- v1 pointed at a guessed S3 path that didn't exist (caught by --discover).
- v2 (county-level) pointed at the real NCEI URLs and worked, but two
  problems emerged after a real backfill: (a) the portfolio scope only
  ever needed state-level granularity, not ~3,100 counties, and (b) NOAA's
  "fips" column in these files is NOT a standard Census FIPS code - the
  2-digit prefix is NOAA's own legacy internal state numbering (01-48,
  alphabetically by state name, CONUS only), confirmed empirically (NOAA
  code '02' = Arizona's real counties, not Alaska's).
- v3 (this version): state-level only, and the state-numbering fix is
  applied HERE, at ingestion, rather than downstream in dbt - for a single
  48-row state crosswalk this is simple enough to do once at the source
  rather than repeat the "raw keeps source values as-is" pattern used for
  the county version's larger, more error-prone per-row remap.

Source (NOAA nClimGrid-Daily User Guide, confirmed via NOAA's own README
snippet: "tmax-202209-ste-prelim.csv contains the preliminary average of
daily maximum temperature for each of the 48 contiguous states..."):
  Web-accessible folder: https://www.ncei.noaa.gov/data/nclimgrid-daily/access/averages/{YYYY}/
  Filename pattern:      {var}-{YYYYMM}-ste-{status}.csv
    var    = tmax | tmin | tavg | prcp
    status = scaled (final, ~3 months lag) | prelim (recent months)
  One file per variable per month, one row per state, one column per day
  of that month (daily product) - this script averages (temp) / sums
  (precip) across the days present to get the monthly number.

Column layout (no header row - fixed positions, same pattern confirmed for
the county-level version of this file family):
  col 0: region type ('ste')
  col 1: NOAA's internal 2-digit state code (NOT Census FIPS - see above)
  col 2: state name (e.g. "AL: Alabama" or similar - unconfirmed exact
         format for this file, re-verify with --discover)
  col 3: year
  col 4: month
  col 5: variable name
  cols 6+: one column per day of the month
Units: temperature in Celsius, precipitation presumed millimeters - both
converted to US units (Fahrenheit / inches) below, same as the county version.

IMPORTANT - run this once before wiring up cron:
    python ingest_noaa_state_climate.py --discover
This downloads one real file and prints its columns/shape so we confirm
the column layout above actually matches before trusting a full backfill -
the state-level file's exact format has not been independently verified
yet, only inferred from the county-level file's confirmed layout.

Usage:
    python ingest_noaa_state_climate.py --discover
    python ingest_noaa_state_climate.py --start-year 1991 --end-year 2026
    python ingest_noaa_state_climate.py --start-year 2026 --end-year 2026  # incremental / cron run
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
STATUSES = ["scaled", "prelim"]

# No header row - fixed column positions (confirmed for the county-level
# sibling of this file family; re-confirm for state-level via --discover).
COL_REGION_TYPE = 0
COL_STATE_CODE = 1
COL_NAME = 2
COL_YEAR = 3
COL_MONTH = 4
COL_VARIABLE = 5
FIRST_DAY_COL = 6

MISSING_SENTINEL_THRESHOLD = -900

# NOAA's internal state numbering (01-48, alphabetical by state name, CONUS
# only) -> (real Census 2-digit state FIPS, USPS postal abbreviation).
# Confirmed empirically: NOAA code '02' has exactly Arizona's 15 counties
# with Arizona's real FIPS suffixes - not Alaska's, which real FIPS '02' is.
NOAA_STATE_CROSSWALK = {
    "01": ("01", "AL"), "02": ("04", "AZ"), "03": ("05", "AR"), "04": ("06", "CA"),
    "05": ("08", "CO"), "06": ("09", "CT"), "07": ("10", "DE"), "08": ("12", "FL"),
    "09": ("13", "GA"), "10": ("16", "ID"), "11": ("17", "IL"), "12": ("18", "IN"),
    "13": ("19", "IA"), "14": ("20", "KS"), "15": ("21", "KY"), "16": ("22", "LA"),
    "17": ("23", "ME"), "18": ("24", "MD"), "19": ("25", "MA"), "20": ("26", "MI"),
    "21": ("27", "MN"), "22": ("28", "MS"), "23": ("29", "MO"), "24": ("30", "MT"),
    "25": ("31", "NE"), "26": ("32", "NV"), "27": ("33", "NH"), "28": ("34", "NJ"),
    "29": ("35", "NM"), "30": ("36", "NY"), "31": ("37", "NC"), "32": ("38", "ND"),
    "33": ("39", "OH"), "34": ("40", "OK"), "35": ("41", "OR"), "36": ("42", "PA"),
    "37": ("44", "RI"), "38": ("45", "SC"), "39": ("46", "SD"), "40": ("47", "TN"),
    "41": ("48", "TX"), "42": ("49", "UT"), "43": ("50", "VT"), "44": ("51", "VA"),
    "45": ("53", "WA"), "46": ("54", "WV"), "47": ("55", "WI"), "48": ("56", "WY"),
}


def _file_url(var: str, year: int, month: int, status: str) -> str:
    yyyymm = f"{year}{month:02d}"
    return f"{BASE_URL}/{year}/{var}-{yyyymm}-ste-{status}.csv"


def _fetch_raw_csv(var: str, year: int, month: int) -> tuple[pd.DataFrame, str] | None:
    for status in STATUSES:
        url = _file_url(var, year, month, status)
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            df = pd.read_csv(io.BytesIO(resp.content), header=None, dtype={COL_STATE_CODE: str})
            return df, url
        if resp.status_code != 404:
            resp.raise_for_status()
    return None


def discover(year: int | None = None, month: int | None = None) -> None:
    """Download one real file and print its shape/columns/parsed output so
    we can confirm the layout before trusting a full backfill."""
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
    print(df.head(50))
    print()
    unmatched = sorted(set(df[COL_STATE_CODE].astype(str)) - set(NOAA_STATE_CROSSWALK))
    if unmatched:
        log.warning("State codes in the file with NO crosswalk entry: %s", unmatched)
    else:
        log.info("All state codes in the file matched the crosswalk (good).")
    print("Parsed aggregate (Fahrenheit):")
    print(_aggregate_month(df, "tavg").sort_values("state_abbr").to_string())


def _aggregate_month(df: pd.DataFrame, var: str) -> pd.DataFrame:
    """Collapse one state-by-day dataframe down to one value per state for
    the month, converted to US units, with real FIPS/postal added.
    Temperature variables are averaged across days; precipitation summed."""
    day_cols = list(range(FIRST_DAY_COL, df.shape[1]))
    daily = df[day_cols].apply(
        lambda col: pd.to_numeric(col.astype(str).str.strip(), errors="coerce")
    )
    daily = daily.where(daily > MISSING_SENTINEL_THRESHOLD)

    if var == "prcp":
        monthly_value = daily.sum(axis=1, min_count=1) / 25.4  # mm -> inches
    else:
        monthly_value = daily.mean(axis=1, skipna=True) * 9 / 5 + 32  # C -> F

    noaa_codes = df[COL_STATE_CODE].astype(str)
    crosswalk = noaa_codes.map(NOAA_STATE_CROSSWALK)
    unmatched_mask = crosswalk.isna()
    if unmatched_mask.any():
        bad = sorted(set(noaa_codes[unmatched_mask]))
        log.warning("Dropping %d rows with unrecognized state code(s): %s",
                    unmatched_mask.sum(), bad)

    out = pd.DataFrame({
        "state_fips": crosswalk.map(lambda t: t[0] if isinstance(t, tuple) else None),
        "state_abbr": crosswalk.map(lambda t: t[1] if isinstance(t, tuple) else None),
        var: monthly_value,
    })
    return out.dropna(subset=["state_fips"])


def fetch_month(year: int, month: int) -> pd.DataFrame | None:
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
        merged = agg if merged is None else merged.merge(agg, on=["state_fips", "state_abbr"], how="outer")

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
    INSERT INTO raw.noaa_state_climate_monthly
        (state_fips, state_abbr, period_year, period_month, tmax_f, tmin_f, tavg_f, prcp_in, source_file, ingested_at)
    VALUES %s
    ON CONFLICT (state_fips, period_year, period_month)
    DO UPDATE SET
        state_abbr = EXCLUDED.state_abbr,
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
            r.state_fips, r.state_abbr, int(r.period_year), int(r.period_month),
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
                continue
            df = fetch_month(year, month)
            if df is None:
                log.warning("%d-%02d: nothing fetched for any variable, skipping", year, month)
                continue
            n = load_to_postgres(df, dsn)
            total_rows += n
            total_months += 1
            log.info("%d-%02d -> %d state rows", year, month, n)
    log.info("Done. %d months processed, %d total rows upserted.", total_months, total_rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discover", action="store_true")
    parser.add_argument("--discover-year", type=int, default=None)
    parser.add_argument("--discover-month", type=int, default=None)
    parser.add_argument("--start-year", type=int, default=1991)
    parser.add_argument("--end-year", type=int, default=date.today().year)
    args = parser.parse_args()

    if args.discover:
        discover(args.discover_year, args.discover_month)
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL is not set.")
        sys.exit(1)

    run(args.start_year, args.end_year, dsn)


if __name__ == "__main__":
    main()
