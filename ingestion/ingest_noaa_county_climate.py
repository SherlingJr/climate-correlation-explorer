"""
ingest_noaa_county_climate.py

Pulls monthly county-level temperature/precipitation data from NOAA's
EpiNOAA product (derived from nClimGrid-daily, aggregated to county FIPS)
and loads it into a raw Postgres table on Railway.

Why EpiNOAA instead of GSOD or the legacy nClimDiv "county" files:
- GSOD is station-level. To get a county value you'd have to pick/interpolate
  stations yourself, and station coverage is uneven across counties.
- The legacy nClimDiv "county" files (climdiv-tmpccy-*) duplicate the same
  climate-division value across every county inside that division, so
  neighboring counties in the same division show identical numbers - not
  real per-county resolution.
- EpiNOAA is the nClimGrid grid (real spatial interpolation from GHCN-D
  stations) pre-aggregated to actual county polygons. It's already keyed by
  FIPS, covers 1951-present (so it comfortably covers 2004+), is public/free
  with no API key or rate limit (flat files on S3, unsigned requests), and
  is updated monthly. That means int_county_fips_reference and any
  station-to-county harmonization step you sketched for climate specifically
  can be dropped - the join key is already FIPS.

Source bucket: s3://noaa-nclimgrid-daily-pds/EpiNOAA/csv/
(AWS Open Data Registry, unsigned/no-account access)

IMPORTANT - run this once before wiring up cron:
    python ingest_noaa_county_climate.py --discover
This lists the actual objects under the EpiNOAA/csv/ prefix and prints the
header row of one file. NOAA's exact file naming and column names for this
product have shifted before between "cty"/"cte" and header casing, so the
column-detection below is written to match by *keyword*, not by hardcoded
position - but confirm it against real output before your first backfill.

Usage:
    python ingest_noaa_county_climate.py --discover
    python ingest_noaa_county_climate.py --start-year 2004 --end-year 2026
    python ingest_noaa_county_climate.py --start-year 2026 --end-year 2026 --start-month 9  # incremental / cron run
"""

import argparse
import io
import logging
import os
import sys
from datetime import date

import boto3
import pandas as pd
import psycopg2
from botocore import UNSIGNED
from botocore.config import Config
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("noaa_ingest")

BUCKET = "noaa-nclimgrid-daily-pds"
PREFIX = "EpiNOAA/csv/"

# Candidate substrings used to identify each field regardless of exact
# header spelling/casing the source file uses.
COLUMN_HINTS = {
    "fips": ["fips"],
    "year": ["year"],
    "month": ["month"],
    "tmax": ["tmax"],
    "tmin": ["tmin"],
    "tavg": ["tavg", "avg_temp", "avgtemp"],
    "prcp": ["prcp", "precip"],
}


def get_s3_client():
    # Public bucket - explicitly unsigned, no AWS credentials needed/used.
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def discover(limit: int = 40) -> None:
    """List objects under the EpiNOAA/csv/ prefix and preview one file's
    header so you can confirm naming/columns before the first real run."""
    s3 = get_s3_client()
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX, MaxKeys=limit)
    keys = [obj["Key"] for obj in resp.get("Contents", [])]
    if not keys:
        log.error("No objects found under s3://%s/%s - check the prefix, "
                   "NOAA may have reorganized the bucket.", BUCKET, PREFIX)
        return
    log.info("Found %d objects (showing up to %d):", resp.get("KeyCount", 0), limit)
    for k in keys:
        print(f"  {k}")

    county_keys = [k for k in keys if "cty" in k.lower() or "cte" in k.lower() or "county" in k.lower()]
    sample_key = county_keys[0] if county_keys else keys[0]
    log.info("Previewing header of: %s", sample_key)
    obj = s3.get_object(Bucket=BUCKET, Key=sample_key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()), nrows=5)
    print(df.columns.tolist())
    print(df.head())


def _find_column(columns, hints) -> str | None:
    lower = {c.lower(): c for c in columns}
    for hint in hints:
        for lc, orig in lower.items():
            if hint in lc:
                return orig
    return None


def _list_county_monthly_keys(s3, start_year: int, end_year: int) -> list[str]:
    """List monthly county-level CSV keys within the year range.
    Paginates since the prefix holds many objects across all years/regions."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            lower = key.lower()
            if not lower.endswith(".csv"):
                continue
            if "cty" not in lower and "cte" not in lower and "county" not in lower:
                continue
            # Expect a YYYYMM somewhere in the filename; filter by year range.
            digits = "".join(ch if ch.isdigit() else " " for ch in key).split()
            year_hit = any(
                len(tok) >= 6 and start_year <= int(tok[:4]) <= end_year
                for tok in digits
            )
            if year_hit:
                keys.append(key)
    return keys


def _parse_county_csv(s3, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    fips_col = _find_column(df.columns, COLUMN_HINTS["fips"])
    tmax_col = _find_column(df.columns, COLUMN_HINTS["tmax"])
    tmin_col = _find_column(df.columns, COLUMN_HINTS["tmin"])
    tavg_col = _find_column(df.columns, COLUMN_HINTS["tavg"])
    prcp_col = _find_column(df.columns, COLUMN_HINTS["prcp"])
    year_col = _find_column(df.columns, COLUMN_HINTS["year"])
    month_col = _find_column(df.columns, COLUMN_HINTS["month"])

    if not fips_col:
        raise ValueError(f"{key}: could not find a FIPS column in {list(df.columns)}")

    out = pd.DataFrame()
    out["fips_code"] = df[fips_col].astype(str).str.zfill(5)

    if year_col and month_col:
        out["period_year"] = df[year_col].astype(int)
        out["period_month"] = df[month_col].astype(int)
    else:
        # Some EpiNOAA exports encode the period in a single date-like column
        # or in the filename (YYYYMM) instead of separate year/month columns.
        date_col = _find_column(df.columns, ["date", "period"])
        if date_col:
            parsed = pd.to_datetime(df[date_col])
            out["period_year"] = parsed.dt.year
            out["period_month"] = parsed.dt.month
        else:
            digits = "".join(ch if ch.isdigit() else " " for ch in key).split()
            yyyymm = next((t for t in digits if len(t) == 6), None)
            if not yyyymm:
                raise ValueError(f"{key}: no year/month column and none found in filename")
            out["period_year"] = int(yyyymm[:4])
            out["period_month"] = int(yyyymm[4:6])

    out["tmax_f"] = df[tmax_col] if tmax_col else None
    out["tmin_f"] = df[tmin_col] if tmin_col else None
    out["tavg_f"] = df[tavg_col] if tavg_col else None
    out["prcp_in"] = df[prcp_col] if prcp_col else None
    out["source_file"] = key
    return out


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
    s3 = get_s3_client()
    keys = _list_county_monthly_keys(s3, start_year, end_year)
    log.info("Found %d county monthly files for %d-%d", len(keys), start_year, end_year)
    if not keys:
        log.warning("Nothing to load - run --discover to check the bucket layout.")
        return

    total_rows = 0
    for i, key in enumerate(keys, 1):
        try:
            df = _parse_county_csv(s3, key)
            n = load_to_postgres(df, dsn)
            total_rows += n
            log.info("[%d/%d] %s -> %d rows", i, len(keys), key, n)
        except Exception:
            log.exception("Failed on %s - skipping, continuing with remaining files", key)
    log.info("Done. %d total rows upserted.", total_rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discover", action="store_true",
                         help="List bucket contents and preview a file's columns, then exit.")
    parser.add_argument("--start-year", type=int, default=2004)
    parser.add_argument("--end-year", type=int, default=date.today().year)
    parser.add_argument("--start-month", type=int, default=1,
                         help="Reserved for future month-level filtering; year-level filtering is used for now.")
    args = parser.parse_args()

    if args.discover:
        discover()
        return

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL is not set. On Railway this is injected automatically "
                   "when you reference the Postgres plugin's connection string as an env var.")
        sys.exit(1)

    run(args.start_year, args.end_year, dsn)


if __name__ == "__main__":
    main()
