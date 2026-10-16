"""
ingest_census_population.py

Pulls annual state-level population estimates from the Census Bureau's
Population Estimates Program (PEP) API and loads them into a raw Postgres
table on Railway. Used as a per-capita denominator for other overlays
(crime rate, mortality rate) rather than as a standalone map layer.

SCOPE (agreed): 2010-present only for now, not the full 1991-2026 climate
range. Census population data isn't one continuous API like NOAA's - it's
published in "vintages", each covering only from the last decennial census
forward, with the schema itself changing at the 2020 census boundary:

  - Vintage 2019 (covers 2010-2019): long format, one row per state per
    DATE_CODE, where DATE_CODE is a numeric code whose meaning (which code
    = which year's July 1 estimate) is specific to this vintage and must
    be confirmed via the variables endpoint, not assumed.
  - Vintage 2024 or later (covers 2020-present): wide format, one column
    per year like POP_2020, POP_2021, POP_2023, etc.

Both require a free API key: https://api.census.gov/data/key_signup.html
Set it as the CENSUS_API_KEY environment variable.

IMPORTANT - run this first, before anything else:
    python ingest_census_population.py --discover
This fetches the variables.json for both vintages (which includes the
DATE_CODE value labels for the old-format vintage) plus a live sample
query for each, so we can confirm the exact DATE_CODE-to-year mapping
and column names before any parsing logic is written against them. This
script intentionally does NOT yet implement the real backfill - that
comes after discover output confirms both schemas.

Usage:
    python ingest_census_population.py --discover
"""

import argparse
import io
import logging
import os
import sys

import pandas as pd
import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("census_ingest")

BASE_URL = "https://api.census.gov/data"

# Old-format vintage (2010-2019), long format with DATE_CODE.
OLD_VINTAGE = 2019
# New-format vintage (2020-present), wide format with POP_YYYY columns.
NEW_VINTAGE = 2024

# Real Census 2-digit state FIPS -> USPS postal abbreviation, for the 48
# contiguous states only (matching the climate pipeline's scope - this
# table intentionally excludes AK, HI, DC, and the territories).
FIPS_TO_ABBR = {
    "01": "AL", "04": "AZ", "05": "AR", "06": "CA", "08": "CO", "09": "CT",
    "10": "DE", "12": "FL", "13": "GA", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD",
    "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO", "30": "MT",
    "31": "NE", "32": "NV", "33": "NH", "34": "NJ", "35": "NM", "36": "NY",
    "37": "NC", "38": "ND", "39": "OH", "40": "OK", "41": "OR", "42": "PA",
    "44": "RI", "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI", "56": "WY",
}


def _get_key() -> str:
    key = os.environ.get("CENSUS_API_KEY")
    if not key:
        log.error("CENSUS_API_KEY is not set. Get a free key at "
                   "https://api.census.gov/data/key_signup.html")
        sys.exit(1)
    return key


def discover() -> None:
    key = _get_key()

    log.info("=== Old-format vintage %d: variables.json (look for DATE_CODE value labels) ===", OLD_VINTAGE)
    resp = requests.get(f"{BASE_URL}/{OLD_VINTAGE}/pep/population/variables.json", timeout=30)
    resp.raise_for_status()
    variables = resp.json().get("variables", {})
    for name in ("DATE_CODE", "DATE_DESC", "POP", "NAME", "STATE"):
        if name in variables:
            print(f"  {name}: {variables[name]}")
        else:
            print(f"  {name}: NOT FOUND in this vintage's variables")

    log.info("=== Old-format vintage %d: sample query for one state (Alabama, FIPS 01) ===", OLD_VINTAGE)
    resp = requests.get(
        f"{BASE_URL}/{OLD_VINTAGE}/pep/population",
        params={"get": "NAME,POP,DATE_CODE,DATE_DESC", "for": "state:01", "key": key},
        timeout=30,
    )
    print(" status:", resp.status_code)
    print(" body:", resp.text[:2000])

    log.info("=== 2021 vintage API (POP_2020, POP_2021): sample query, Alabama ===")
    resp = requests.get(
        f"{BASE_URL}/2021/pep/population",
        params={"get": "NAME,POP_2020,POP_2021", "for": "state:01", "key": key},
        timeout=30,
    )
    print(" status:", resp.status_code)
    print(" body:", resp.text[:500])

    log.info("=== Flat-file probe: Census state totals CSVs (2022+ is not in the API) ===")
    base = "https://www2.census.gov/programs-surveys/popest/datasets"
    candidates = [
        f"{base}/2020-2025/state/totals/NST-EST2025-ALLDATA.csv",
        f"{base}/2020-2024/state/totals/NST-EST2024-ALLDATA.csv",
        f"{base}/2020-2023/state/totals/NST-EST2023-ALLDATA.csv",
    ]
    for url in candidates:
        r = requests.get(url, timeout=60, headers={"User-Agent": "kasling-analytics-climate-explorer/1.0"})
        print(f"  {r.status_code}  {url}")
        if r.status_code == 200:
            lines = r.text.splitlines()
            print("   header:", lines[0][:600])
            for line in lines[1:]:
                if ",Alabama," in line:
                    print("   Alabama:", line[:400])
                    break



DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.census_population_annual (
    state_fips   CHAR(2)     NOT NULL,
    state_abbr   CHAR(2)     NOT NULL,
    period_year  SMALLINT    NOT NULL,
    population   BIGINT      NOT NULL,
    source       TEXT        NOT NULL,
    ingested_at  DATE        NOT NULL DEFAULT CURRENT_DATE,
    PRIMARY KEY (state_fips, period_year)
);
"""

CSV_BASE = "https://www2.census.gov/programs-surveys/popest/datasets"
# Newest first; the first one that responds 200 wins. Add new years on top.
CSV_CANDIDATES = [
    ("2020-2025", "NST-EST2025-ALLDATA.csv"),
    ("2020-2024", "NST-EST2024-ALLDATA.csv"),
]
UA = {"User-Agent": "kasling-analytics-climate-explorer/1.0"}


def fetch_2010s(key: str) -> pd.DataFrame:
    """2010-2019 from the 2019-vintage API. DATE_CODE 3..12 = July 1 of
    2010..2019 (codes 1-2 are the April 2010 census/base rows - skipped)."""
    resp = requests.get(
        f"{BASE_URL}/{OLD_VINTAGE}/pep/population",
        params={"get": "NAME,POP,DATE_CODE", "for": "state:*", "key": key},
        timeout=60,
    )
    resp.raise_for_status()
    rows = resp.json()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df["date_code"] = df["DATE_CODE"].astype(int)
    df = df[df["date_code"].between(3, 12)].copy()
    df["period_year"] = df["date_code"] + 2007  # 3 -> 2010 ... 12 -> 2019
    df["state_fips"] = df["state"].str.zfill(2)
    df["population"] = df["POP"].astype("int64")
    df["source"] = f"api_{OLD_VINTAGE}_vintage"
    return df[["state_fips", "period_year", "population", "source"]]


def fetch_2020s() -> pd.DataFrame:
    """2020-present from Census's flat state-totals CSV (the API stops at
    vintage 2021). POPESTIMATEyyyy = July 1 estimate."""
    for folder, fname in CSV_CANDIDATES:
        url = f"{CSV_BASE}/{folder}/state/totals/{fname}"
        resp = requests.get(url, timeout=60, headers=UA)
        if resp.status_code != 200:
            log.warning("CSV %s returned %s, trying next", url, resp.status_code)
            continue
        raw = pd.read_csv(io.StringIO(resp.text), dtype={"STATE": str})
        raw = raw[raw["SUMLEV"].astype(str).str.zfill(3) == "040"]
        cols = [c for c in raw.columns if c.startswith("POPESTIMATE")]
        long = raw.melt(id_vars=["STATE"], value_vars=cols,
                        var_name="col", value_name="population")
        long["period_year"] = long["col"].str.replace("POPESTIMATE", "").astype(int)
        long["state_fips"] = long["STATE"].str.zfill(2)
        long["population"] = long["population"].astype("int64")
        long["source"] = fname
        log.info("Using %s (years %d-%d)", fname,
                 long["period_year"].min(), long["period_year"].max())
        return long[["state_fips", "period_year", "population", "source"]]
    raise RuntimeError("No Census state-totals CSV candidate responded 200")


def backfill() -> None:
    key = _get_key()
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL is not set.")
        sys.exit(1)

    df = pd.concat([fetch_2010s(key), fetch_2020s()], ignore_index=True)
    df = df[df["state_fips"].isin(FIPS_TO_ABBR)].copy()
    df["state_abbr"] = df["state_fips"].map(FIPS_TO_ABBR)
    df = df.drop_duplicates(["state_fips", "period_year"], keep="last")

    expected = 48 * (df["period_year"].max() - 2010 + 1)
    log.info("Prepared %d rows, %d states, years %d-%d (expected %d)",
             len(df), df["state_fips"].nunique(),
             df["period_year"].min(), df["period_year"].max(), expected)
    if len(df) != expected:
        log.warning("Row count differs from expected - check for missing states/years")

    records = [
        (r.state_fips, r.state_abbr, int(r.period_year), int(r.population), r.source)
        for r in df.itertuples()
    ]
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO raw.census_population_annual
                (state_fips, state_abbr, period_year, population, source)
            VALUES %s
            ON CONFLICT (state_fips, period_year) DO UPDATE SET
                population = EXCLUDED.population,
                source = EXCLUDED.source,
                ingested_at = CURRENT_DATE
            """,
            records,
        )
    log.info("Done. %d rows upserted into raw.census_population_annual", len(records))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discover", action="store_true",
                        help="print schema/endpoint info only; no DB writes")
    args = parser.parse_args()

    if args.discover:
        discover()
        return

    backfill()


if __name__ == "__main__":
    main()
