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
import logging
import os
import sys

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

    log.info("=== Catalog scan: every PEP (population estimates) dataset vintage 2020+ ===")
    cat = requests.get("https://api.census.gov/data.json", timeout=120)
    print(" catalog status:", cat.status_code)
    found = []
    if cat.status_code == 200:
        for ds in cat.json().get("dataset", []):
            ident = "/".join(ds.get("c_dataset", []) or [])
            vintage = ds.get("c_vintage")
            if ident.startswith("pep/") and vintage and int(vintage) >= 2020:
                found.append((int(vintage), ident, ds.get("title", "")))
    for vintage, ident, title in sorted(found):
        print(f"  {vintage}  {ident}  -- {title}")
    if not found:
        print("  (none found - will probe candidate URLs directly)")

    log.info("=== Direct probe of candidate variables.json URLs ===")
    for vintage in (2020, 2021, 2022, 2023, 2024, 2025):
        for ds_name in ("population", "natmonthly", "charv"):
            url = f"{BASE_URL}/{vintage}/pep/{ds_name}/variables.json"
            r = requests.get(url, timeout=30)
            extra = ""
            if r.status_code == 200:
                pv = sorted(v for v in r.json().get("variables", {}) if v.startswith("POP"))
                extra = f" POP vars: {pv}"
            print(f"  {r.status_code}  {url}{extra}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discover", action="store_true")
    args = parser.parse_args()

    if args.discover:
        discover()
        return

    log.error("Only --discover is implemented so far - confirm the schema first, "
              "then the real backfill logic gets added based on what it shows.")
    sys.exit(1)


if __name__ == "__main__":
    main()
