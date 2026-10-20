from typing import Optional

from fastapi import APIRouter, HTTPException

from db import get_cursor

router = APIRouter(prefix="/api", tags=["climate"])

ANNUAL_COLS = ["state_fips", "state_abbr", "period_year", "tavg_f", "tmax_f",
               "tmin_f", "prcp_in", "anomaly_f", "months_reported",
               "is_partial_year", "has_data_gap"]

MONTHLY_COLS = ["state_fips", "state_abbr", "period_year", "period_month",
                "tavg_f", "tmax_f", "tmin_f", "prcp_in", "anomaly_f", "has_data_gap"]


@router.get("/states/map/{year}")
def annual_map(year: int):
    """All 48 states for one year - the primary dashboard map. One query,
    48 rows - cheap regardless of how often a visitor drags the year picker."""
    with get_cursor() as cur:
        cur.execute(
            f"""
            select {', '.join(ANNUAL_COLS)}
            from analytics_marts.mart_state_climate_annual
            where period_year = %s
            """,
            (year,),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for {year}")
    return {"year": year, "states": [dict(zip(ANNUAL_COLS, r)) for r in rows]}


@router.get("/states/compare")
def compare_years(start_year: int, end_year: int):
    """Two annual snapshots in one call - powers the before/after compare
    map. One round trip instead of two separate /map calls."""
    with get_cursor() as cur:
        cur.execute(
            f"""
            select {', '.join(ANNUAL_COLS)}
            from analytics_marts.mart_state_climate_annual
            where period_year in (%s, %s)
            """,
            (start_year, end_year),
        )
        rows = cur.fetchall()
    by_year: dict[int, list[dict]] = {start_year: [], end_year: []}
    for r in rows:
        row = dict(zip(ANNUAL_COLS, r))
        by_year.setdefault(row["period_year"], []).append(row)
    return {
        "start_year": start_year,
        "end_year": end_year,
        "start": by_year.get(start_year, []),
        "end": by_year.get(end_year, []),
    }


@router.get("/states/period")
def period_summary(start_year: int = 1991, end_year: Optional[int] = None):
    """Whole-period view that powers the annual map (no year slider):
      - change_f:   how much each state warmed/cooled from start_year to
                    end_year (end_year defaults to the latest year loaded).
                    Only the months present in end_year are compared in BOTH
                    years, so a partial current year isn't compared against a
                    full 1991 (that would fake a seasonal difference).
      - avg_tavg_f: average temperature across every year in the range."""
    with get_cursor() as cur:
        if end_year is None:
            cur.execute("select max(period_year) from analytics_marts.mart_state_climate_monthly")
            end_year = cur.fetchone()[0]
        cur.execute(
            "select max(period_month) from analytics_marts.mart_state_climate_monthly where period_year = %s",
            (end_year,),
        )
        months = cur.fetchone()[0]
        if end_year is None or months is None:
            raise HTTPException(status_code=404, detail=f"No data for {end_year}")

        cur.execute(
            """
            select state_abbr, period_year, avg(anomaly_f)
            from analytics_marts.mart_state_climate_monthly
            where period_year in (%s, %s) and period_month <= %s
            group by state_abbr, period_year
            """,
            (start_year, end_year, months),
        )
        anomaly = {(a, y): v for a, y, v in cur.fetchall()}

        cur.execute(
            """
            select state_abbr, avg(tavg_f)
            from analytics_marts.mart_state_climate_monthly
            where period_year between %s and %s
            group by state_abbr
            """,
            (start_year, end_year),
        )
        avg_temp = dict(cur.fetchall())

    states = []
    for abbr, avg in avg_temp.items():
        a0, a1 = anomaly.get((abbr, start_year)), anomaly.get((abbr, end_year))
        states.append({
            "state_abbr": abbr,
            "change_f": float(a1 - a0) if a0 is not None and a1 is not None else None,
            "avg_tavg_f": float(avg) if avg is not None else None,
        })
    return {"start_year": start_year, "end_year": end_year,
            "months_compared": months, "states": states}


@router.get("/states/{state}/trend")
def state_trend(state: str, start_year: int = 1991, end_year: int = 2026):
    """Annual time series for one state - powers the trend chart when a
    date range is selected. state is a 2-letter postal abbreviation."""
    with get_cursor() as cur:
        cur.execute(
            f"""
            select {', '.join(ANNUAL_COLS)}
            from analytics_marts.mart_state_climate_annual
            where state_abbr = %s
              and period_year between %s and %s
            order by period_year
            """,
            (state.upper(), start_year, end_year),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for state '{state}'")
    return {"state": state.upper(), "series": [dict(zip(ANNUAL_COLS, r)) for r in rows]}


@router.get("/states/trend")
def states_trend(states: str, start_year: int = 1991, end_year: int = 2026):
    """Annual series for up to 5 states in ONE call - powers the
    multi-state trend chart without the frontend firing one request per
    selected state. states is a comma-separated list of postal
    abbreviations, e.g. 'GA,FL,TX'."""
    abbrs = [s.strip().upper() for s in states.split(",") if s.strip()]
    if not abbrs:
        raise HTTPException(status_code=422, detail="states must be a non-empty comma-separated list")
    if len(abbrs) > 5:
        raise HTTPException(status_code=422, detail="up to 5 states at a time")

    with get_cursor() as cur:
        cur.execute(
            f"""
            select {', '.join(ANNUAL_COLS)}
            from analytics_marts.mart_state_climate_annual
            where state_abbr = any(%s)
              and period_year between %s and %s
            order by state_abbr, period_year
            """,
            (abbrs, start_year, end_year),
        )
        rows = cur.fetchall()

    series: dict[str, list[dict]] = {a: [] for a in abbrs}
    for r in rows:
        row = dict(zip(ANNUAL_COLS, r))
        series.setdefault(row["state_abbr"], []).append(row)
    return {"states": abbrs, "start_year": start_year, "end_year": end_year, "series": series}


@router.get("/states/{state}/month/{month}")
def state_month_drilldown(state: str, month: int):
    """One state's value for one calendar month across every year on
    record - e.g. 'July in Georgia since 1991'. Powers the seasonal
    drill-down chart."""
    if not 1 <= month <= 12:
        raise HTTPException(status_code=422, detail="month must be 1-12")
    with get_cursor() as cur:
        cur.execute(
            f"""
            select {', '.join(MONTHLY_COLS)}
            from analytics_marts.mart_state_climate_monthly
            where state_abbr = %s
              and period_month = %s
            order by period_year
            """,
            (state.upper(), month),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for state '{state}', month {month}")
    return {"state": state.upper(), "month": month, "series": [dict(zip(MONTHLY_COLS, r)) for r in rows]}


# Overlay registry. To add a dataset: add a union branch in
# dbt/models/marts/mart_overlay_annual.sql, then flip its status to "live"
# here. The frontend selector reads /available_overlays, so listing it needs
# no frontend change (display rules live in index.html OVERLAYS).
OVERLAYS = {
    "census_population": {"label": "Population", "unit": "people", "status": "live"},
    "bls_unemployment": {"label": "Unemployment rate", "unit": "%", "status": "planned"},
    "usda_crop_yields": {"label": "Crop yields", "status": "planned"},
    "fbi_crime": {"label": "Crime rate", "status": "planned"},
    "cdc_mortality": {"label": "Mortality rate", "status": "planned"},
}


@router.get("/available_overlays")
def available_overlays():
    """Overlay list for the frontend selector. Only 'live' ones are usable."""
    return {"overlays": [{"id": k, **v} for k, v in OVERLAYS.items()]}


@router.get("/overlay/{dataset}")
def overlay_series(dataset: str, states: str, start_year: int = 1991, end_year: int = 2026):
    """Annual overlay series for up to 5 states in ONE call, same shape as
    /states/trend. 501 for datasets not built yet, so the frontend can tell
    'not built yet' from 'no data for this state'. Overlays can start later
    than the climate record (population starts in 2010) - the series is
    simply shorter, never padded."""
    meta = OVERLAYS.get(dataset)
    if meta is None:
        raise HTTPException(status_code=404, detail=f"Unknown overlay '{dataset}'")
    if meta["status"] != "live":
        raise HTTPException(status_code=501, detail=f"Overlay '{dataset}' not implemented yet")

    abbrs = [s.strip().upper() for s in states.split(",") if s.strip()]
    if not abbrs:
        raise HTTPException(status_code=422, detail="states must be a non-empty comma-separated list")
    if len(abbrs) > 5:
        raise HTTPException(status_code=422, detail="up to 5 states at a time")

    with get_cursor() as cur:
        cur.execute(
            """
            select state_abbr, period_year, value
            from analytics_marts.mart_overlay_annual
            where dataset = %s
              and state_abbr = any(%s)
              and period_year between %s and %s
            order by state_abbr, period_year
            """,
            (dataset, abbrs, start_year, end_year),
        )
        rows = cur.fetchall()

    series: dict[str, list[dict]] = {a: [] for a in abbrs}
    for abbr, year, value in rows:
        series.setdefault(abbr, []).append({"period_year": year, "value": float(value)})
    return {"dataset": dataset, "label": meta["label"], "unit": meta.get("unit"),
            "states": abbrs, "series": series}


@router.get("/correlation/{dataset_a}/{dataset_b}")
def correlation(dataset_a: str, dataset_b: str, state: str):
    """Placeholder for the Pearson-r scatter panel - implement once at
    least one overlay mart exists to correlate climate against."""
    raise HTTPException(status_code=501, detail="Correlation endpoint not implemented yet")
