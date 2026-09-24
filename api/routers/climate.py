from fastapi import APIRouter, HTTPException, Query

from db import get_cursor

router = APIRouter(prefix="/api", tags=["climate"])


@router.get("/climate/{fips}")
def county_time_series(fips: str, start_year: int = 2004, end_year: int = 2026):
    """Time series for one county - powers the detail panel when a user
    clicks a county on the map, or a single-county line chart."""
    with get_cursor() as cur:
        cur.execute(
            """
            select period_year, period_month, tavg_f, tmax_f, tmin_f, prcp_in,
                   anomaly_f, has_data_gap
            from marts.mart_climate_trends
            where fips_code = %s
              and period_year between %s and %s
            order by period_year, period_month
            """,
            (fips.zfill(5), start_year, end_year),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data for FIPS {fips}")
    cols = ["period_year", "period_month", "tavg_f", "tmax_f", "tmin_f",
            "prcp_in", "anomaly_f", "has_data_gap"]
    return {"fips": fips.zfill(5), "series": [dict(zip(cols, r)) for r in rows]}


@router.get("/climate/map/{year}/{month}")
def national_snapshot(year: int, month: int):
    """All counties for one calendar month - this is what the frontend's
    time-slider actually pulls per frame for the choropleth."""
    with get_cursor() as cur:
        cur.execute(
            """
            select fips_code, county_name, state_abbr, tavg_f, anomaly_f, has_data_gap
            from marts.mart_climate_trends
            where period_year = %s and period_month = %s
            """,
            (year, month),
        )
        rows = cur.fetchall()
    cols = ["fips_code", "county_name", "state_abbr", "tavg_f", "anomaly_f", "has_data_gap"]
    return {"year": year, "month": month, "counties": [dict(zip(cols, r)) for r in rows]}


@router.get("/available_overlays")
def available_overlays():
    """Stub for Phase 2+ - only climate is live right now. The frontend
    overlay selector reads this list so adding USDA/CDC/FBI later doesn't
    require a frontend code change, just a new entry here."""
    return {
        "overlays": [
            {"id": "climate", "label": "Temperature anomaly", "status": "live"},
            {"id": "usda_crop_yields", "label": "Crop yields", "status": "planned"},
            {"id": "cdc_mortality", "label": "Mortality rate", "status": "planned"},
            {"id": "fbi_crime", "label": "Crime rate", "status": "planned"},
        ]
    }


@router.get("/overlay/{dataset}/{fips}")
def overlay_series(dataset: str, fips: str, start_year: int = 2004, end_year: int = 2026):
    """Placeholder until Phase 2/3 land their marts - returns 501 rather
    than a fake 200 so the frontend can distinguish 'not built yet' from
    'no data for this county'."""
    raise HTTPException(status_code=501, detail=f"Overlay '{dataset}' not implemented yet")


@router.get("/correlation/{dataset_a}/{dataset_b}")
def correlation(dataset_a: str, dataset_b: str, fips: str = Query(...)):
    """Placeholder for the Pearson-r scatter panel - implement once at
    least one overlay mart exists to correlate climate against."""
    raise HTTPException(status_code=501, detail="Correlation endpoint not implemented yet")
