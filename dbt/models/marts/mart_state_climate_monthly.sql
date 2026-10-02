-- marts/mart_state_climate_monthly.sql
-- One row per state per month. Powers the seasonal drill-down (e.g. "July
-- in Georgia across every year on record") and the monthly-resolution
-- trend chart when a user picks a tight date range.

select
    state_fips,
    state_abbr,
    period_year,
    period_month,
    period_date,
    tmax_f,
    tmin_f,
    tavg_f,
    prcp_in,
    anomaly_f,
    is_baseline_missing,
    (tavg_f is null or is_baseline_missing) as has_data_gap
from {{ ref('int_climate_anomaly_by_state') }}
