-- marts/mart_state_climate_annual.sql
-- One row per state per year. Powers the primary year-picker map, the
-- start-year/end-year compare map, and the annual trend chart.
--
-- Annual anomaly is the unweighted average of that year's monthly anomalies
-- (each already computed against the same calendar month's 1991-2020
-- baseline) - not a separate annual baseline calc, so a state's annual
-- number is directly traceable back to its monthly detail.
--
-- months_reported flags partial years (the current year, before December's
-- data exists) so the frontend can visually distinguish "this year so far"
-- from a complete year rather than silently averaging a partial year as if
-- it were final - same intellectual-honesty principle as the has_data_gap
-- flag on the monthly mart.

select
    state_fips,
    state_abbr,
    period_year,
    avg(tavg_f)                          as tavg_f,
    avg(tmax_f)                          as tmax_f,
    avg(tmin_f)                          as tmin_f,
    sum(prcp_in)                         as prcp_in,
    avg(anomaly_f)                       as anomaly_f,
    count(period_month)                  as months_reported,
    (count(period_month) < 12)           as is_partial_year,
    bool_or(is_baseline_missing)         as has_data_gap
from {{ ref('int_climate_anomaly_by_state') }}
group by state_fips, state_abbr, period_year
