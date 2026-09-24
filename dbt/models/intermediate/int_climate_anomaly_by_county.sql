-- intermediate/int_climate_anomaly_by_county.sql
--
-- Anomaly = this county's tavg for a given month minus that same county's
-- average tavg for that same calendar month across the baseline window.
-- Comparing March to March (not March to the yearly average) is what
-- makes this a real anomaly rather than a seasonal cycle.
--
-- Baseline window is configurable via dbt vars so you're not locked in:
--   dbt run --vars '{"baseline_start_year": 1991, "baseline_end_year": 2020}'
-- Defaults to NOAA's standard 1991-2020 climate normal period. IMPORTANT:
-- your ingestion script currently defaults to backfilling from 2004, which
-- does not cover this baseline. Either backfill from 1991 for the baseline
-- calculation, or override these vars to a window inside your actual data
-- (e.g. 2004-2013) - just be explicit in the UI about which baseline is in
-- use, since it changes what "anomaly" means.

{% set baseline_start = var('baseline_start_year', 1991) %}
{% set baseline_end = var('baseline_end_year', 2020) %}

with monthly as (

    select * from {{ ref('stg_noaa_climate_county') }}

),

baseline as (

    select
        fips_code,
        period_month,
        avg(tavg_f) as baseline_tavg_f
    from monthly
    where period_year between {{ baseline_start }} and {{ baseline_end }}
    group by fips_code, period_month

),

joined as (

    select
        m.fips_code,
        m.period_year,
        m.period_month,
        m.period_date,
        m.tmax_f,
        m.tmin_f,
        m.tavg_f,
        m.prcp_in,
        b.baseline_tavg_f,
        m.tavg_f - b.baseline_tavg_f as anomaly_f,
        (b.baseline_tavg_f is null)  as is_baseline_missing
    from monthly m
    left join baseline b
        on m.fips_code = b.fips_code
       and m.period_month = b.period_month

)

select * from joined
