-- intermediate/int_climate_anomaly_by_state.sql
--
-- Anomaly = this state's tavg for a given month minus that same state's
-- average tavg for that same calendar month across the baseline window
-- (March vs March, not March vs the yearly average).
--
-- Baseline window configurable via dbt vars, defaults to NOAA's standard
-- 1991-2020 climate normal period - matches the backfill range, so the
-- baseline has real data behind it (unlike the county version's first
-- attempt, which defaulted to 2004+ before that was caught and fixed).

{% set baseline_start = var('baseline_start_year', 1991) %}
{% set baseline_end = var('baseline_end_year', 2020) %}

with monthly as (

    select * from {{ ref('stg_noaa_climate_state') }}

),

baseline as (

    select
        state_fips,
        period_month,
        avg(tavg_f) as baseline_tavg_f
    from monthly
    where period_year between {{ baseline_start }} and {{ baseline_end }}
    group by state_fips, period_month

),

joined as (

    select
        m.state_fips,
        m.state_abbr,
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
        on m.state_fips = b.state_fips
       and m.period_month = b.period_month

)

select * from joined
