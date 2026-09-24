-- marts/mart_climate_trends.sql
-- Final, pre-aggregated table your FastAPI endpoints read from directly.
-- One row per county per month - shaped for /api/climate/{fips}/{year_range}.

with anomaly as (

    select * from {{ ref('int_climate_anomaly_by_county') }}

),

ref_data as (

    select * from {{ source('raw', 'county_fips_reference') }}

)

select
    a.fips_code,
    r.county_name,
    r.state_abbr,
    a.period_year,
    a.period_month,
    a.period_date,
    a.tmax_f,
    a.tmin_f,
    a.tavg_f,
    a.prcp_in,
    a.anomaly_f,
    a.is_baseline_missing,
    -- Gray-out flag for the frontend: distinguishes "no data" from "zero
    -- anomaly" per the intellectual-honesty requirement in the project
    -- brief - a NULL/true here should render as gray, not as 0 on the map.
    (a.tavg_f is null or a.is_baseline_missing) as has_data_gap
from anomaly a
left join ref_data r
    on a.fips_code = r.fips_code
