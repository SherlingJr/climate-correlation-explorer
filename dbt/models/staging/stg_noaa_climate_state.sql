-- staging/stg_noaa_climate_state.sql
-- One row per state per month. State-number-to-FIPS remapping already
-- happened at ingestion (ingest_noaa_state_climate.py) - this is a
-- straight cast/rename, no business logic.

with source as (

    select * from {{ source('raw', 'noaa_state_climate_monthly') }}

),

cleaned as (

    select
        state_fips,
        state_abbr,
        period_year::int                              as period_year,
        period_month::int                              as period_month,
        make_date(period_year, period_month, 1)        as period_date,
        tmax_f::numeric                                as tmax_f,
        tmin_f::numeric                                as tmin_f,
        tavg_f::numeric                                as tavg_f,
        prcp_in::numeric                                as prcp_in,
        source_file,
        ingested_at
    from source
    where state_fips is not null
      and period_month between 1 and 12

)

select * from cleaned
