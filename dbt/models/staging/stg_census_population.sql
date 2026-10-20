-- staging/stg_census_population.sql
-- One row per state per year (July 1 population estimate), 2010-present.
-- Straight cast/rename of raw.census_population_annual; no business logic.

select
    state_fips,
    state_abbr,
    period_year::int      as period_year,
    population::bigint    as population,
    source
from {{ source('raw', 'census_population_annual') }}
where state_fips is not null
