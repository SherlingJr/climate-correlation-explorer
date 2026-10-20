-- marts/mart_overlay_annual.sql
-- Every overlay dataset in one long table: one row per dataset per state per
-- year. The API reads this with a single `where dataset = ...` query, so
-- adding an overlay (BLS, USDA, FBI, CDC) means adding one union branch
-- below plus one entry in api/routers/climate.py - no new endpoint, no new
-- query shape.
--
-- value is in the dataset's natural unit (people for population). The
-- frontend decides how to display it (e.g. % change since the start year).

select
    'census_population'::text   as dataset,
    state_fips,
    state_abbr,
    period_year,
    population::numeric         as value
from {{ ref('stg_census_population') }}

-- To add an overlay, append a branch like:
--   union all
--   select 'bls_unemployment'::text, state_fips, state_abbr, period_year, value
--   from <ref to the new staging model>
-- NOTE: never put curly-brace Jinja in a SQL comment - dbt renders it anyway.
