# US Climate Correlation Explorer

Nationwide county-level heatmap of temperature anomaly (2004-present) with a
time slider, built on Railway.

## Data source: NOAA EpiNOAA (county-scale nClimGrid), not GSOD or nClimDiv

You asked me to advise between NOAA GSOD and the Climate Divisional Database.
Neither is actually the best fit once you look closely:

- **GSOD** is raw station data. To get a county-level number you'd have to pick
  representative stations per county and interpolate yourself — real work, and
  accuracy varies a lot by how many stations a county has.
- **Climate Divisional Database (nClimDiv)** does publish "county" files, but
  they duplicate the same climate-division value across every county inside
  that division — neighboring counties in the same division show identical
  numbers. Not real per-county resolution, which matters for a map that's
  supposed to show county-level variation.
- **EpiNOAA** (a NOAA Big Data Program product built on nClimGrid-daily) is the
  grid already aggregated to actual county polygons — real per-county values,
  not shared division averages. Public, free, no API key or rate limit (flat
  CSV/parquet files on S3, unsigned requests), covers 1951–present (comfortably
  covers your 2004+ range), updates monthly. Because it's already keyed by
  FIPS, the "int_county_fips_reference" harmonization step in your original
  architecture doc becomes unnecessary for climate specifically — you still
  want a county reference table for joining the *other* overlays
  (USDA/CDC/FBI), just not for reprojecting climate data.

Source: `s3://noaa-nclimgrid-daily-pds/EpiNOAA/csv/` (AWS Open Data Registry).

**Before you run anything else:** NOAA's exact file naming/column headers for
this product have shifted before. Run this first and eyeball the output:

```
python ingest_noaa_county_climate.py --discover
```

It lists what's actually in the bucket and prints one file's columns. The
parser detects columns by keyword match (`tmax`, `tmin`, `fips`, etc.) rather
than hardcoded position specifically so small header changes don't silently
break it, but confirm the match before trusting a full backfill.

## Folder structure -> Railway services

This is a monorepo. Each top-level folder maps to its own Railway service,
configured by setting that folder as the service's **Root Directory** in the
Railway dashboard. Each service's `railway.json` lives inside its own folder
for that reason - Railway reads it relative to the Root Directory, not the
repo root.

```
climate-explorer/
├── ingestion/          -> Railway service #1: cron job
│   ├── ingest_noaa_county_climate.py
│   ├── requirements.txt
│   └── railway.json     (cron: 3rd of month, 06:00 UTC)
├── dbt/                -> Railway service #2: cron job
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml      (no secrets - reads PG* env vars)
│   ├── requirements.txt
│   ├── railway.json     (cron: 3rd of month, 07:00 UTC - after ingestion)
│   └── models/{staging,intermediate,marts}/
├── api/                -> Railway service #3: web service
│   ├── main.py           (FastAPI app; also serves frontend/ as static files)
│   ├── db.py
│   ├── routers/climate.py
│   ├── requirements.txt
│   └── railway.json
├── frontend/            (not its own Railway service - see below)
│   └── index.html         (Plotly.js choropleth + time slider)
└── sql/
    └── schema_raw.sql    (run once by hand, not a Railway service)
```

**Why frontend/ isn't its own service:** `api/main.py` mounts `frontend/` as
static files on the same FastAPI app, so the API and the page it powers
deploy together as one service. One fewer moving part while the Phase 1
milestone is just "get a live heatmap up." Split it into its own service
(or onto KaslingAnalytics.com) once the frontend outgrows a single page -
at that point also tighten the CORS `allow_origins` in `main.py`, which is
wide open right now for a same-origin setup.

**Why Plotly.js over D3 for the map:** Plotly has a built-in `choropleth`
trace with `scope: "usa"` that needs no Mapbox token and handles the
county-level rendering, hover text, and color scale in a few lines - D3
would give you more visual control but costs real dev time you don't have
budgeted for a Phase 1 milestone. Worth revisiting for Phase 4+ if you want
a more custom look once the data pipeline is proven out.

**Why the raw climate table isn't partitioned yet:** your architecture doc
sketched "partitioned by source + year" for raw tables generally. For this
specific table I'd hold off: at ~3,200 counties × ~270 months you're looking
at well under a million rows, which a single indexed Postgres table handles
fine on a Railway-sized instance, and native partitioning adds real
maintenance overhead (per-partition indexes, more DDL to manage). Worth
doing once you ingest something at daily station-level granularity (if you
ever pull raw GSOD), not for this pre-aggregated county table. Full
reasoning is also inline as a comment in `sql/schema_raw.sql`.

## Deploy order (first time)

1. **Postgres**: add the plugin to your Railway project (or point at the
   same instance DataDr uses, in a separate database). Run `sql/schema_raw.sql`
   against it once via `psql` or any Postgres client.
2. **`ingestion/` service**: set Root Directory to `ingestion`, add env var
   `DATABASE_URL` (reference the Postgres plugin's variable, don't paste a
   literal). For the *first* run only, override the start command to backfill
   the full history the anomaly baseline needs:
   ```
   python ingest_noaa_county_climate.py --start-year 1991 --end-year 2026
   ```
   Then switch it back to the `railway.json` cron command (current-year-only)
   for ongoing monthly runs.
3. **`dbt/` service**: set Root Directory to `dbt`, add env vars `PGHOST`,
   `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` (Railway's Postgres plugin
   exposes these individually), plus `DBT_PROFILES_DIR=.`. For the first run,
   manually trigger it (Railway dashboard "Run now" or `railway run dbt run`
   via the CLI) rather than waiting for the cron schedule, so you're not
   waiting a month to see `analytics_marts.mart_climate_trends` populate.
   (Note: dbt prefixes the configured `marts` schema with the profile's
   default `analytics` schema, so the real table lives in
   `analytics_marts`, not a bare `marts` schema - the API queries the
   correct one.)
4. **`api/` service**: set Root Directory to `api`, add env var
   `DATABASE_URL`. This one's a normal web service (`ON_FAILURE` restart),
   not a cron job. Once it's up, its public Railway URL serves both
   `/api/...` and the frontend at `/`.

After that, steps 2 and 3 run themselves monthly (EpiNOAA publishes new data
roughly monthly) and step 4 stays running continuously.

## What's stubbed vs. live

- Live: `/api/climate/{fips}`, `/api/climate/map/{year}/{month}`, the
  frontend heatmap.
- Stubbed (return `501` on purpose, not fake data): `/api/overlay/*` and
  `/api/correlation/*` - wire these up once a Phase 2/3 overlay mart exists
  to correlate against. `/api/available_overlays` already lists them as
  `"status": "planned"` so the frontend selector doesn't need a code change
  when they go live.

## Embedding on your WordPress site (Ionos, self-hosted)

The frontend is published via an `<iframe>` embed in a WordPress Custom HTML
block, not by pasting the app's HTML/JS directly into page content. Two
reasons: WordPress themes/plugins inject their own CSS/JS that can clash
with Plotly's styling and the slider's event handlers, and the frontend's
`fetch('/api/...')` calls use relative paths that assume same-origin with
the API - paste the raw JS into a WordPress page and those requests resolve
against your WordPress domain instead of Railway and 404. An iframe keeps
the whole app inside its own origin so nothing needs to change.

1. **Custom subdomain for the API/frontend service.** In Ionos DNS, add a
   CNAME record, e.g. `climate` -> your Railway app's `*.up.railway.app`
   target. Then in the Railway `api` service settings, add that custom
   domain (`climate.kaslinganalytics.com` or similar) so you're iframing a
   clean URL instead of a raw Railway one.
2. **Custom HTML block** on whatever WordPress page/post you're embedding
   this in:
   ```html
   <iframe id="climate-explorer" src="https://climate.kaslinganalytics.com/"
           style="width:100%; border:0; display:block;" scrolling="no"></iframe>
   <script>
     window.addEventListener("message", function (e) {
       if (e.data && e.data.type === "climate-explorer-resize") {
         document.getElementById("climate-explorer").style.height = e.data.height + "px";
       }
     });
   </script>
   ```
   The frontend already posts its content height on load/resize (see the
   `reportHeight()` script at the bottom of `frontend/index.html`), so the
   iframe grows/shrinks to fit instead of you having to guess a fixed
   height per theme/column width.
3. **Column width.** A US county choropleth is dense - most WordPress theme
   content columns (~700-800px) will feel cramped. Check whether your theme
   offers a full-width/full-bleed page template or block layout for the page
   this lives on, rather than dropping it into a standard post column.

## Suggested next step

Backfill 1991–2026 (not just 2004+) in one run so the anomaly baseline in
`dbt/models/intermediate/int_climate_anomaly_by_county.sql` has real data to
average over, then run `dbt run` and spot-check `mart_climate_trends` for a
couple of Florida counties against the 2°F finding before considering Phase 1
done.
