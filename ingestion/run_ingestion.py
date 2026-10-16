"""
run_ingestion.py - single entry point for the ingestion service's cron.

Runs each source in turn. NOAA only re-pulls the previous and current year
(enough to pick up the newly published month and any revisions); Census is
tiny, so it is refreshed in full each run. One source failing does not stop
the others, but the exit code is non-zero if any failed.
"""

import logging
import subprocess
import sys
from datetime import date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("run_ingestion")

this_year = date.today().year
STEPS = [
    ("noaa_state_climate",
     [sys.executable, "ingest_noaa_state_climate.py",
      "--start-year", str(this_year - 1), "--end-year", str(this_year)]),
    ("census_population", [sys.executable, "ingest_census_population.py"]),
]

failed = []
for name, cmd in STEPS:
    log.info("=== %s ===", name)
    if subprocess.run(cmd).returncode != 0:
        log.error("%s FAILED", name)
        failed.append(name)

if failed:
    log.error("Failed steps: %s", ", ".join(failed))
    sys.exit(1)
log.info("All ingestion steps finished")
