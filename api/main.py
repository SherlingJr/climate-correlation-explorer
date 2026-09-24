from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from db import init_pool, close_pool
from routers.climate import router as climate_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pool()
    yield
    close_pool()


app = FastAPI(title="Climate Correlation Explorer API", lifespan=lifespan)

# Wide open for now since this is a public read-only portfolio API with no
# auth and no write endpoints - tighten allow_origins if you later split
# the frontend onto KaslingAnalytics.com and want to restrict it.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(climate_router)

# Serving the static frontend from the same FastAPI service (rather than a
# separate Railway service) is a deliberate Phase 1 simplification - one
# fewer service to deploy and keep in sync while the milestone is just
# "get a live heatmap up." Split it out once the frontend grows past a
# single page or you want it on a different domain.
# frontend/ lives inside api/ (not as a repo-root sibling) specifically
# because Railway's Root Directory setting for this service is "api" -
# the container only ever sees what's inside that folder, so a sibling
# directory at the repo root is invisible to it.
frontend_dir = Path(__file__).parent / "frontend"
app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
