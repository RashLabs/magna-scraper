"""Magna Scraper v2 — FastAPI application."""

import logging
import sys
from pathlib import Path

# Ensure src/ is on path for absolute imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import API_PORT
from api.routes import pipeline, reports, search

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
)
# Silence noisy HTTP client loggers (qdrant httpx, google-genai, etc.)
for _name in ("httpx", "httpcore", "urllib3", "google", "qdrant_client", "uvicorn.access"):
    logging.getLogger(_name).setLevel(logging.WARNING)

app = FastAPI(
    title="Magna Scraper API",
    version="2.0.0",
    description="Pipeline control and data browse API for MAGNA ISA corporate filings",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # v1: internal only, no auth
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pipeline.router, prefix="/api")
app.include_router(reports.router, prefix="/api")
app.include_router(search.router, prefix="/api")


@app.on_event("startup")
def _startup_export_companies():
    """Export companies.json on every API start so the file is always fresh."""
    from api.deps import get_db
    from api.routes.reports import _export_companies_json
    try:
        _export_companies_json(get_db())
        logging.getLogger(__name__).info("Exported companies.json on startup")
    except Exception:
        logging.getLogger(__name__).warning("Failed to export companies.json on startup", exc_info=True)


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "magna-scraper", "version": "2.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=API_PORT)
