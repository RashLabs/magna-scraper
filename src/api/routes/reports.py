"""Data browse endpoints for reports and attachments."""

import json
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from api.deps import get_db
from config import DATA_DIR

router = APIRouter(tags=["data"])

# ── MAGNA entity registry (loaded once for search) ───────────
_magna_entities: list[dict] | None = None


def _get_magna_entities() -> list[dict]:
    global _magna_entities
    if _magna_entities is None:
        path = DATA_DIR / "magna_entities.json"
        if not path.exists():
            raise HTTPException(404, f"MAGNA entities file not found: {path}")
        _magna_entities = json.loads(path.read_text(encoding="utf-8"))
    return _magna_entities


# ── Companies ────────────────────────────────────────────────


class AddCompanyRequest(BaseModel):
    magna_id: str
    name: str
    magna_name: str | None = None
    english_name: str | None = None
    symbol: str | None = None
    tase_number: str | None = None
    isin: str | None = None


@router.get("/companies")
def list_companies():
    """Return the company list from DB (seeded from TA-125 + manually added)."""
    db = get_db()
    return db.get_companies()


@router.post("/companies", status_code=201)
def add_company(body: AddCompanyRequest):
    """Add a company to the tracked list."""
    db = get_db()
    inserted = db.add_company(
        magna_id=body.magna_id, name=body.name, magna_name=body.magna_name,
        english_name=body.english_name, symbol=body.symbol, tase_number=body.tase_number,
        isin=body.isin,
    )
    if not inserted:
        raise HTTPException(409, f"Company {body.magna_id} already exists")
    return {"ok": True, "magna_id": body.magna_id}


@router.delete("/companies/{magna_id}")
def remove_company(magna_id: str):
    """Remove a company from the tracked list."""
    db = get_db()
    removed = db.remove_company(magna_id)
    if not removed:
        raise HTTPException(404, f"Company {magna_id} not found")
    return {"ok": True}


@router.get("/companies/search")
def search_companies(q: str = Query(..., min_length=1)):
    """Substring search on MAGNA entity registry, excluding already-added companies."""
    db = get_db()
    existing_ids = {c["magna_id"] for c in db.get_companies()}
    entities = _get_magna_entities()

    query = q.strip().lower()
    results = []
    for ent in entities:
        if str(ent["id"]) in existing_ids:
            continue
        if query in ent.get("name", "").lower() or query in str(ent.get("id", "")):
            results.append({"magna_id": str(ent["id"]), "name": ent["name"]})
            if len(results) >= 20:
                break
    return results


@router.get("/reports")
def list_reports(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
    form_type: str = "",
    company: str = "",
    search: str = "",
    status: str = "",
):
    db = get_db()
    return db.get_reports_page(
        page=page, size=size, form_type=form_type,
        company=company, search=search, status=status,
    )


@router.get("/reports/{report_id}")
def get_report(report_id: int):
    db = get_db()
    report = db.get_report(report_id)
    if not report:
        raise HTTPException(404, "Report not found")
    report["attachments"] = db.get_report_attachments(report_id)
    return report


@router.get("/attachments")
def list_attachments(
    page: int = Query(1, ge=1),
    size: int = Query(50, ge=1, le=200),
    status: str = "",
    report_id: int = 0,
):
    db = get_db()
    return db.get_attachments_page(
        page=page, size=size, status=status, report_id=report_id,
    )


@router.get("/stats")
def get_stats():
    db = get_db()
    result = db.stats()

    # Add live Qdrant point count so admin can detect drift
    try:
        from qdrant_client import QdrantClient
        from config import QDRANT_URL, QDRANT_COLLECTION
        client = QdrantClient(url=QDRANT_URL)
        info = client.get_collection(QDRANT_COLLECTION)
        result["qdrant"] = {
            "points_count": info.points_count,
            "status": info.status.value if info.status else "unknown",
        }
    except Exception:
        result["qdrant"] = {"points_count": None, "status": "unavailable"}

    return result


@router.get("/form-types")
def get_form_types():
    db = get_db()
    return db.form_type_counts()
