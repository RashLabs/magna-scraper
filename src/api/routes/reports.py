"""Data browse endpoints for reports and attachments."""

import json
from fastapi import APIRouter, HTTPException, Query

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from api.deps import get_db
from config import COMPANY_LIST_PATH

router = APIRouter(tags=["data"])


@router.get("/companies")
def list_companies():
    """Return the TA-125 company list with magna_id mappings."""
    if not COMPANY_LIST_PATH.exists():
        raise HTTPException(404, f"Company list not found: {COMPANY_LIST_PATH}")
    companies = json.loads(COMPANY_LIST_PATH.read_text(encoding="utf-8"))
    return companies


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
