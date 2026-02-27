"""Search endpoint — Qdrant semantic / lexical / hybrid search."""

from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

router = APIRouter(tags=["search"])


class SearchRequest(BaseModel):
    query: str
    mode: Literal["semantic", "lexical", "hybrid"] = "hybrid"
    form_type: str = ""
    company: str = ""
    date_from: str = ""
    date_to: str = ""
    limit: int = 10


def _build_filter(req: SearchRequest):
    """Build Qdrant Filter from request metadata fields."""
    from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

    must_conditions = []
    if req.form_type:
        must_conditions.append(
            FieldCondition(key="form_type", match=MatchValue(value=req.form_type))
        )
    if req.company:
        must_conditions.append(
            FieldCondition(key="company_name", match=MatchValue(value=req.company))
        )
    if req.date_from:
        # report_date stored as integer YYYYMMDD
        must_conditions.append(
            FieldCondition(key="report_date", range=Range(gte=float(int(req.date_from.replace("-", "")))))
        )
    if req.date_to:
        must_conditions.append(
            FieldCondition(key="report_date", range=Range(lte=float(int(req.date_to.replace("-", "")))))
        )

    return Filter(must=must_conditions) if must_conditions else None


@router.post("/search")
def search(req: SearchRequest):
    """Search Qdrant magna collection in semantic, lexical, or hybrid mode."""
    try:
        from config import QDRANT_URL, QDRANT_COLLECTION
        from qdrant_client import QdrantClient, models
        from pipeline.indexer import _embed_texts

        client = QdrantClient(url=QDRANT_URL)
        query_filter = _build_filter(req)

        if req.mode == "semantic":
            query_vector = _embed_texts([req.query], task_type="RETRIEVAL_QUERY")[0]
            results = client.query_points(
                collection_name=QDRANT_COLLECTION,
                query=query_vector,
                query_filter=query_filter,
                limit=req.limit,
            )

        elif req.mode == "lexical":
            results = client.query_points(
                collection_name=QDRANT_COLLECTION,
                query=models.Document(text=req.query, model="Qdrant/bm25"),
                using="bm25",
                query_filter=query_filter,
                limit=req.limit,
            )

        else:  # hybrid
            query_vector = _embed_texts([req.query], task_type="RETRIEVAL_QUERY")[0]
            results = client.query_points(
                collection_name=QDRANT_COLLECTION,
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                prefetch=[
                    models.Prefetch(
                        query=query_vector,
                        filter=query_filter,
                        limit=req.limit,
                    ),
                    models.Prefetch(
                        query=models.Document(text=req.query, model="Qdrant/bm25"),
                        using="bm25",
                        filter=query_filter,
                        limit=req.limit,
                    ),
                ],
                limit=req.limit,
            )

        return {
            "results": [
                {
                    "score": point.score,
                    "payload": point.payload,
                }
                for point in results.points
            ]
        }
    except Exception as e:
        raise HTTPException(500, str(e))
