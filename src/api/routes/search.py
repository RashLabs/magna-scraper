"""Search endpoint — Qdrant semantic search preview."""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

router = APIRouter(tags=["search"])


class SearchRequest(BaseModel):
    query: str
    form_type: str = ""
    company: str = ""
    date_from: str = ""
    date_to: str = ""
    limit: int = 10


@router.post("/search")
def search(req: SearchRequest):
    """Semantic search against Qdrant magna collection.
    Placeholder — will be implemented when Qdrant integration is tested.
    """
    try:
        from config import QDRANT_URL, QDRANT_COLLECTION
        from qdrant_client import QdrantClient
        from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

        # Build filter
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
            must_conditions.append(
                FieldCondition(key="report_date", range=Range(gte=req.date_from))
            )
        if req.date_to:
            must_conditions.append(
                FieldCondition(key="report_date", range=Range(lte=req.date_to))
            )

        query_filter = Filter(must=must_conditions) if must_conditions else None

        # Embed query
        from pipeline.indexer import _embed_texts
        vectors = _embed_texts([req.query], task_type="RETRIEVAL_QUERY")
        query_vector = vectors[0]

        client = QdrantClient(url=QDRANT_URL)
        results = client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=req.limit,
        )

        return {
            "results": [
                {
                    "score": hit.score,
                    "payload": hit.payload,
                }
                for hit in results
            ]
        }
    except ImportError:
        raise HTTPException(501, "Qdrant client not installed")
    except Exception as e:
        raise HTTPException(500, str(e))
