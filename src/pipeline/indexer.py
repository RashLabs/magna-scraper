"""Stage 5: Embed content and upsert to Qdrant.

For each report:
1. Delete all existing Qdrant points for that reference_number
2. Chunk narrative form fields → embed → collect points
3. Chunk extracted attachment text → embed → collect points
4. Batch upsert all points
5. Mark report + attachments as indexed
"""

import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db_v2 import Database
from config import (
    QDRANT_URL, QDRANT_COLLECTION, GEMINI_API_KEY,
    EMBEDDING_MODEL, EMBEDDING_DIMS,
    CHUNK_WORDS, OVERLAP_WORDS, MIN_PAGE_WORDS, MAX_PAGE_WORDS_SINGLE,
)
from pipeline.form_configs import get_config

log = logging.getLogger(__name__)

# Lazy imports for qdrant and genai — only needed when actually indexing
_qdrant_client = None
_genai_client = None


def _get_qdrant():
    global _qdrant_client
    if _qdrant_client is None:
        from qdrant_client import QdrantClient
        _qdrant_client = QdrantClient(url=QDRANT_URL)
        _ensure_collection()
    return _qdrant_client


def _ensure_collection():
    from qdrant_client.models import Distance, VectorParams, PayloadSchemaType
    from qdrant_client.models import SparseVectorParams, Modifier
    client = _qdrant_client
    collections = [c.name for c in client.get_collections().collections]
    if QDRANT_COLLECTION not in collections:
        client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=EMBEDDING_DIMS, distance=Distance.COSINE),
            sparse_vectors_config={
                "bm25": SparseVectorParams(modifier=Modifier.IDF),
            },
        )
        # Create payload indexes
        for field in ["source_type", "reference_number", "company_id", "form_type", "company_name"]:
            client.create_payload_index(
                collection_name=QDRANT_COLLECTION,
                field_name=field,
                field_schema=PayloadSchemaType.KEYWORD,
            )
        client.create_payload_index(
            collection_name=QDRANT_COLLECTION,
            field_name="report_date",
            field_schema=PayloadSchemaType.KEYWORD,
        )
        log.info(f"Created Qdrant collection '{QDRANT_COLLECTION}'")
    else:
        # Ensure BM25 sparse vector config exists on pre-existing collections
        try:
            info = client.get_collection(QDRANT_COLLECTION)
            if not info.config.params.sparse_vectors or "bm25" not in info.config.params.sparse_vectors:
                client.update_collection(
                    collection_name=QDRANT_COLLECTION,
                    sparse_vectors_config={
                        "bm25": SparseVectorParams(modifier=Modifier.IDF),
                    },
                )
                log.info("Added BM25 sparse vector config to existing collection")
        except Exception as e:
            log.warning(f"Could not add BM25 sparse config to existing collection: {e}")


def _get_genai():
    global _genai_client
    if _genai_client is None:
        from google import genai
        api_key = GEMINI_API_KEY or os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY not set")
        _genai_client = genai.Client(api_key=api_key)
    return _genai_client


def _embed_texts(texts: list[str], task_type: str = "RETRIEVAL_DOCUMENT") -> list[list[float]]:
    """Embed texts with retry on rate limit."""
    from google.genai import types
    client = _get_genai()
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = client.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=texts,
                config=types.EmbedContentConfig(
                    output_dimensionality=EMBEDDING_DIMS,
                    task_type=task_type,
                ),
            )
            return [list(emb.values) for emb in response.embeddings]
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                wait = 0.5 * (2 ** attempt)
                log.warning(f"Rate limited, waiting {wait:.1f}s")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("Embedding failed after retries")


def _chunk_text(text: str) -> list[dict]:
    """Split text into chunks. Returns [{text, word_count, chunk_index}]."""
    words = text.split()
    if len(words) < MIN_PAGE_WORDS:
        return []

    if len(words) <= MAX_PAGE_WORDS_SINGLE:
        return [{"text": text, "word_count": len(words), "chunk_index": 0}]

    chunks = []
    start = 0
    idx = 0
    while start < len(words):
        end = start + CHUNK_WORDS
        chunk_text = " ".join(words[start:end])
        chunks.append({"text": chunk_text, "word_count": len(chunk_text.split()), "chunk_index": idx})
        if end >= len(words):
            break
        start = end - OVERLAP_WORDS
        idx += 1
    return chunks


def _delete_report_points(ref: str):
    """Delete all Qdrant points for a given reference_number."""
    from qdrant_client.models import Filter, FieldCondition, MatchValue, FilterSelector
    client = _get_qdrant()
    client.delete(
        collection_name=QDRANT_COLLECTION,
        points_selector=FilterSelector(
            filter=Filter(must=[
                FieldCondition(key="reference_number", match=MatchValue(value=ref))
            ])
        ),
    )


def _index_report(db: Database, report: dict) -> int:
    """Index a single report. Returns number of points upserted."""
    from qdrant_client.models import PointStruct

    ref = report["reference_number"]
    form_fields = {}
    if report.get("form_fields"):
        try:
            form_fields = json.loads(report["form_fields"])
        except json.JSONDecodeError:
            pass

    config = get_config(report.get("form_type") or "")
    base_payload = {
        "reference_number": ref,
        "report_date": report.get("report_date") or "",
        "company_id": report.get("company_id") or "",
        "company_name": report.get("company_name") or "",
        "form_type": report.get("form_type") or "",
        "form_name": report.get("form_name") or "",
        "subject": report.get("subject") or "",
    }

    points = []
    texts_to_embed = []
    point_payloads = []

    # 1. Chunk narrative form fields
    fields_dict = form_fields.get("fields", {})
    for field_name in config.get("narrative_fields", []):
        text = fields_dict.get(field_name, "")
        if not text or len(text.strip()) < 20:
            continue
        for chunk in _chunk_text(text):
            payload = {
                **base_payload,
                "source_type": "form",
                "field_name": field_name,
                "chunk_index": chunk["chunk_index"],
                "chunk_text": chunk["text"],
            }
            texts_to_embed.append(chunk["text"])
            point_payloads.append(payload)

    # 2. Chunk attachment text
    extracted_atts = db.get_extracted_attachments_for_report(report["id"])
    for att in extracted_atts:
        try:
            doc_data = json.loads(att["text_content"])
        except (json.JSONDecodeError, TypeError):
            continue

        for page in doc_data.get("pages", []):
            content = page.get("content", "")
            page_num = page.get("page_number", 0)
            for chunk in _chunk_text(content):
                payload = {
                    **base_payload,
                    "source_type": "attachment",
                    "filename": att.get("filename") or "",
                    "page_number": page_num,
                    "chunk_index": chunk["chunk_index"],
                    "chunk_text": chunk["text"],
                }
                texts_to_embed.append(chunk["text"])
                point_payloads.append(payload)

    if not texts_to_embed:
        return 0

    # 3. Embed in batches of 40
    all_vectors = []
    batch_size = 40
    for i in range(0, len(texts_to_embed), batch_size):
        batch = texts_to_embed[i:i + batch_size]
        vectors = _embed_texts(batch)
        all_vectors.extend(vectors)
        if i + batch_size < len(texts_to_embed):
            time.sleep(0.5)

    # 4. Build points (dense + BM25 sparse vectors)
    from qdrant_client.models import Document
    for vector, payload in zip(all_vectors, point_payloads):
        points.append(PointStruct(
            id=str(uuid.uuid4()),
            vector={
                "": vector,
                "bm25": Document(text=payload["chunk_text"], model="Qdrant/bm25"),
            },
            payload=payload,
        ))

    # 5. Delete old + upsert new
    _delete_report_points(ref)

    client = _get_qdrant()
    # Upsert in batches of 100
    for i in range(0, len(points), 100):
        client.upsert(
            collection_name=QDRANT_COLLECTION,
            points=points[i:i + 100],
        )

    return len(points)


def run(reprocess: bool = False, since: str = "", cancel_check=None, progress_cb=None):
    """Index all reports that are parsed but not yet indexed.
    When reprocess=True, re-index all parsed reports."""
    db = Database()
    log.info(f"Querying reports to index (reprocess={reprocess}, since={since or 'all'})...")
    reports = db.get_reports_needing_index(reprocess=reprocess, since=since)
    total = len(reports)

    if not reports:
        log.info("No reports to index.")
        db.close()
        return

    log.info(f"Indexing {total} reports to Qdrant...")
    indexed = 0
    total_points = 0
    errors = 0

    for i, report in enumerate(reports, 1):
        if cancel_check and cancel_check():
            log.info("Indexing cancelled.")
            break

        try:
            n_points = _index_report(db, report)

            # Mark indexed attachments
            for att in db.get_extracted_attachments_for_report(report["id"]):
                db.set_attachment_indexed(att["id"])

            # Only mark report fully indexed if no pending/unextracted attachments remain
            if not db.report_has_pending_attachments(report["id"]):
                db.set_report_indexed(report["id"])

            indexed += 1
            total_points += n_points
            log.info(f"  [{indexed}/{total}] {report['reference_number']} — {report.get('company_name','')} — {n_points} pts")

        except Exception as e:
            errors += 1
            log.error(f"  [{i}/{total}] FAILED {report['reference_number']}: {e}")

        if progress_cb:
            progress_cb(i, total)

    log.info(f"Indexing complete: {indexed} reports, {total_points} points, {errors} errors")
    db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    run()
