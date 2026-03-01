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
            field_schema=PayloadSchemaType.INTEGER,
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


def _prepare_report_chunks(db: Database, report: dict) -> tuple[list[str], list[dict], list[int]]:
    """Build chunks for a single report. Returns (texts, payloads, att_ids).

    Does NOT embed or upsert — just collects the text chunks and their payloads.
    att_ids: attachment IDs that were used (for marking indexed later).
    """
    ref = report["reference_number"]
    form_fields = {}
    if report.get("form_fields"):
        try:
            form_fields = json.loads(report["form_fields"])
        except json.JSONDecodeError:
            pass

    config = get_config(report.get("form_type") or "")
    raw_date = report.get("report_date") or ""
    date_int = int(raw_date.replace("-", "")) if raw_date else 0

    base_payload = {
        "reference_number": ref,
        "report_date": date_int,
        "company_id": report.get("company_id") or "",
        "company_name": report.get("company_name") or "",
        "form_type": report.get("form_type") or "",
        "form_name": report.get("form_name") or "",
        "subject": report.get("subject") or "",
    }

    texts = []
    payloads = []
    att_ids = []

    # 1. Chunk narrative form fields
    fields_dict = form_fields.get("fields", {})
    for field_name in config.get("narrative_fields", []):
        text = fields_dict.get(field_name, "")
        if not text or len(text.strip()) < 20:
            continue
        for chunk in _chunk_text(text):
            payloads.append({
                **base_payload,
                "source_type": "form",
                "field_name": field_name,
                "chunk_index": chunk["chunk_index"],
                "chunk_text": chunk["text"],
            })
            texts.append(chunk["text"])

    # 2. Chunk attachment text
    extracted_atts = db.get_extracted_attachments_for_report(report["id"])
    for att in extracted_atts:
        att_ids.append(att["id"])
        try:
            doc_data = json.loads(att["text_content"])
        except (json.JSONDecodeError, TypeError):
            continue

        for page in doc_data.get("pages", []):
            content = page.get("content", "")
            page_num = page.get("page_number", 0)
            for chunk in _chunk_text(content):
                payloads.append({
                    **base_payload,
                    "source_type": "attachment",
                    "filename": att.get("filename") or "",
                    "page_number": page_num,
                    "chunk_index": chunk["chunk_index"],
                    "chunk_text": chunk["text"],
                })
                texts.append(chunk["text"])

    return texts, payloads, att_ids


def _flush_to_qdrant(pending_reports: list[dict]):
    """Embed all pending texts in full batches, then upsert to Qdrant per report.

    Each entry in pending_reports: {report, texts, payloads, att_ids}.
    """
    from qdrant_client.models import PointStruct, Document

    # 1. Collect all texts across reports into one flat list for batched embedding
    all_texts = []
    report_offsets = []  # (start_idx, count) per report
    for entry in pending_reports:
        start = len(all_texts)
        all_texts.extend(entry["texts"])
        report_offsets.append((start, len(entry["texts"])))

    if not all_texts:
        return

    # 2. Embed in full batches of 100
    all_vectors = []
    batch_size = 100
    for i in range(0, len(all_texts), batch_size):
        batch = all_texts[i:i + batch_size]
        vectors = _embed_texts(batch)
        all_vectors.extend(vectors)

    # 3. Per report: delete old points, build new points, upsert, mark indexed
    client = _get_qdrant()
    for entry, (offset, count) in zip(pending_reports, report_offsets):
        ref = entry["report"]["reference_number"]
        vectors = all_vectors[offset:offset + count]
        payloads = entry["payloads"]

        # Build points
        points = []
        for vector, payload in zip(vectors, payloads):
            points.append(PointStruct(
                id=str(uuid.uuid4()),
                vector={
                    "": vector,
                    "bm25": Document(text=payload["chunk_text"], model="Qdrant/bm25"),
                },
                payload=payload,
            ))

        # Delete old + upsert new
        _delete_report_points(ref)
        for i in range(0, len(points), 200):
            client.upsert(
                collection_name=QDRANT_COLLECTION,
                points=points[i:i + 200],
            )

        entry["n_points"] = len(points)


# How many texts to accumulate before flushing a cross-report embedding batch.
# 200 balances batch efficiency (2 full Gemini API calls of 100) with
# responsive progress logging — avoids long silent accumulation periods.
_FLUSH_THRESHOLD = 200


def run(reprocess: bool = False, since: str = "", company_ids: list[str] | None = None,
        cancel_check=None, progress_cb=None):
    """Index all reports that are parsed but not yet indexed.
    When reprocess=True, re-index all parsed reports.

    Chunks are accumulated across reports and embedded in full batches of 100
    to maximize Gemini API throughput (avoids underfilled single-report calls).
    """
    db = Database()
    log.info(f"Querying reports to index (reprocess={reprocess}, since={since or 'all'})...")
    reports = db.get_reports_needing_index(reprocess=reprocess, since=since, company_ids=company_ids)
    total = len(reports)

    if not reports:
        log.info("No reports to index.")
        db.close()
        return

    log.info(f"Indexing {total} reports to Qdrant...")
    indexed = 0
    total_points = 0
    errors = 0
    _run_start = time.time()

    pending: list[dict] = []  # reports waiting to be flushed
    pending_text_count = 0
    _flush_count = 0

    def _flush():
        """Embed + upsert all pending reports, then mark them indexed."""
        nonlocal indexed, total_points, _flush_count
        if not pending:
            return

        flush_texts = sum(len(e["texts"]) for e in pending)
        flush_reports = len(pending)
        t0 = time.time()

        _flush_to_qdrant(pending)

        elapsed = time.time() - t0
        _flush_count += 1

        for entry in pending:
            n_pts = entry.get("n_points", 0)
            for att_id in entry["att_ids"]:
                db.set_attachment_indexed(att_id)
            if not db.report_has_pending_attachments(entry["report"]["id"]):
                db.set_report_indexed(entry["report"]["id"])
            indexed += 1
            total_points += n_pts

        run_elapsed = time.time() - _run_start
        rate = indexed / run_elapsed * 60 if run_elapsed > 0 else 0
        remaining = total - indexed
        eta_min = remaining / rate if rate > 0 else 0
        log.info(
            f"  Flush #{_flush_count}: {flush_reports} reports, {flush_texts} chunks"
            f" embedded+upserted in {elapsed:.1f}s"
            f" | Progress: {indexed}/{total} ({indexed*100//total}%)"
            f" | {rate:.0f} reports/min | ETA: {eta_min:.0f} min"
        )
        pending.clear()

    for i, report in enumerate(reports, 1):
        if cancel_check and cancel_check():
            log.info("Indexing cancelled, flushing pending...")
            _flush()
            break

        try:
            texts, payloads, att_ids = _prepare_report_chunks(db, report)
            if texts:
                pending.append({
                    "report": report,
                    "texts": texts,
                    "payloads": payloads,
                    "att_ids": att_ids,
                })
                pending_text_count += len(texts)

                # Flush when buffer exceeds threshold
                if pending_text_count >= _FLUSH_THRESHOLD:
                    _flush()
                    pending_text_count = 0
            else:
                # No chunks — still mark as indexed
                if not db.report_has_pending_attachments(report["id"]):
                    db.set_report_indexed(report["id"])
                indexed += 1

            # Periodic progress so the log never goes silent
            if i % 100 == 0:
                run_elapsed = time.time() - _run_start
                rate = i / run_elapsed * 60 if run_elapsed > 0 else 0
                log.info(
                    f"  Scanned {i}/{total} reports"
                    f" | indexed: {indexed}, buffered: {len(pending)} ({pending_text_count} chunks)"
                    f" | {rate:.0f} reports/min"
                )

        except Exception as e:
            errors += 1
            log.error(f"  [{i}/{total}] FAILED {report['reference_number']}: {e}")

        if progress_cb:
            progress_cb(i, total)

    # Final flush for remaining reports
    try:
        _flush()
    except Exception as e:
        errors += 1
        log.error(f"  Final flush failed: {e}")

    log.info(f"Indexing complete: {indexed} reports, {total_points} points, {errors} errors")
    db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    for _name in ("httpx", "httpcore", "urllib3", "google", "qdrant_client"):
        logging.getLogger(_name).setLevel(logging.WARNING)
    run()
