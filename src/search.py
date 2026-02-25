"""Semantic search over embedded document chunks."""

import argparse
import json
import logging
import math

from db import Database
from embedder import embed_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def search(query: str, top_k: int = 5) -> list[dict]:
    """Search for chunks matching the query."""
    log.info(f"Embedding query: {query!r}")
    query_vec = embed_query(query)

    db = Database()
    all_embs = db.get_all_embeddings()
    db.conn.close()

    log.info(f"Searching {len(all_embs)} chunks...")

    results = []
    for row in all_embs:
        vec = json.loads(row["embedding"])
        score = cosine_similarity(query_vec, vec)
        results.append({
            "score": score,
            "chunk_text": row["chunk_text"],
            "chunk_index": row["chunk_index"],
            "page_number": row["page_number"],
            "attachment_id": row["attachment_id"],
            "report_reference": row["report_reference"],
            "filename": row["filename"],
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]


def main():
    parser = argparse.ArgumentParser(description="Semantic search over MAGNA documents")
    parser.add_argument("query", help="Search query text")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results")
    args = parser.parse_args()

    results = search(args.query, args.top_k)

    for i, r in enumerate(results, 1):
        print(f"\n{'='*60}")
        print(f"Result {i} | Score: {r['score']:.4f}")
        print(f"Report: {r['report_reference']} | File: {r['filename']} | Page: {r['page_number']} | Chunk: {r['chunk_index']}")
        print(f"{'─'*60}")
        text = r["chunk_text"]
        if len(text) > 300:
            text = text[:300] + "..."
        print(text)


if __name__ == "__main__":
    main()
