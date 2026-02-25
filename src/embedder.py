"""Create Gemini embeddings for extracted document text (per-page chunking)."""

import json
import logging
import os
import time

from google import genai
from google.genai import types

from db import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

BATCH_SIZE = 8
BATCH_DELAY = 0.5
MODEL = "gemini-embedding-001"
DIMENSIONS = 1536
CHUNK_WORDS = 500
OVERLAP_WORDS = 50
MIN_PAGE_WORDS = 10
MAX_PAGE_WORDS_SINGLE = 1000


def pages_to_chunks(doc_json: str) -> list[dict]:
    """Convert per-page JSON to chunks. One chunk per page unless page > 1000 words."""
    data = json.loads(doc_json)
    pages = data.get("pages", [])
    chunks = []

    for page in pages:
        content = page["content"]
        words = content.split()
        word_count = len(words)
        page_num = page["page_number"]

        if word_count < MIN_PAGE_WORDS:
            continue

        if word_count <= MAX_PAGE_WORDS_SINGLE:
            # Single chunk for this page
            chunks.append({
                "page_number": page_num,
                "chunk_index": 0,
                "text": content,
                "word_count": word_count,
                "metadata": json.dumps({
                    "page_number": page_num,
                    "page_word_count": word_count,
                    "page_chunk_index": 0,
                    "page_chunk_count": 1,
                }),
            })
        else:
            # Split into sub-chunks of ~500 words with 50 word overlap
            sub_chunks = []
            start = 0
            while start < len(words):
                end = start + CHUNK_WORDS
                chunk_text = " ".join(words[start:end])
                sub_chunks.append(chunk_text)
                if end >= len(words):
                    break
                start = end - OVERLAP_WORDS

            for idx, chunk_text in enumerate(sub_chunks):
                chunks.append({
                    "page_number": page_num,
                    "chunk_index": idx,
                    "text": chunk_text,
                    "word_count": len(chunk_text.split()),
                    "metadata": json.dumps({
                        "page_number": page_num,
                        "page_word_count": word_count,
                        "page_chunk_index": idx,
                        "page_chunk_count": len(sub_chunks),
                    }),
                })

    return chunks


def embed_texts(client: genai.Client, texts: list[str], task_type: str = "RETRIEVAL_DOCUMENT") -> list[list[float]]:
    """Embed a batch of texts using Gemini with retry on 429."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = client.models.embed_content(
                model=MODEL,
                contents=texts,
                config=types.EmbedContentConfig(
                    output_dimensionality=DIMENSIONS,
                    task_type=task_type,
                ),
            )
            return [list(emb.values) for emb in response.embeddings]
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                wait = BATCH_DELAY * (2 ** attempt)
                log.warning(f"Rate limited (attempt {attempt+1}/{max_retries}), waiting {wait:.1f}s")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"Failed after {max_retries} retries")


def embed_query(query: str) -> list[float]:
    """Embed a search query."""
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    vectors = embed_texts(client, [query], task_type="RETRIEVAL_QUERY")
    return vectors[0]


def run():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        log.error("GEMINI_API_KEY not set")
        return

    client = genai.Client(api_key=api_key)
    db = Database()

    attachments = db.get_unembedded_attachments()
    log.info(f"Found {len(attachments)} documents to embed")

    total_chunks = 0
    for i, att in enumerate(attachments, 1):
        doc_json = att["full_text"]
        chunks = pages_to_chunks(doc_json)

        if not chunks:
            log.info(f"[{i}/{len(attachments)}] attachment_id={att['attachment_id']}: no chunks (all pages too short)")
            continue

        log.info(
            f"[{i}/{len(attachments)}] attachment_id={att['attachment_id']}: "
            f"{len(chunks)} chunks, {att['char_count']} chars"
        )

        # Embed in batches
        all_embeddings = []
        for batch_start in range(0, len(chunks), BATCH_SIZE):
            batch_texts = [c["text"] for c in chunks[batch_start:batch_start + BATCH_SIZE]]
            vectors = embed_texts(client, batch_texts)
            all_embeddings.extend(vectors)
            if batch_start + BATCH_SIZE < len(chunks):
                time.sleep(BATCH_DELAY)

        # Attach embeddings to chunk data
        for chunk, vec in zip(chunks, all_embeddings):
            chunk["embedding"] = json.dumps(vec)

        db.insert_chunks_and_embeddings(att["attachment_id"], chunks)
        total_chunks += len(chunks)

        # Delay between documents
        time.sleep(BATCH_DELAY)

    log.info(f"Done: embedded {total_chunks} chunks from {len(attachments)} documents")
    db.conn.close()


def main():
    run()


if __name__ == "__main__":
    main()
