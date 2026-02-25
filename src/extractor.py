"""Extract text from downloaded PDF/TXT attachments (per-page)."""

import json
import logging
import sys
from pathlib import Path

import meowpdf

from db import Database

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def extract_pages(local_path: str) -> list[dict]:
    """Extract text per page. Returns list of {content, word_count, page_number}."""
    path = PROJECT_ROOT / local_path
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".pdf":
        raw_pages = meowpdf.extract_text_pages(str(path))
        pages = []
        for i, text in enumerate(raw_pages, 1):
            pages.append({
                "content": text,
                "word_count": len(text.split()),
                "page_number": i,
            })
        return pages
    elif suffix == ".txt":
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = path.read_text(encoding="windows-1255")
        return [{
            "content": text,
            "word_count": len(text.split()),
            "page_number": 1,
        }]
    else:
        raise ValueError(f"Unsupported file type: {suffix}")


def run(reset: bool = False):
    db = Database()

    if reset:
        log.info("Resetting extraction tables (dropping doc_texts, chunks, embeddings)...")
        db.reset_extraction_tables()

    attachments = db.get_downloaded_attachments()
    total = len(attachments)

    # In incremental mode, skip already-extracted attachments
    if not reset:
        attachments = [a for a in attachments if not db.is_extracted(a["id"])]

    if not attachments:
        log.info(f"Nothing to extract. {total} downloaded, all already extracted.")
        db.conn.close()
        return

    log.info(f"Extracting {len(attachments)}/{total} attachments (reset={reset})...")

    extracted, errors = 0, 0
    for att in attachments:
        try:
            pages = extract_pages(att["local_path"])
            doc_json = json.dumps({"pages": pages}, ensure_ascii=False)
            total_chars = sum(len(p["content"]) for p in pages)
            db.insert_doc_text(att["id"], doc_json, total_chars)
            extracted += 1
            log.info(
                f"[{extracted}] Extracted {att['filename']} -> "
                f"{len(pages)} pages, {total_chars} chars"
            )
        except Exception as e:
            db.insert_doc_text(att["id"], "", 0)
            errors += 1
            log.warning(f"Failed to extract {att['filename']}: {e}")

    log.info(f"Done: {extracted} extracted, {errors} errors")
    db.conn.close()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Extract text from downloaded attachments")
    parser.add_argument("--reset", action="store_true",
                        help="Drop and recreate doc_texts/chunks/embeddings before extracting")
    args = parser.parse_args()
    run(reset=args.reset)


if __name__ == "__main__":
    main()
