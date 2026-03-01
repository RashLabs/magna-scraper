"""Stage 4: Extract text from downloaded PDF/TXT attachments via Libre2."""

import json
import logging
import sys
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db_v2 import Database
from config import PROJECT_ROOT, LIBRE_URL

log = logging.getLogger(__name__)


def _extract_via_libre(path: Path) -> list[dict]:
    """POST PDF to Libre2 /extract and map response to page dicts."""
    with open(path, "rb") as f:
        resp = httpx.post(
            f"{LIBRE_URL}/extract",
            files={"file": (path.name, f, "application/pdf")},
            timeout=120.0,
        )
    resp.raise_for_status()
    result = resp.json()

    if not result.get("success"):
        error = result.get("error", {})
        raise RuntimeError(f"Libre extraction failed: {error.get('details', 'unknown error')}")

    return [
        {
            "content": p["content"],
            "word_count": p["word_count"],
            "page_number": p["page"],
        }
        for p in result["pages"]
    ]


def extract_pages(local_path: str) -> list[dict]:
    """Extract text per page from a file."""
    path = PROJECT_ROOT / local_path
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return _extract_via_libre(path)
    elif suffix == ".txt":
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = path.read_text(encoding="windows-1255")
        return [{"content": text, "word_count": len(text.split()), "page_number": 1}]
    else:
        raise ValueError(f"Unsupported file type: {suffix}")


def run(reprocess: bool = False, since: str = "", company_ids: list[str] | None = None,
        cancel_check=None, progress_cb=None):
    """Extract text from all downloaded-but-not-extracted attachments.
    When reprocess=True, re-extract all downloaded attachments."""
    db = Database()
    attachments = db.get_downloaded_unextracted(reprocess=reprocess, since=since, company_ids=company_ids)
    total = len(attachments)

    if not attachments:
        log.info("Nothing to extract.")
        db.close()
        return

    log.info(f"Extracting text from {total} attachments...")
    extracted = 0
    errors = 0

    for i, att in enumerate(attachments, 1):
        if cancel_check and cancel_check():
            log.info("Extraction cancelled.")
            break

        try:
            pages = extract_pages(att["local_path"])
            doc_json = json.dumps({"pages": pages}, ensure_ascii=False)
            total_chars = sum(len(p["content"]) for p in pages)
            db.insert_doc_text(att["id"], doc_json, total_chars)
            db.set_attachment_extracted(att["id"], page_count=len(pages))
            extracted += 1

            if extracted % 50 == 0 or extracted == total:
                log.info(f"  Extracted {extracted}/{total}")

        except Exception as e:
            errors += 1
            log.warning(f"  Failed: {att['filename']}: {e}")

        if progress_cb:
            progress_cb(i, total)

    log.info(f"Extraction complete: {extracted} ok, {errors} errors")
    db.close()


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    parser = argparse.ArgumentParser(description="Stage 4: Extract text from attachments")
    parser.add_argument("--reprocess", action="store_true", help="Re-extract all downloaded attachments")
    parser.add_argument("--since", default="", help="Only process reports from this date (YYYY-MM-DD)")
    parser.add_argument("--company-ids", nargs="+", default=None, help="Only process these company IDs")
    args = parser.parse_args()
    run(reprocess=args.reprocess, since=args.since, company_ids=args.company_ids)
