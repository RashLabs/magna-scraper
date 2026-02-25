"""Stage 4: Extract text from downloaded PDF/TXT attachments."""

import json
import logging
import sys
from pathlib import Path

import meowpdf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db_v2 import Database
from config import PROJECT_ROOT

log = logging.getLogger(__name__)


def extract_pages(local_path: str) -> list[dict]:
    """Extract text per page from a file."""
    path = PROJECT_ROOT / local_path
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".pdf":
        raw_pages = meowpdf.extract_text_pages(str(path))
        return [
            {"content": text, "word_count": len(text.split()), "page_number": i}
            for i, text in enumerate(raw_pages, 1)
        ]
    elif suffix == ".txt":
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = path.read_text(encoding="windows-1255")
        return [{"content": text, "word_count": len(text.split()), "page_number": 1}]
    else:
        raise ValueError(f"Unsupported file type: {suffix}")


def run(cancel_check=None, progress_cb=None):
    """Extract text from all downloaded-but-not-extracted attachments."""
    db = Database()
    attachments = db.get_downloaded_unextracted()
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
            db.set_attachment_extracted(att["id"])
            extracted += 1

            if extracted % 50 == 0 or extracted == total:
                log.info(f"  Extracted {extracted}/{total}")

        except Exception as e:
            # Do NOT mark as extracted — leave it retryable on next run
            errors += 1
            log.warning(f"  Failed: {att['filename']}: {e}")

        if progress_cb:
            progress_cb(i, total)

    log.info(f"Extraction complete: {extracted} ok, {errors} errors")
    db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    run()
