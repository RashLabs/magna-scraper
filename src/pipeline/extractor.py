"""Stage 4: Extract text from downloaded PDF/TXT attachments via Libre2."""

import json
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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
            timeout=300.0,
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


class _DynamicSemaphore:
    """A semaphore whose permit count can be adjusted at runtime.

    Increasing permits releases extra waiters immediately.
    Decreasing permits takes effect as workers finish (no preemption).
    """

    def __init__(self, initial: int):
        self._cond = threading.Condition(threading.Lock())
        self._permits = initial
        self._limit = initial

    def acquire(self):
        with self._cond:
            while self._permits <= 0:
                self._cond.wait()
            self._permits -= 1

    def release(self):
        with self._cond:
            self._permits += 1
            self._cond.notify()

    def set_limit(self, new_limit: int):
        with self._cond:
            delta = new_limit - self._limit
            self._limit = new_limit
            self._permits += delta
            # Wake waiters if we increased permits
            if delta > 0:
                self._cond.notify_all()


def run(reprocess: bool = False, since: str = "", company_ids: list[str] | None = None,
        cancel_check=None, progress_cb=None, retry_errors: bool = False):
    """Extract text from all downloaded-but-not-extracted attachments.
    When reprocess=True, re-extract all downloaded attachments.
    When retry_errors=True, also retry previously-failed extractions.

    Runs extractions in parallel using a ThreadPoolExecutor gated by a
    dynamic semaphore. The worker count is read from the runtime setting
    `extract_workers` and can be changed mid-run via the API.
    """
    from api.deps import get_setting

    db = Database()
    attachments = db.get_downloaded_unextracted(
        reprocess=reprocess, since=since, company_ids=company_ids,
        retry_errors=retry_errors,
    )
    total = len(attachments)

    if not attachments:
        log.info("Nothing to extract.")
        db.close()
        return

    initial_workers = get_setting("extract_workers")
    log.info(f"Extracting text from {total} attachments ({initial_workers} workers)...")

    sem = _DynamicSemaphore(initial_workers)
    counter_lock = threading.Lock()
    extracted = 0
    errors = 0
    done_count = 0

    def _process_one(att: dict):
        """Worker: extract a single attachment. Uses its own DB connection."""
        nonlocal extracted, errors, done_count
        worker_db = Database()
        try:
            pages = extract_pages(att["local_path"])
            doc_json = json.dumps({"pages": pages}, ensure_ascii=False)
            total_chars = sum(len(p["content"]) for p in pages)
            worker_db.insert_doc_text(att["id"], doc_json, total_chars)
            worker_db.set_attachment_extracted(att["id"], page_count=len(pages))

            with counter_lock:
                extracted += 1
                done_count += 1
                local_extracted = extracted
                local_done = done_count

            if local_extracted % 50 == 0 or local_extracted == total:
                log.info(f"  Extracted {local_extracted}/{total}")

            if progress_cb:
                progress_cb(local_done, total)
        except Exception as e:
            with counter_lock:
                errors += 1
                done_count += 1
                local_done = done_count
            log.warning(f"  Failed: {att['filename']}: {e}")
            worker_db.set_extract_failed(att["id"], str(e))
            if progress_cb:
                progress_cb(local_done, total)
        finally:
            worker_db.close()
            sem.release()

    # Monitor thread: adjusts semaphore when the setting changes
    stop_monitor = threading.Event()

    def _monitor_workers():
        last_seen = initial_workers
        while not stop_monitor.is_set():
            stop_monitor.wait(2.0)
            try:
                current = get_setting("extract_workers")
                if current != last_seen:
                    log.info(f"  Extract workers changed: {last_seen} -> {current}")
                    sem.set_limit(current)
                    last_seen = current
            except Exception:
                pass

    monitor = threading.Thread(target=_monitor_workers, daemon=True, name="extract-worker-monitor")
    monitor.start()

    # Dispatch work through the semaphore-gated pool
    # Pool max_workers=20 (the hard cap); actual concurrency controlled by semaphore
    futures = []
    try:
        with ThreadPoolExecutor(max_workers=20, thread_name_prefix="extract") as pool:
            for att in attachments:
                if cancel_check and cancel_check():
                    log.info("Extraction cancelled.")
                    break
                sem.acquire()
                if cancel_check and cancel_check():
                    sem.release()
                    log.info("Extraction cancelled.")
                    break
                futures.append(pool.submit(_process_one, att))

            # Wait for all submitted futures to complete
            for f in as_completed(futures):
                # Propagation not needed; _process_one handles its own errors
                pass
    finally:
        stop_monitor.set()
        monitor.join(timeout=3.0)
        db.close()

    log.info(f"Extraction complete: {extracted} ok, {errors} errors")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    parser = argparse.ArgumentParser(description="Stage 4: Extract text from attachments")
    parser.add_argument("--reprocess", action="store_true", help="Re-extract all downloaded attachments")
    parser.add_argument("--since", default="", help="Only process reports from this date (YYYY-MM-DD)")
    parser.add_argument("--company-ids", nargs="+", default=None, help="Only process these company IDs")
    parser.add_argument("--retry-errors", action="store_true", help="Also retry previously-failed extractions")
    args = parser.parse_args()
    run(reprocess=args.reprocess, since=args.since, company_ids=args.company_ids,
        retry_errors=args.retry_errors)
