"""Stage 3: Download PDF/TXT attachments from MAGNA."""

import logging
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db_v2 import Database
from config import (
    MAGNA_URL, TIMEOUT_MS, VIEWPORT, ATTACHMENTS_DIR,
    DELAY_BETWEEN_DOWNLOADS, MAX_RETRIES,
)

log = logging.getLogger(__name__)


def run(headless: bool = True, reprocess: bool = False, cancel_check=None, progress_cb=None):
    """Download all pending attachments.
    When reprocess=True, re-download all attachments."""
    db = Database()
    pending = db.get_pending_attachments(reprocess=reprocess)

    if not pending:
        log.info("No pending attachments to download.")
        db.close()
        return

    total = len(pending)
    log.info(f"Downloading {total} pending attachments...")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            slow_mo=50,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            viewport=VIEWPORT,
            locale="he-IL",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        log.info("Loading MAGNA SPA to establish session...")
        page.goto(MAGNA_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
        page.wait_for_load_state("networkidle", timeout=15000)
        time.sleep(2)
        log.info(f"Session established: {page.title()}")

        for i, att in enumerate(pending, 1):
            if cancel_check and cancel_check():
                log.info("Download cancelled.")
                break

            att_id = att["id"]
            ref = att["reference_number"]
            raw_filename = att["filename"] or f"attachment_{att_id}"
            url = att["url"]

            # Sanitize filename to prevent path traversal
            from pathlib import PurePosixPath
            filename = PurePosixPath(raw_filename).name  # strips any directory components
            filename = filename.replace("\0", "")  # strip null bytes
            if not filename:
                filename = f"attachment_{att_id}"

            dest_dir = ATTACHMENTS_DIR / ref
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = dest_dir / filename

            # Verify resolved path is within ATTACHMENTS_DIR
            if not dest_path.resolve().is_relative_to(ATTACHMENTS_DIR.resolve()):
                log.warning(f"  [{i}/{total}] Path traversal blocked: {raw_filename}")
                db.mark_failed(att_id)
                continue

            rel_path = f"data/attachments/{ref}/{filename}"

            if not reprocess and dest_path.exists() and dest_path.stat().st_size > 0:
                db.mark_downloaded(att_id, rel_path)
                log.info(f"  [{i}/{total}] Already on disk: {ref}/{filename}")
                if progress_cb:
                    progress_cb(i, total)
                continue

            success = False
            for attempt in range(1 + MAX_RETRIES):
                try:
                    resp = page.request.get(url, timeout=TIMEOUT_MS)
                    if resp.ok:
                        body = resp.body()
                        dest_path.write_bytes(body)
                        db.mark_downloaded(att_id, rel_path)
                        log.info(f"  [{i}/{total}] Downloaded: {ref}/{filename} ({len(body) // 1024}KB)")
                        success = True
                        break
                    else:
                        log.warning(f"  [{i}/{total}] HTTP {resp.status} for {ref}/{filename}")
                except Exception as e:
                    log.warning(f"  [{i}/{total}] Error: {ref}/{filename}: {e}")

                if attempt < MAX_RETRIES:
                    time.sleep(1)

            if not success:
                db.mark_failed(att_id)

            if progress_cb:
                progress_cb(i, total)

            time.sleep(DELAY_BETWEEN_DOWNLOADS)

        browser.close()

    db.close()
    log.info("Download complete.")


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    parser = argparse.ArgumentParser()
    parser.add_argument("--headless", action="store_true")
    args = parser.parse_args()
    run(headless=args.headless)
