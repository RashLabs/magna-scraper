"""Download all PDF/TXT attachments from the MAGNA database.

Uses Playwright browser session (needed for cookies/session on magna.isa.gov.il).
Downloads to data/attachments/{report_reference}/{filename}.
"""

import logging
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from db import Database

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

MAGNA_URL = "https://www.magna.isa.gov.il/"
ATTACHMENTS_DIR = Path(__file__).resolve().parent.parent / "data" / "attachments"
TIMEOUT_MS = 60_000
DELAY_BETWEEN = 0.5  # seconds between downloads
MAX_RETRIES = 1


def download_all(headless: bool = False):
    """Download all pending attachments."""
    with Database() as db:
        pending = db.get_pending_attachments()
        total = db.count_attachments()

        if not pending:
            log.info(f"All {total} attachments already downloaded.")
            return

        log.info(f"Downloading {len(pending)}/{total} pending attachments...")

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                slow_mo=50,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                viewport={"width": 1280, "height": 900},
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

            # Load the SPA to establish session/cookies
            log.info("Loading MAGNA SPA to establish session...")
            page.goto(MAGNA_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            page.wait_for_load_state("networkidle", timeout=15000)
            time.sleep(2)
            log.info(f"Session established: {page.title()}")

            done_before = total - len(pending)
            for i, att in enumerate(pending, 1):
                att_id = att["id"]
                ref = att["report_reference"]
                filename = att["filename"] or f"attachment_{att_id}"
                url = att["url"]

                # Build local path
                dest_dir = ATTACHMENTS_DIR / ref
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest_path = dest_dir / filename
                rel_path = f"data/attachments/{ref}/{filename}"

                # Skip if file already on disk (extra safety)
                if dest_path.exists() and dest_path.stat().st_size > 0:
                    db.mark_downloaded(att_id, rel_path)
                    log.info(f"  [{done_before + i}/{total}] Already on disk: {ref}/{filename}")
                    continue

                # Download with retry
                success = False
                for attempt in range(1 + MAX_RETRIES):
                    try:
                        resp = page.request.get(url, timeout=TIMEOUT_MS)
                        if resp.ok:
                            dest_path.write_bytes(resp.body())
                            db.mark_downloaded(att_id, rel_path)
                            log.info(
                                f"  [{done_before + i}/{total}] Downloaded: {ref}/{filename} "
                                f"({len(resp.body()) // 1024}KB)"
                            )
                            success = True
                            break
                        else:
                            log.warning(
                                f"  [{done_before + i}/{total}] HTTP {resp.status} for {ref}/{filename}"
                                f"{' — retrying...' if attempt < MAX_RETRIES else ' — marking failed'}"
                            )
                    except Exception as e:
                        log.warning(
                            f"  [{done_before + i}/{total}] Error downloading {ref}/{filename}: {e}"
                            f"{' — retrying...' if attempt < MAX_RETRIES else ' — marking failed'}"
                        )

                    if attempt < MAX_RETRIES:
                        time.sleep(1)

                if not success:
                    db.mark_failed(att_id)

                time.sleep(DELAY_BETWEEN)

            browser.close()

    # Print summary
    with Database() as db:
        stats = db.download_stats()
    log.info(f"Download complete. Stats: {stats}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Download MAGNA attachments")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    args = parser.parse_args()

    download_all(headless=args.headless)


if __name__ == "__main__":
    main()
