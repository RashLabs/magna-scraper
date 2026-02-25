"""Run-all orchestrator: scrape → parse → download → extract → index with retry."""

import logging

from db_v2 import Database

log = logging.getLogger("pipeline.orchestrator")

MAX_STAGE_RETRIES = 2

# Each stage: (name, import path, extra kwargs builder, remaining-count query)
STAGE_DEFS = [
    "scrape",
    "parse",
    "download",
    "extract",
    "index",
]


def _count_remaining(db: Database, stage: str) -> int:
    """Count items still pending for a given stage."""
    q = db.conn.execute
    if stage == "scrape":
        # Nothing to count — scrape is driven by the input company list,
        # not by DB state.  Always returns 0 after a run.
        return 0
    elif stage == "parse":
        return q("SELECT COUNT(*) FROM reports WHERE form_html IS NOT NULL AND parsed_at IS NULL").fetchone()[0]
    elif stage == "download":
        return q("SELECT COUNT(*) FROM attachments WHERE download_status = 'pending'").fetchone()[0]
    elif stage == "extract":
        return q("SELECT COUNT(*) FROM attachments WHERE download_status = 'downloaded' AND extracted_at IS NULL").fetchone()[0]
    elif stage == "index":
        return q("""SELECT COUNT(*) FROM reports r
                    WHERE r.parsed_at IS NOT NULL
                      AND (r.indexed_at IS NULL
                           OR EXISTS (
                               SELECT 1 FROM attachments a
                               WHERE a.report_id = r.id
                                 AND a.extracted_at IS NOT NULL
                                 AND a.indexed_at IS NULL
                           ))""").fetchone()[0]
    return 0


def _run_stage(stage: str, cancel_check, progress_cb,
               scrape_kwargs: dict, reprocess: bool = False):
    """Import and run a single pipeline stage."""
    if stage == "scrape":
        from pipeline.scraper import run
        run(cancel_check=cancel_check, progress_cb=progress_cb, **scrape_kwargs)
    elif stage == "parse":
        from pipeline.parser import run
        run(reprocess=reprocess, cancel_check=cancel_check, progress_cb=progress_cb)
    elif stage == "download":
        from pipeline.downloader import run
        run(headless=scrape_kwargs.get("headless", True), reprocess=reprocess,
            cancel_check=cancel_check, progress_cb=progress_cb)
    elif stage == "extract":
        from pipeline.extractor import run
        run(reprocess=reprocess, cancel_check=cancel_check, progress_cb=progress_cb)
    elif stage == "index":
        from pipeline.indexer import run
        run(reprocess=reprocess, cancel_check=cancel_check, progress_cb=progress_cb)


def run(since: str = "2024-01-01", headless: bool = True,
        company_list: str = "", company_ids: list[str] | None = None,
        rescrape: bool = False, reprocess: bool = False,
        cancel_check=None, progress_cb=None):
    """Run all pipeline stages sequentially with retry on remaining items.

    Args match the scraper's run() signature for the scrape-specific params.
    cancel_check / progress_cb are wired by the job framework in deps.py.
    """
    scrape_kwargs = {
        "since": since,
        "headless": headless,
        "company_list": company_list,
        "company_ids": company_ids,
        "rescrape": rescrape,
    }

    total_stages = len(STAGE_DEFS)
    db = Database()

    for stage_idx, stage in enumerate(STAGE_DEFS):
        if cancel_check and cancel_check():
            log.info("Run-all cancelled before stage '%s'", stage)
            return

        stage_label = f"[{stage_idx + 1}/{total_stages}] {stage}"
        log.info("━━━ %s: starting ━━━", stage_label)

        if progress_cb:
            progress_cb(stage_idx, total_stages)

        for attempt in range(1, MAX_STAGE_RETRIES + 1):
            if cancel_check and cancel_check():
                log.info("Run-all cancelled during stage '%s'", stage)
                return

            _run_stage(stage, cancel_check, progress_cb=None,
                       scrape_kwargs=scrape_kwargs, reprocess=reprocess)

            if cancel_check and cancel_check():
                log.info("Run-all cancelled during stage '%s'", stage)
                return

            remaining = _count_remaining(db, stage)
            if remaining == 0:
                log.info("━━━ %s: complete ━━━", stage_label)
                break

            if attempt < MAX_STAGE_RETRIES:
                log.info("%s: %d items remaining, retrying (attempt %d/%d)",
                         stage_label, remaining, attempt + 1, MAX_STAGE_RETRIES)
            else:
                log.warning("%s: finished with %d items still remaining after %d attempts",
                            stage_label, remaining, MAX_STAGE_RETRIES)

    # Final verification
    stats = db.stats()
    log.info("━━━ Run-all complete ━━━")
    log.info("Reports:     %d total, %d parsed, %d indexed",
             stats["reports"]["total"], stats["reports"]["parsed"], stats["reports"]["indexed"])
    log.info("Attachments: %d total, %d downloaded, %d extracted, %d indexed, %d failed",
             stats["attachments"]["total"], stats["attachments"]["downloaded"],
             stats["attachments"]["extracted"], stats["attachments"]["indexed"],
             stats["attachments"]["failed"])

    if progress_cb:
        progress_cb(total_stages, total_stages)
