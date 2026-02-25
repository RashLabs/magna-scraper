"""Stage 1: Scrape MAGNA report metadata + form HTML.

Uses Playwright to establish a browser session, then makes API calls
to POST /api/results with entity filtering.
"""

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, Page

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db_v2 import Database
from config import (
    MAGNA_URL, API_RESULTS_URL, DEFAULT_SINCE,
    SLOW_MO, TIMEOUT_MS, VIEWPORT,
    DELAY_BETWEEN_PAGES, DELAY_BETWEEN_COMPANIES,
    COMPANY_LIST_PATH,
)

log = logging.getLogger(__name__)


def _to_magna_date(iso_date: str) -> str:
    parts = iso_date.split("-")
    return f"{parts[2]}/{parts[1]}/{parts[0]}"


def _build_request_body(entity_id: str, from_date: str, to_date: str, page: int = 0) -> dict:
    return {
        "params": {
            "ResultType": 0,
            "MainEntityTypes": "",
            "EntityIds": entity_id,
            "RegisterNumber": None,
            "ExchangeIds": None,
            "ReportTypes": "",
            "References": None,
            "Branches": "",
            "FromDate": from_date,
            "ToDate": to_date,
            "ReportTypesToExclude": "",
            "DateType": "0",
            "DataSource": "0",
            "Page": page,
            "Sort": "0",
            "Language": 1,
        }
    }


def _fetch_results(pw_page: Page, body: dict) -> dict | None:
    script = """
    async ([url, body]) => {
        const resp = await fetch(url, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body),
        });
        return await resp.json();
    }
    """
    try:
        return pw_page.evaluate(script, [API_RESULTS_URL, body])
    except Exception as e:
        log.error(f"API fetch failed: {e}")
        return None


def _fetch_form_html(pw_page: Page, report_url: str) -> str | None:
    """Fetch the form HTML page for a report."""
    if not report_url:
        return None
    try:
        # report_url is typically a relative path or full URL
        full_url = report_url
        if not report_url.startswith("http"):
            full_url = f"https://www.magna.isa.gov.il{report_url}"

        # Use page.request.get to fetch raw HTML
        resp = pw_page.request.get(full_url, timeout=30000)
        if resp.ok:
            # The response body is the HTML — try to decode
            body = resp.body()
            # Try UTF-8 first, fallback to windows-1255
            try:
                return body.decode("utf-8")
            except UnicodeDecodeError:
                return body.decode("windows-1255", errors="replace")
        else:
            log.warning(f"  Form HTML fetch HTTP {resp.status}: {report_url}")
            return None
    except Exception as e:
        log.warning(f"  Form HTML fetch error: {e}")
        return None


def _launch_browser(pw, headless: bool):
    browser = pw.chromium.launch(
        headless=headless,
        slow_mo=SLOW_MO,
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
    pw_page = context.new_page()
    pw_page.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    log.info("Loading MAGNA SPA...")
    pw_page.goto(MAGNA_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
    pw_page.wait_for_load_state("networkidle", timeout=15000)
    time.sleep(2)
    log.info(f"Page loaded: {pw_page.title()}")
    return browser, pw_page


def _parse_report(item: dict) -> dict:
    report_date = item.get("ReportDate", "")
    if report_date and "/" in report_date:
        parts = report_date.split("/")
        if len(parts) == 3:
            report_date = f"{parts[2]}-{parts[1]}-{parts[0]}"

    # Extract form type from FormName — e.g. "ת053 - דיווח מיידי" → "ת053"
    form_name = item.get("FormName", "")
    form_type = ""
    if form_name:
        # Form type is typically the first word (e.g. "ת053")
        parts = form_name.split(" - ", 1)
        if parts:
            form_type = parts[0].strip()

    return {
        "reference_number": str(item.get("ReferenceNumber", "")),
        "report_date": report_date,
        "form_type": form_type,
        "form_name": form_name,
        "subject": item.get("Subject", ""),
        "report_url": item.get("ReportName", ""),
    }


def _parse_attachments(item: dict) -> list[dict]:
    attachments = []
    for att in (item.get("Attachments") or []):
        url = att.get("FileUrl", "")
        filename = att.get("FileName", "")
        if url:
            attachments.append({"filename": filename, "url": url})
    return attachments


def _process_reports(db: Database, pw_page: Page, reports: list[dict], stats: dict,
                     company_id: str | None = None, company_name: str | None = None,
                     fetch_html: bool = True, cancel_check=None):
    for item in reports:
        if cancel_check and cancel_check():
            return

        parsed = _parse_report(item)
        ref = parsed["reference_number"]
        if not ref:
            continue

        # Upsert report
        report_id = db.upsert_report(
            reference_number=ref,
            report_date=parsed["report_date"],
            company_id=company_id,
            company_name=company_name,
            form_type=parsed["form_type"],
            form_name=parsed["form_name"],
            subject=parsed["subject"],
            report_url=parsed["report_url"],
            scraped_at=datetime.now().isoformat(),
        )

        # Upsert attachments
        att_count = 0
        for att in _parse_attachments(item):
            if db.upsert_attachment(report_id, ref, att["filename"], att["url"]):
                att_count += 1
        db.update_attachment_count(report_id)

        stats["reports"] += 1
        stats["attachments"] += att_count

        # Fetch form HTML if not already present
        if fetch_html:
            existing = db.get_report(report_id)
            if existing and not existing.get("form_html") and parsed["report_url"]:
                html = _fetch_form_html(pw_page, parsed["report_url"])
                if html:
                    db.set_form_html(report_id, html)
                    stats["html_fetched"] += 1
                time.sleep(0.3)  # Be gentle


def _scrape_entity(pw_page: Page, db: Database, entity_id: str,
                   from_date: str, to_date: str,
                   company_id: str | None = None,
                   company_name: str | None = None,
                   fetch_html: bool = True,
                   cancel_check=None) -> dict:
    stats = {"reports": 0, "attachments": 0, "html_fetched": 0, "pages": 0}

    body = _build_request_body(entity_id, from_date, to_date, page=0)
    response = _fetch_results(pw_page, body)

    if not response or "Result" not in response:
        log.error(f"Bad API response for entity {entity_id}")
        return stats

    result = response["Result"]
    total_records = int(result.get("TotalRecords", 0))
    records_per_page = int(result.get("RecordsPerPage", 30))
    visual_per_page = int(result.get("VisualRecordsPerPage", 10))
    reports = result.get("Report", [])
    page_step = records_per_page // visual_per_page if visual_per_page else 3

    log.info(f"  Total records: {total_records}, first batch: {len(reports)}")

    if total_records == 0:
        return stats

    total_visual_pages = (total_records + visual_per_page - 1) // visual_per_page

    _process_reports(db, pw_page, reports, stats,
                     company_id=company_id, company_name=company_name,
                     fetch_html=fetch_html, cancel_check=cancel_check)
    stats["pages"] = 1

    for page_num in range(page_step, total_visual_pages, page_step):
        if cancel_check and cancel_check():
            break

        time.sleep(DELAY_BETWEEN_PAGES)
        body = _build_request_body(entity_id, from_date, to_date, page=page_num)
        response = _fetch_results(pw_page, body)

        if not response or "Result" not in response:
            log.warning(f"  Failed on page {page_num}, stopping")
            break

        page_reports = response["Result"].get("Report", [])
        if not page_reports:
            break

        _process_reports(db, pw_page, page_reports, stats,
                         company_id=company_id, company_name=company_name,
                         fetch_html=fetch_html, cancel_check=cancel_check)
        stats["pages"] += 1

    return stats


def _load_companies(company_list: str = "", company_ids: list[str] | None = None) -> list[dict]:
    """Load company list, optionally filtered by magna_ids."""
    if company_list:
        path = Path(company_list)
    elif COMPANY_LIST_PATH.exists():
        path = COMPANY_LIST_PATH
    else:
        log.error(f"No company list found at {COMPANY_LIST_PATH}")
        return []

    companies = json.loads(path.read_text(encoding="utf-8"))

    if company_ids:
        id_set = set(company_ids)
        companies = [c for c in companies if str(c["magna_id"]) in id_set]
        log.info(f"Filtered to {len(companies)} companies from {len(id_set)} requested IDs")

    return companies


def run(since: str = DEFAULT_SINCE, headless: bool = True,
        company_list: str = "", company_ids: list[str] | None = None,
        cancel_check=None, progress_cb=None):
    """Main scrape entry point.

    Args:
        company_list: Path to a JSON file with company list.
        company_ids: List of magna_id strings to scrape (filters the company list).
                     If empty/None, scrapes all companies in the list.
    """
    from_date = _to_magna_date(since)
    to_date = _to_magna_date(datetime.now().strftime("%Y-%m-%d"))

    companies = _load_companies(company_list, company_ids)
    if not companies:
        log.error("No companies to scrape.")
        return

    total = len(companies)
    log.info(f"Scraping {total} companies (since={since})")

    db = Database()

    with sync_playwright() as p:
        browser, pw_page = _launch_browser(p, headless)

        for i, company in enumerate(companies, 1):
            if cancel_check and cancel_check():
                break

            entity_id = str(company["magna_id"])
            name = company.get("name", "").strip() or company.get("magna_name", "")

            log.info(f"[{i}/{total}] {name} (entity {entity_id})...")

            try:
                stats = _scrape_entity(
                    pw_page, db, entity_id, from_date, to_date,
                    company_id=entity_id, company_name=name,
                    cancel_check=cancel_check,
                )
                log.info(f"  => {stats['reports']} reports, {stats['attachments']} att, {stats['html_fetched']} html")
            except Exception as e:
                log.error(f"  ERROR: {e}")

            if progress_cb:
                progress_cb(i, total)

            if i < total:
                time.sleep(DELAY_BETWEEN_COMPANIES)

        browser.close()

    db.close()


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(sys.stdout)])
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", default=DEFAULT_SINCE)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--company-list", metavar="JSON")
    parser.add_argument("--company-ids", nargs="+", metavar="ID", help="Magna entity IDs to scrape")
    args = parser.parse_args()
    run(since=args.since, headless=args.headless,
        company_list=args.company_list or "", company_ids=args.company_ids)
