"""MAGNA (ISA) report scraper — supports single entity or bulk company list.

Uses Playwright to establish a browser session, then makes direct API calls
to POST /api/results with entity filtering.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, Page

from db import Database

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

MAGNA_URL = "https://www.magna.isa.gov.il/"
API_RESULTS_URL = "https://www.magna.isa.gov.il/api/results"
NEWMED_ENTITY_ID = "228"
DEFAULT_SINCE = "2024-01-01"

# Browser settings
SLOW_MO = 100
TIMEOUT_MS = 60_000
VIEWPORT = {"width": 1280, "height": 900}

# Rate limiting
DELAY_BETWEEN_PAGES = 1    # seconds between API page requests
DELAY_BETWEEN_COMPANIES = 2  # seconds between companies


def build_request_body(entity_id: str, from_date: str, to_date: str, page: int = 0) -> dict:
    """Build the POST body for /api/results."""
    # from_date/to_date in DD/MM/YYYY format
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


def to_magna_date(iso_date: str) -> str:
    """Convert YYYY-MM-DD to DD/MM/YYYY."""
    parts = iso_date.split("-")
    return f"{parts[2]}/{parts[1]}/{parts[0]}"


def parse_report(item: dict) -> dict:
    """Parse a single report item from the API response."""
    # Normalize date from DD/MM/YYYY to YYYY-MM-DD
    report_date = item.get("ReportDate", "")
    if report_date and "/" in report_date:
        parts = report_date.split("/")
        if len(parts) == 3:
            report_date = f"{parts[2]}-{parts[1]}-{parts[0]}"

    return {
        "reference_number": str(item.get("ReferenceNumber", "")),
        "report_date": report_date,
        "report_time": item.get("ReportTime", ""),
        "reporter_name": item.get("ReporterName", ""),
        "form_name": item.get("FormName", ""),
        "report_name": item.get("Subject", ""),
        "report_url": item.get("ReportName", ""),  # ReportName is actually the URL
        "subject": item.get("Subject", ""),
    }


def parse_attachments(item: dict) -> list[dict]:
    """Extract file attachments from a report item."""
    attachments = []
    for att in (item.get("Attachments") or []):
        url = att.get("FileUrl", "")
        filename = att.get("FileName", "")
        if url:
            attachments.append({"filename": filename, "url": url})
    return attachments


def fetch_results(page: Page, body: dict) -> dict | None:
    """Fetch results via browser's fetch (inherits session cookies)."""
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
        return page.evaluate(script, [API_RESULTS_URL, body])
    except Exception as e:
        log.error(f"API fetch failed: {e}")
        return None


def _launch_browser(pw, headless: bool):
    """Launch browser and return (browser, pw_page)."""
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

    # Load the SPA to establish session
    log.info("Loading MAGNA SPA...")
    pw_page.goto(MAGNA_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
    pw_page.wait_for_load_state("networkidle", timeout=15000)
    time.sleep(2)
    log.info(f"Page loaded: {pw_page.title()}")
    return browser, pw_page


def _scrape_entity(pw_page: Page, db: Database, entity_id: str,
                   from_date: str, to_date: str,
                   entity_id_for_db: str | None = None,
                   company_name: str | None = None) -> dict:
    """Scrape all reports for a single entity. Returns stats dict."""
    stats = {"reports": 0, "attachments": 0, "skipped": 0, "pages": 0}

    body = build_request_body(entity_id, from_date, to_date, page=0)
    response = fetch_results(pw_page, body)

    if not response or "Result" not in response:
        log.error(f"Bad API response for entity {entity_id}: {response}")
        return stats

    result = response["Result"]
    total_records = int(result.get("TotalRecords", 0))
    records_per_page = int(result.get("RecordsPerPage", 30))
    visual_per_page = int(result.get("VisualRecordsPerPage", 10))
    reports = result.get("Report", [])
    page_step = records_per_page // visual_per_page if visual_per_page else 3

    log.info(
        f"  Total records: {total_records}, first batch: {len(reports)}"
    )

    if total_records == 0:
        log.info(f"  0 reports found")
        return stats

    total_visual_pages = (total_records + visual_per_page - 1) // visual_per_page

    # Process first batch
    _process_reports(db, reports, stats,
                     entity_id=entity_id_for_db, company_name=company_name)
    stats["pages"] = 1

    # Paginate
    for page_num in range(page_step, total_visual_pages, page_step):
        time.sleep(DELAY_BETWEEN_PAGES)

        body = build_request_body(entity_id, from_date, to_date, page=page_num)
        response = fetch_results(pw_page, body)

        if not response or "Result" not in response:
            log.warning(f"  Failed on page {page_num}, stopping pagination")
            break

        page_reports = response["Result"].get("Report", [])
        if not page_reports:
            break

        _process_reports(db, page_reports, stats,
                         entity_id=entity_id_for_db, company_name=company_name)
        stats["pages"] += 1

    return stats


def scrape(since: str = DEFAULT_SINCE, headless: bool = False) -> dict:
    """Scrape NewMed (single entity, original behavior). Returns stats dict."""
    from_date = to_magna_date(since)
    to_date = to_magna_date(datetime.now().strftime("%Y-%m-%d"))

    with Database() as db:
        with sync_playwright() as p:
            browser, pw_page = _launch_browser(p, headless)

            log.info(f"Fetching NewMed reports (entity={NEWMED_ENTITY_ID}, from={from_date}, to={to_date})...")
            stats = _scrape_entity(pw_page, db, NEWMED_ENTITY_ID, from_date, to_date)

            browser.close()

    log.info(
        f"Done. Reports: {stats['reports']}, Attachments: {stats['attachments']}, "
        f"Skipped: {stats['skipped']}, Pages: {stats['pages']}"
    )
    return stats


def scrape_company_list(json_path: str, since: str = DEFAULT_SINCE,
                        headless: bool = False) -> dict:
    """Bulk scrape all companies from a JSON file. Returns aggregate stats."""
    companies = json.loads(Path(json_path).read_text(encoding="utf-8"))
    total = len(companies)
    log.info(f"Loaded {total} companies from {json_path}")

    from_date = to_magna_date(since)
    to_date = to_magna_date(datetime.now().strftime("%Y-%m-%d"))

    agg = {"reports": 0, "attachments": 0, "skipped": 0,
           "companies_scraped": 0, "companies_skipped": 0, "companies_zero": 0,
           "companies_failed": 0, "per_company": []}

    with Database() as db:
        with sync_playwright() as p:
            browser, pw_page = _launch_browser(p, headless)

            for i, company in enumerate(companies, 1):
                entity_id = str(company["magna_id"])
                name = company.get("name", "").strip()
                magna_name = company.get("magna_name", "")
                symbol = company.get("symbol", "").strip()

                log.info(f"Scraping {i}/{total}: {name} (entity {entity_id})...")

                # Skip if already scraped
                if db.has_reports_for_entity(entity_id):
                    log.info(f"  Already scraped, skipping")
                    agg["companies_skipped"] += 1
                    continue

                try:
                    stats = _scrape_entity(
                        pw_page, db, entity_id, from_date, to_date,
                        entity_id_for_db=entity_id,
                        company_name=name or magna_name,
                    )
                except Exception as e:
                    log.error(f"  ERROR scraping {name}: {e}")
                    agg["companies_failed"] += 1
                    time.sleep(DELAY_BETWEEN_COMPANIES)
                    continue

                agg["reports"] += stats["reports"]
                agg["attachments"] += stats["attachments"]
                agg["skipped"] += stats["skipped"]

                if stats["reports"] == 0 and stats["skipped"] == 0:
                    agg["companies_zero"] += 1
                else:
                    agg["companies_scraped"] += 1

                agg["per_company"].append({
                    "entity_id": entity_id,
                    "name": name,
                    "symbol": symbol,
                    "reports": stats["reports"],
                    "attachments": stats["attachments"],
                })

                log.info(
                    f"  => {stats['reports']} reports, {stats['attachments']} attachments"
                )

                # Be gentle
                if i < total:
                    time.sleep(DELAY_BETWEEN_COMPANIES)

            browser.close()

    log.info("=" * 60)
    log.info(f"BULK SCRAPE COMPLETE")
    log.info(f"  Companies scraped: {agg['companies_scraped']}")
    log.info(f"  Companies skipped (already done): {agg['companies_skipped']}")
    log.info(f"  Companies with 0 reports: {agg['companies_zero']}")
    log.info(f"  Companies failed: {agg['companies_failed']}")
    log.info(f"  Total new reports: {agg['reports']}")
    log.info(f"  Total new attachments: {agg['attachments']}")
    log.info(f"  Total skipped (dupes): {agg['skipped']}")
    log.info("=" * 60)

    return agg


def _process_reports(db: Database, reports: list[dict], stats: dict,
                     entity_id: str | None = None,
                     company_name: str | None = None):
    """Process and store a list of report items."""
    for item in reports:
        parsed = parse_report(item)
        ref = parsed["reference_number"]

        if not ref:
            log.warning(f"Report missing reference number, skipping")
            continue

        # Add entity tracking fields
        if entity_id:
            parsed["entity_id"] = entity_id
        if company_name:
            parsed["company_name"] = company_name

        existed = db.report_exists(ref)
        if existed:
            stats["skipped"] += 1
        elif db.insert_report(**parsed):
            stats["reports"] += 1
            log.info(f"  + {ref}: {parsed.get('report_name', '')[:80]}")
        else:
            stats["skipped"] += 1

        # Always process attachments (even for existing reports — idempotent)
        for att in parse_attachments(item):
            if db.insert_attachment(ref, att["filename"], att["url"]):
                stats["attachments"] += 1


def main():
    parser = argparse.ArgumentParser(description="MAGNA report scraper")
    parser.add_argument("--since", default=DEFAULT_SINCE, help="Cutoff date YYYY-MM-DD (default: 2024-01-01)")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--company-list", metavar="JSON_FILE",
                        help="Path to JSON file with company list for bulk scraping")
    args = parser.parse_args()

    if args.company_list:
        log.info(f"Starting MAGNA bulk scraper (since={args.since}, companies={args.company_list})")
        stats = scrape_company_list(args.company_list, since=args.since, headless=args.headless)
    else:
        log.info(f"Starting MAGNA scraper (since={args.since}, headless={args.headless})")
        stats = scrape(since=args.since, headless=args.headless)

    log.info(f"Final stats: {stats}")


if __name__ == "__main__":
    main()
