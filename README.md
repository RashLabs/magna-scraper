# MAGNA Scraper - NewMed Energy

Scrapes MAGNA (ISA) reports for NewMed Energy (ניו-מד אנרג'י - שותפות מוגבלת) and stores metadata in SQLite. Includes a Streamlit viewer.

## Setup

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/playwright install chromium
```

## Usage

### Scrape reports

```bash
# Headed mode (default, requires X display)
.venv/bin/python -m src.scraper

# Headless mode
.venv/bin/python -m src.scraper --headless

# Custom cutoff date
.venv/bin/python -m src.scraper --since 2024-06-01
```

### View reports

```bash
.venv/bin/streamlit run src/app.py
```

## Architecture

- `src/scraper.py` - Playwright session + direct API calls to POST /api/results
- `src/db.py` - SQLite database (data/magna.db)
- `src/app.py` - Streamlit viewer

## Entity Mapping Update Step

Use the standalone resolver to match each company in `data/ta125_magna.json` to the canonical MAGNA reporting entity from `https://www.magna.isa.gov.il/assets/data/init.json`.

```powershell
# Dry run: no file updates, writes review report only
python tools/update_magna_entities.py

# Apply high-confidence updates to data/ta125_magna.json
python tools/update_magna_entities.py --write
```

Outputs:
- `tmp/magna_entity_lookup_report.json` - per-company scores, top candidates, and review items.
- `data/magna_entity_aliases.json` - optional manual aliases for trade names/edge cases.

## API

The MAGNA SPA at `https://www.magna.isa.gov.il/` uses a POST API at `/api/results`. Entity filtering is done via `EntityIds` parameter (NewMed = 228). Pagination uses visual page numbers (step by 3, since RecordsPerPage=30 but VisualRecordsPerPage=10).
