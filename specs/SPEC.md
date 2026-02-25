# Magna Scraper v2 — Spec

## Overview

Offline pipeline that scrapes Israeli MAGNA (ISA) corporate filings, extracts structured data and text, embeds into Qdrant. Exposes a REST API consumed by:
1. **Moses Admin Dashboard** — new "Magna" tab for pipeline controls + data browsing
2. **Moses retriever** — semantic/hybrid search over magna collection at runtime

## Architecture

```
Moses (existing)                        Magna Scraper (new service)
┌──────────────────────┐               ┌──────────────────┐
│ Moses Frontend       │               │ Magna FastAPI     │
│ (React/CRA/Tailwind) │──── HTTP ────▶│ :8400             │
│                      │               │                   │
│ Admin Dashboard      │               │  Pipeline Workers │
│  ├── Users tab       │               │  ├── scraper      │──▶ MAGNA ISA.gov
│  ├── Threads tab     │               │  ├── parser       │
│  ├── ...             │               │  ├── downloader   │
│  └── Magna tab (NEW) │               │  ├── extractor    │
│                      │               │  └── indexer      │
│ Moses Backend        │               └────────┬──┬──────┘
│  ├── retriever tool ─┼── Qdrant query ──┐     │  │
│  └── pg-vector      │               │     │  │
└──────────────────────┘               │  ┌──┘  └──┐
                                       │  ▼        ▼
                                   ┌───────┐  ┌─────────┐
                                   │Qdrant │  │ SQLite   │
                                   │magna  │  │ pipeline │
                                   │collect│  │ state    │
                                   └───────┘  └─────────┘
```

### Components

- **SQLite (WAL mode)** — pipeline state only. Single-writer, WAL mode for concurrent reads during batch jobs.
- **Qdrant** — all embedded content with rich metadata payloads. Moses retriever queries directly.
- **Magna FastAPI** — separate service on port 8400. Hosts pipeline workers + admin API.
- **Moses Admin "Magna" tab** — new tab in existing Moses admin dashboard.

### Auth
v1: Magna API is internal-only (localhost / LAN). No authentication required.
Later: Moses backend proxies magna API calls, adding its own JWT validation. Frontend never calls magna directly in production.

### Why separate backend?
- Magna has heavy deps (Playwright, meowpdf) that don't belong in Moses
- Long-running batch jobs (15K+ reports) need process isolation
- Independent dev/deploy cycle
- Moses pgvector = runtime user docs; Qdrant = offline batch-processed magna corpus

## Conventions

- **Dates**: ISO-8601 format, always. SQLite TEXT columns store `YYYY-MM-DD` (dates) or `YYYY-MM-DDTHH:MM:SS` (timestamps). Qdrant payload dates use same format (string, range-filterable). All dates are Israel local time (no timezone suffix) since MAGNA source data has no timezone.
- **Encoding**: UTF-8 everywhere. Form HTML is fetched as Windows-1255 and transcoded to UTF-8 before storage.
- **Empty values**: `NULL` in SQLite, omitted from Qdrant payload. The MAGNA sentinel `_________` (9 underscores) is treated as empty/NULL during parsing.

## Data Model

### SQLite — Pipeline State

```sql
CREATE TABLE reports (
    id                  INTEGER PRIMARY KEY,
    reference_number    TEXT UNIQUE NOT NULL,
    report_date         TEXT,           -- YYYY-MM-DD
    company_id          TEXT,           -- Ession (entity number)
    company_name        TEXT,           -- Ession2
    form_type           TEXT,           -- e.g. "ת053", "ת121"
    form_name           TEXT,           -- e.g. "דיווח מיידי"
    subject             TEXT,           -- ReportSubject
    report_url          TEXT,
    attachment_count    INTEGER DEFAULT 0,

    -- form content
    form_html           TEXT,           -- raw HTML of the MAGNA form page (UTF-8)
    form_fields         TEXT,           -- JSON: parsed FieldAlias output (see Form Parsing)
    form_category       TEXT,           -- container|structured|registry|mixed

    -- pipeline status
    scraped_at          TEXT,           -- when metadata row was created
    html_fetched_at     TEXT,           -- when form_html was fetched
    parsed_at           TEXT,           -- when form_fields were extracted from HTML
    indexed_at          TEXT,           -- when embedded into Qdrant

    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE attachments (
    id                  INTEGER PRIMARY KEY,
    report_id           INTEGER NOT NULL REFERENCES reports(id),
    reference_number    TEXT NOT NULL,   -- parent report ref
    filename            TEXT,
    url                 TEXT,
    file_size_kb        INTEGER,

    -- pipeline status
    download_status     TEXT DEFAULT 'pending',  -- pending|downloaded|failed|skipped
    local_path          TEXT,
    downloaded_at       TEXT,
    extracted_at        TEXT,           -- when text extracted from PDF
    indexed_at          TEXT,           -- when embedded into Qdrant

    created_at          TEXT DEFAULT (datetime('now')),

    UNIQUE(reference_number, filename)  -- prevent duplicate attachments on re-scrape
);

-- Text extracted from attachments (intermediate staging before Qdrant)
CREATE TABLE doc_texts (
    id                  INTEGER PRIMARY KEY,
    attachment_id       INTEGER UNIQUE REFERENCES attachments(id),
    text_content        TEXT,           -- JSON array of pages [{page, text}]
    char_count          INTEGER DEFAULT 0,
    extracted_at        TEXT DEFAULT (datetime('now'))
);
```

Notes:
- `reports.reference_number` is UNIQUE — scraper upserts on re-scrape
- `attachments(reference_number, filename)` is UNIQUE — prevents duplicates on re-scrape
- Pipeline status tracked per-record via timestamp columns (NULL = not done yet)
- SQLite opened with `PRAGMA journal_mode=WAL` for concurrent read access during batch writes

### Qdrant — Vector Store

**Collection: `magna`**

Two point types coexist in one collection, distinguished by `source_type` payload field.

**Indexing protocol (idempotent):**
Before upserting points for a report, delete all existing points for that reference_number:
```python
client.delete(
    collection_name="magna",
    points_selector=FilterSelector(filter=Filter(must=[
        FieldCondition(key="reference_number", match=MatchValue(value=ref))
    ]))
)
# then upsert new points
```
This ensures re-indexing is safe — no stale vectors from previous chunk counts.

#### 1. Form Content Points
```json
{
    "id": "<uuid>",
    "vector": [/* 1536 dims, gemini-embedding-001 */],
    "payload": {
        "source_type": "form",
        "reference_number": "2024-01-012345",
        "report_date": "2024-01-15",
        "company_id": "520",
        "company_name": "טבע תעשיות פרמצבטיות",
        "form_type": "ת053",
        "form_name": "דיווח מיידי",
        "subject": "הודעה על חלוקת דיבידנד",
        "chunk_index": 0,
        "chunk_text": "the actual text chunk",
        "field_name": "TextHofshi"
    }
}
```

#### 2. Attachment Content Points
```json
{
    "id": "<uuid>",
    "vector": [/* 1536 dims */],
    "payload": {
        "source_type": "attachment",
        "reference_number": "2024-01-012345",
        "report_date": "2024-01-15",
        "company_id": "520",
        "company_name": "טבע תעשיות פרמצבטיות",
        "form_type": "ת053",
        "filename": "annual_report_2023.pdf",
        "page_number": 5,
        "chunk_index": 0,
        "chunk_text": "the actual text chunk"
    }
}
```

**Point IDs**: UUID v4 (generated at indexing time). Since we delete-by-reference before upsert, stable IDs are not needed.

**Qdrant payload indexes:**
- `source_type` — keyword
- `reference_number` — keyword (used for delete-before-upsert)
- `company_id` — keyword
- `form_type` — keyword
- `report_date` — keyword (ISO string, supports range via prefix match)
- `company_name` — keyword

### Moses Integration

Moses retriever tool queries Qdrant `magna` collection directly:
```python
from qdrant_client import QdrantClient

client = QdrantClient(url="http://localhost:6333")
results = client.search(
    collection_name="magna",
    query_vector=embedding,
    query_filter=Filter(must=[
        FieldCondition(key="company_name", match=MatchValue(value="טבע")),
        FieldCondition(key="report_date", range=Range(gte="2024-01-01")),
    ]),
    limit=10
)
```

No coupling between magna-scraper and Moses beyond shared Qdrant access.

## Pipeline

### Job Model

Each pipeline stage runs as a **single background task** within the FastAPI process (asyncio task wrapping a sync worker in a thread). At most one job per stage runs at a time.

**State machine per stage:**
```
idle ──▶ running ──▶ done
            │          │
            ▼          │
          error        │
            │          │
            └──────────┴──▶ idle  (on next start or manual reset)
```

**Job state** (held in memory, not persisted — resets to idle on server restart):
```python
@dataclass
class JobState:
    status: Literal["idle", "running", "done", "error"] = "idle"
    progress: str = ""          # e.g. "1523/15000"
    processed: int = 0
    total: int = 0
    error: str | None = None
    log_tail: list[str] = []    # last 100 log lines (ring buffer)
    started_at: str | None = None
    cancel_requested: bool = False
```

**Concurrency rules:**
- Only one instance of each stage can run at a time (start returns 409 if already running)
- Different stages CAN run concurrently (e.g. download + extract in parallel)
- Workers check `cancel_requested` between iterations and exit gracefully

**No persistent job IDs.** This is a single-user admin tool, not a job scheduler. The current run is the only run.

### Stage 1: Scrape
- Input: company list (JSON) or single entity + date range
- Fetches report listing pages from MAGNA
- Upserts report metadata + attachment URLs into SQLite (ON CONFLICT reference_number DO UPDATE)
- **Also fetches each report's form HTML page** (new in v2)
- Sets `scraped_at`, `html_fetched_at`

### Stage 2: Parse Forms
- Input: reports where `form_html IS NOT NULL AND parsed_at IS NULL`
- Extracts FieldAlias elements from HTML → structured JSON (see Form Parsing below)
- Classifies `form_category` based on form_type lookup
- Sets `form_fields`, `form_category`, `parsed_at`

### Stage 3: Download Attachments
- Input: attachments where `download_status = 'pending'`
- Downloads PDFs via Playwright browser session
- Sets `download_status`, `local_path`, `downloaded_at`

### Stage 4: Extract Text
- Input: attachments where `download_status = 'downloaded' AND extracted_at IS NULL`
- Runs meowpdf on each PDF → JSON pages
- Stores in `doc_texts` table
- Sets `extracted_at` on attachment

### Stage 5: Index to Qdrant
- Input: reports where `parsed_at IS NOT NULL AND indexed_at IS NULL`
- For each report:
  1. Delete all existing Qdrant points with this `reference_number`
  2. Chunk narrative form fields (from `form_fields`) → embed → collect points
  3. For each attachment that has `extracted_at IS NOT NULL`: chunk text → embed → collect points
  4. Batch upsert all points to Qdrant
  5. Set `indexed_at` on the report and on each indexed attachment
- **Attachments not yet extracted are skipped** (not blocked on). Report can be re-indexed later when more attachments are ready — the delete-before-upsert protocol handles this cleanly.

## Form Parsing

### Input
Raw HTML of a MAGNA form page (UTF-8 transcoded from Windows-1255).

### Output: `form_fields` JSON

```json
{
    "fields": {
        "ReportSubject": "הודעה על חלוקת דיבידנד",
        "TextHofshi": "החברה מודיעה בזאת...",
        "TaarichDivuach": "2024-01-15",
        "Ession": "520",
        "Ession2": "טבע תעשיות פרמצבטיות"
    },
    "tables": {
        "signatories": [
            {"Signer": "ישראל ישראלי", "SignerTitle": "מנכ\"ל", "SignerDate": "2024-01-15"},
            {"Signer": "שרה כהן", "SignerTitle": "יו\"ר", "SignerDate": "2024-01-15"}
        ],
        "holdings": [
            {"ShemBaalInyan": "קרן השקעות", "AchuzHachzaka": "5.2", "SugMashraHolder": "תאגיד"}
        ]
    },
    "std_fields": {
        "IssuerName": "Teva Pharmaceutical"
    }
}
```

### Parsing rules

1. **Find all elements** with `FieldAlias` attribute (case-insensitive search on `fieldalias`).
2. **Extract value**: `.value` for `<input>`/`<select>`, `.textContent` for `<span>`/`<textarea>`.
3. **Discard empties**: skip if value is `NULL`, empty string, or `_________` (9 underscores).
4. **Detect Row pattern**: if alias matches `Row{N}_{FieldName}`, it's a table row.
   - Group by table name (inferred from surrounding `XMLType="TABLE"` container, or from field name prefix).
   - Each `Row{N}` becomes one dict in the table array.
   - Row dicts use the `{FieldName}` part as key (strip `Row{N}_` prefix).
5. **Non-row fields** go into `fields` dict. If the same alias appears multiple times (rare), keep the last non-empty value.
6. **STD-FieldAlias**: elements with `STD-FieldAlias` attribute go into `std_fields` dict (standardized English names, found in some forms like ת076).

### Form Category Classification

Based on `form_type` lookup:
- `container` — forms whose value is mainly attachments + free text (ת053, ת121, etc.)
- `structured` — forms with rich tabular data (ת087, ת076, ת081)
- `registry` — massive repeating-row forms (ת077)
- `mixed` — forms with both narrative and structured sections (ת460)

Default for unknown form types: `container`.

## Form Type Config

Per-form-type configuration defining field roles (used by indexer in stage 5):

```python
FORM_CONFIGS = {
    "ת053": {
        "category": "container",
        "narrative_fields": ["TextHofshi", "ReportSubject"],
        "metadata_fields": ["TaarichDivuach", "SugDivuach"],
        "structured_fields": [],
        "skip_fields": ["Shem", "Mispar", "TaarichIdkun"],
    },
    "ת081": {
        "category": "structured",
        "narrative_fields": ["ReportSubject"],
        "metadata_fields": ["TaarichKeta", "TaarichTashlum", "SachDividend"],
        "structured_fields": ["DividendTerms_*", "TaxBreakdown_*"],
        "skip_fields": ["Shem", "Mispar"],
    },
    "ת077": {
        "category": "registry",
        "narrative_fields": [],
        "metadata_fields": ["TaarichDivuach"],
        "structured_fields": ["Row*_ShemBaalInyan", "Row*_AchuzHachzaka"],
        "skip_fields": [],
    },
    "_default": {
        "category": "container",
        "narrative_fields": ["TextHofshi", "ReportSubject"],
        "metadata_fields": [],
        "structured_fields": [],
        "skip_fields": [],
    }
}
```

v1: Use `_default` config for all form types. Per-type configs added iteratively.

## API Endpoints (FastAPI)

Base URL: `http://localhost:8400/api`

### Pipeline Control

**POST /pipeline/{stage}/start**
- `stage` enum: `scrape | parse | download | extract | index`
- Body (optional, stage-specific):
  ```json
  {
      "since": "2024-01-01",
      "headless": true,
      "company_list": "ta125_magna.json"
  }
  ```
- Returns: `{"status": "started"}` or `409 {"error": "already running"}`

**POST /pipeline/{stage}/stop**
- Sets `cancel_requested = True` on the running job
- Returns: `{"status": "stopping"}` or `404` if not running

**GET /pipeline/status**
- Returns status for all stages:
  ```json
  {
      "scrape":   {"status": "idle", "progress": "", "processed": 0, "total": 0},
      "parse":    {"status": "running", "progress": "450/2000", "processed": 450, "total": 2000},
      "download": {"status": "done", "progress": "15000/15000", "processed": 15000, "total": 15000},
      "extract":  {"status": "idle"},
      "index":    {"status": "error", "error": "Qdrant connection refused"}
  }
  ```

**GET /pipeline/{stage}/log**
- Returns: `{"lines": ["line1", "line2", ...]}` (last 100 lines)

### Data Browse

**GET /reports**
- Query params: `page` (default 1), `size` (default 50), `form_type`, `company`, `search`, `status` (scraped|parsed|indexed|all)
- Returns:
  ```json
  {
      "items": [
          {
              "id": 1,
              "reference_number": "2024-01-012345",
              "report_date": "2024-01-15",
              "company_name": "טבע",
              "form_type": "ת053",
              "form_name": "דיווח מיידי",
              "subject": "...",
              "attachment_count": 3,
              "form_category": "container",
              "scraped_at": "2024-01-20T10:00:00",
              "parsed_at": "2024-01-20T10:05:00",
              "indexed_at": null
          }
      ],
      "total": 20000,
      "page": 1,
      "size": 50
  }
  ```

**GET /reports/{id}**
- Returns full report including `form_fields` (parsed JSON) and attachments list

**GET /attachments**
- Query params: `page`, `size`, `status` (pending|downloaded|failed|extracted|indexed), `report_id`
- Returns paginated list

**GET /stats**
- Pipeline funnel metrics:
  ```json
  {
      "reports": {"total": 20000, "html_fetched": 18000, "parsed": 15000, "indexed": 10000},
      "attachments": {"total": 15052, "downloaded": 12000, "failed": 50, "extracted": 8000, "indexed": 6000}
  }
  ```

### Search Preview (later)
```
POST /search    — {query, filters} → Qdrant search results
```

## Frontend — Moses Admin "Magna" Tab

New tab added to existing Moses admin dashboard (`frontend/src/components/admin/MagnaTab.tsx`).
Follows existing patterns: `ProjectsMemoryTab.tsx` for task management, `ThreadsTabView.tsx` for data tables.

### Sub-views (within the Magna tab)

1. **Overview** — pipeline funnel metrics, per-stage run/stop controls, live progress
2. **Reports** — paginated table (form_type, company, date range filters), click row → detail modal
3. **Report Detail** (modal) — form fields, attachment list, raw HTML preview
4. **Attachments** — filterable table by status, report_id
5. **Search** (later) — test semantic search against Qdrant

### Integration with Moses
- Magna tab calls magna-scraper FastAPI directly (configurable base URL, default `http://localhost:8400`)
- Uses existing Moses patterns: axios for API calls, Tailwind for styling, Lucide icons
- Auth: v1 none (internal). Later: Moses backend proxies requests.
- No new npm dependencies needed

## Project Structure

```
magna-scraper/                        # this repo
├── src/
│   ├── api/                          # FastAPI app
│   │   ├── main.py                   # app, CORS, lifespan
│   │   ├── routes/
│   │   │   ├── pipeline.py           # pipeline control endpoints
│   │   │   ├── reports.py            # data browse endpoints
│   │   │   └── search.py             # qdrant search endpoint
│   │   └── deps.py                   # shared deps (db, qdrant client)
│   ├── pipeline/
│   │   ├── scraper.py                # stage 1: scrape MAGNA
│   │   ├── parser.py                 # stage 2: parse form HTML → fields
│   │   ├── downloader.py             # stage 3: download attachments
│   │   ├── extractor.py              # stage 4: PDF → text
│   │   ├── indexer.py                # stage 5: embed + upsert to Qdrant
│   │   └── form_configs.py           # per-form-type field mappings
│   ├── db.py                         # SQLite operations
│   ├── models.py                     # pydantic models
│   └── config.py                     # settings (paths, Qdrant URL, API keys)
├── data/                             # SQLite DB + downloaded files
├── SPEC.md
└── pyproject.toml

moses/                                # separate repo (MosesLabs/moses)
├── frontend/src/components/admin/
│   └── MagnaTab.tsx                  # NEW — Magna admin tab
├── frontend/src/services/
│   └── magnaApiService.ts            # NEW — API client for magna-scraper
└── ...existing moses code...
```

## v1 Scope (First Iteration)

Build the minimum to prove the pipeline works end-to-end:

1. **SQLite schema + db.py** — new tables with pipeline status tracking
2. **Form parser** — generic FieldAlias extractor (works for any form type)
3. **FastAPI backend** — pipeline control + data browse endpoints
4. **Moses admin Magna tab** — overview + reports table + report detail modal
5. **Qdrant indexer** — embed form text + attachment text, upsert with payloads
6. **Migrate existing data** — one-time script to port v1 SQLite data to new schema

Defer to later iterations:
- Per-form-type configs (start with `_default` config for all)
- Moses retriever integration (just ensure Qdrant schema is compatible)
- Search preview UI
- Bulk operations / advanced filtering
- Auth proxy through Moses backend
