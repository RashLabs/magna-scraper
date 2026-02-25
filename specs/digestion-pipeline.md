# Magna Digestion Pipeline — Sequence Diagram

## Diagram

[Open in Excalidraw](https://excalidraw.com/#json=8cbQThTJyDhCsJHOp3sBC,WlLLfNBJkcgyFixTX8x9nA)

![Digestion Pipeline Sequence](digestion-pipeline.excalidraw.svg)

## Actors

| Actor | Role |
|-------|------|
| **Admin UI** | Moses admin dashboard (MagnaTab) — triggers pipeline stages, displays progress |
| **FastAPI** | Magna backend service on port 8400 — orchestrates pipeline jobs in background threads |
| **MAGNA ISA** | Israel Securities Authority website — source of corporate filings HTML and attachments |
| **SQLite** | Local database (WAL mode) — stores reports, attachments, doc_texts, pipeline state |
| **Qdrant** | Vector database — stores embedded chunks for semantic search |

## Stage 1: Scrape

Admin UI triggers scrape via `POST /api/pipeline/scrape/start`.

FastAPI launches a Playwright browser session that:
1. Navigates MAGNA ISA and browses report listings (filtered by company list and `--since` date)
2. For each report, fetches the form HTML page
3. Extracts report metadata (reference number, date, company, form type, subject) and attachment URLs
4. Upserts reports and attachments into SQLite (idempotent on `reference_number`)

**Inputs**: company list (JSON), since date, headless flag
**Outputs**: `reports` rows with `form_html`, `attachments` rows with URLs

## Stage 2: Parse

Admin UI triggers parse via `POST /api/pipeline/parse/start`.

FastAPI reads all reports that have `form_html` but no `parsed_at`:
1. Runs regex-based FieldAlias extraction on the HTML
2. Handles quoted/unquoted attributes, `<input>` values, `<select>` selected options, `<span>`/`<textarea>` inner text
3. Groups fields into: regular fields, Row{N}_ tables, repeated-alias tables, STD fields
4. Classifies form type into category (container, structured, registry, mixed)
5. Saves `form_fields` JSON and category back to SQLite, marks `parsed_at`

**Inputs**: `form_html` from SQLite
**Outputs**: `form_fields` JSON (`{fields, tables, std_fields}`), `form_category`, `parsed_at` timestamp

### Parsed Output Schema

```json
{
  "fields": { "FieldAlias": "value", ... },
  "tables": {
    "rows": [{"FieldName": "value", ...}, ...],
    "repeated": [{"Alias": "value", ...}, ...]
  },
  "std_fields": { "STD-FieldAlias": "value", ... }
}
```

## Stage 3: Download

Admin UI triggers download via `POST /api/pipeline/download/start`.

FastAPI queries SQLite for attachments with `download_status = 'pending'`:
1. Establishes a Playwright browser session on MAGNA to get cookies
2. For each pending attachment, fetches the PDF/TXT file via HTTP
3. Saves to disk at `data/attachments/{reference_number}/{filename}`
4. Sanitizes filenames (path traversal protection via `PurePosixPath.name`, null-byte strip, `is_relative_to` guard)
5. Marks attachment as `downloaded` with `local_path` in SQLite
6. Retries up to `MAX_RETRIES` on failure, then marks as `failed`

**Inputs**: pending attachment URLs from SQLite
**Outputs**: PDF/TXT files on disk, `download_status` + `local_path` updated

## Stage 4: Extract

Admin UI triggers extraction via `POST /api/pipeline/extract/start`.

FastAPI queries SQLite for downloaded attachments without `extracted_at`:
1. For PDFs: extracts text using `meowpdf`
2. For TXT files: reads with encoding fallback (utf-8 → windows-1255 → latin-1)
3. Saves extracted text to `doc_texts` table with character count
4. Marks attachment `extracted_at` only on success
5. **Failed extractions remain retryable** — no extracted_at is set, so they'll be picked up on next run

**Inputs**: downloaded files from disk
**Outputs**: `doc_texts` rows, `extracted_at` timestamps on attachments

## Stage 5: Index

Admin UI triggers indexing via `POST /api/pipeline/index/start`.

FastAPI queries SQLite for reports that are parsed but not indexed, OR have newly extracted attachments:
1. For each report, gathers narrative form fields (from `form_fields` JSON) and attachment text (from `doc_texts`)
2. Chunks text at 500 words with 50-word overlap (minimum 10 words per chunk)
3. Embeds chunks via Gemini (`gemini-embedding-001`, 1536 dimensions, batch size 8)
4. **Idempotent upsert**: deletes all existing Qdrant points for this `reference_number`, then upserts new vectors
5. Each Qdrant point payload includes: `reference_number`, `company_name`, `form_type`, `report_date`, `source` (form_field/attachment), `chunk_index`, `text`
6. Only marks `indexed_at` when `report_has_pending_attachments()` returns False (all attachments are downloaded and extracted)

**Inputs**: `form_fields` JSON + `doc_texts` from SQLite
**Outputs**: vectors in Qdrant `magna` collection, `indexed_at` timestamps

## Key Design Decisions

- **Each stage is independently triggerable** — no forced sequential execution
- **Idempotent operations** — safe to re-run any stage
- **Late attachment handling** — if new attachments are downloaded/extracted after initial indexing, Stage 5 re-indexes the report
- **Progress reporting** — each stage calls `progress_cb(done, total)` and logs to a stage-specific handler, polled by the UI every 3 seconds
- **Cancellation** — each stage checks `cancel_check()` between iterations; setting `cancel_requested` flag via `POST /api/pipeline/{stage}/stop` gracefully stops the job
- **Log isolation** — each stage's log handler is attached to its specific logger (`pipeline.scraper`, `pipeline.parser`, etc.), preventing cross-stage contamination
