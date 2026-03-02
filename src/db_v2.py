"""SQLite database layer for MAGNA v2 pipeline."""

import sqlite3
import json
import logging
from pathlib import Path

from config import DB_PATH

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    reference_number    TEXT UNIQUE NOT NULL,
    report_date         TEXT,
    company_id          TEXT,
    company_name        TEXT,
    form_type           TEXT,
    form_name           TEXT,
    subject             TEXT,
    report_url          TEXT,
    attachment_count    INTEGER DEFAULT 0,

    form_html           TEXT,
    form_fields         TEXT,
    form_category       TEXT,

    scraped_at          TEXT,
    html_fetched_at     TEXT,
    parsed_at           TEXT,
    indexed_at          TEXT,

    created_at          TEXT DEFAULT (datetime('now')),
    updated_at          TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS attachments (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id           INTEGER NOT NULL REFERENCES reports(id),
    reference_number    TEXT NOT NULL,
    filename            TEXT,
    url                 TEXT,
    file_size_kb        INTEGER,

    download_status     TEXT DEFAULT 'pending',
    local_path          TEXT,
    downloaded_at       TEXT,
    extracted_at        TEXT,
    indexed_at          TEXT,
    page_count          INTEGER DEFAULT 0,

    created_at          TEXT DEFAULT (datetime('now')),
    UNIQUE(reference_number, filename)
);

CREATE TABLE IF NOT EXISTS doc_texts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    attachment_id       INTEGER UNIQUE REFERENCES attachments(id),
    text_content        TEXT,
    char_count          INTEGER DEFAULT 0,
    extracted_at        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS scrape_watermarks (
    company_id      TEXT PRIMARY KEY,
    scraped_through  TEXT NOT NULL,
    updated_at       TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_reports_date ON reports(report_date);
CREATE INDEX IF NOT EXISTS idx_reports_company ON reports(company_id);
CREATE INDEX IF NOT EXISTS idx_reports_form_type ON reports(form_type);
CREATE INDEX IF NOT EXISTS idx_att_report_id ON attachments(report_id);
CREATE INDEX IF NOT EXISTS idx_att_ref ON attachments(reference_number);
CREATE INDEX IF NOT EXISTS idx_att_status ON attachments(download_status);

CREATE TABLE IF NOT EXISTS companies (
    magna_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    magna_name  TEXT,
    english_name TEXT,
    symbol      TEXT,
    tase_number TEXT,
    isin        TEXT,
    weight      TEXT,
    market_cap  TEXT,
    source      TEXT DEFAULT 'manual',
    created_at  TEXT DEFAULT (datetime('now'))
);
"""


class Database:
    def __init__(self, path: Path | None = None):
        self.path = path or DB_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self):
        """Add columns that may be missing in older databases."""
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(attachments)").fetchall()}
        if "page_count" not in cols:
            self.conn.execute("ALTER TABLE attachments ADD COLUMN page_count INTEGER DEFAULT 0")
            self.conn.commit()
            log.info("Migration: added page_count column to attachments")

        # Auto-seed companies from ta125_magna.json if table is empty
        if self.company_count() == 0:
            self._seed_companies()

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ── Reports ──────────────────────────────────────────────────

    def upsert_report(self, **kwargs) -> int:
        """Insert or update a report. Returns the report id."""
        ref = kwargs["reference_number"]
        cur = self.conn.execute(
            "SELECT id FROM reports WHERE reference_number = ?", (ref,)
        )
        row = cur.fetchone()
        if row:
            # Update non-null fields
            sets = []
            vals = []
            for k, v in kwargs.items():
                if k == "reference_number":
                    continue
                if v is not None:
                    sets.append(f"{k} = ?")
                    vals.append(v)
            if sets:
                sets.append("updated_at = datetime('now')")
                vals.append(row["id"])
                self.conn.execute(
                    f"UPDATE reports SET {', '.join(sets)} WHERE id = ?", vals
                )
                self.conn.commit()
            return row["id"]
        else:
            cols = [k for k in kwargs if kwargs[k] is not None]
            placeholders = ", ".join("?" for _ in cols)
            col_names = ", ".join(cols)
            vals = [kwargs[k] for k in cols]
            cur = self.conn.execute(
                f"INSERT INTO reports ({col_names}) VALUES ({placeholders})", vals
            )
            self.conn.commit()
            return cur.lastrowid

    def get_report(self, report_id: int) -> dict | None:
        cur = self.conn.execute("SELECT * FROM reports WHERE id = ?", (report_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_report_by_ref(self, ref: str) -> dict | None:
        cur = self.conn.execute(
            "SELECT * FROM reports WHERE reference_number = ?", (ref,)
        )
        row = cur.fetchone()
        return dict(row) if row else None

    def report_exists(self, ref: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM reports WHERE reference_number = ?", (ref,)
        )
        return cur.fetchone() is not None

    def has_reports_for_entity(self, entity_id: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM reports WHERE company_id = ? LIMIT 1", (entity_id,)
        )
        return cur.fetchone() is not None

    def get_reports_page(self, page: int = 1, size: int = 50,
                         form_type: str = "", company: str = "",
                         search: str = "", status: str = "") -> dict:
        """Paginated reports query. Returns {items, total, page, size}."""
        conditions = []
        params = []

        if form_type:
            conditions.append("r.form_type = ?")
            params.append(form_type)
        if company:
            conditions.append("(r.company_name LIKE ? OR r.company_id = ?)")
            params.extend([f"%{company}%", company])
        if search:
            conditions.append(
                "(r.subject LIKE ? OR r.reference_number LIKE ? OR r.company_name LIKE ?)"
            )
            params.extend([f"%{search}%"] * 3)
        if status == "scraped":
            conditions.append("r.scraped_at IS NOT NULL AND r.parsed_at IS NULL")
        elif status == "parsed":
            conditions.append("r.parsed_at IS NOT NULL AND r.indexed_at IS NULL")
        elif status == "indexed":
            conditions.append("r.indexed_at IS NOT NULL")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        count_sql = f"SELECT COUNT(*) FROM reports r {where}"
        total = self.conn.execute(count_sql, params).fetchone()[0]

        offset = (page - 1) * size
        query = f"""
            SELECT r.*,
                   (SELECT COUNT(*) FROM attachments a WHERE a.report_id = r.id) as att_count
            FROM reports r {where}
            ORDER BY r.report_date DESC, r.id DESC
            LIMIT ? OFFSET ?
        """
        rows = self.conn.execute(query, params + [size, offset]).fetchall()
        return {
            "items": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "size": size,
        }

    def get_reports_needing_html(self, limit: int = 0) -> list[dict]:
        """Reports with report_url but no form_html."""
        sql = """SELECT id, reference_number, report_url FROM reports
                 WHERE report_url IS NOT NULL AND form_html IS NULL
                 ORDER BY id"""
        if limit:
            sql += f" LIMIT {limit}"
        return [dict(r) for r in self.conn.execute(sql).fetchall()]

    def set_form_html(self, report_id: int, html: str):
        self.conn.execute(
            "UPDATE reports SET form_html = ?, html_fetched_at = datetime('now') WHERE id = ?",
            (html, report_id),
        )
        self.conn.commit()

    def get_reports_needing_parse(self, limit: int = 0, reprocess: bool = False, since: str = "",
                                   company_ids: list[str] | None = None) -> list[dict]:
        """Reports with form_html but not yet parsed (or all with form_html if reprocess)."""
        if reprocess:
            sql = """SELECT id, reference_number, form_type, form_html FROM reports
                     WHERE form_html IS NOT NULL"""
        else:
            sql = """SELECT id, reference_number, form_type, form_html FROM reports
                     WHERE form_html IS NOT NULL AND parsed_at IS NULL"""
        params = []
        if since:
            sql += " AND report_date >= ?"
            params.append(since)
        if company_ids:
            placeholders = ", ".join("?" for _ in company_ids)
            sql += f" AND company_id IN ({placeholders})"
            params.extend(company_ids)
        sql += " ORDER BY id"
        if limit:
            sql += f" LIMIT {limit}"
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def set_form_fields(self, report_id: int, form_fields: str, form_category: str,
                        form_type_code: str | None = None):
        # Clear form_html after parsing — it's only needed for field extraction
        # and storing it bloats the DB (~100 KB per report, ~2 GB total).
        if form_type_code:
            self.conn.execute(
                """UPDATE reports SET form_fields = ?, form_category = ?, form_type = ?,
                   form_html = NULL,
                   parsed_at = datetime('now'), updated_at = datetime('now') WHERE id = ?""",
                (form_fields, form_category, form_type_code, report_id),
            )
        else:
            self.conn.execute(
                """UPDATE reports SET form_fields = ?, form_category = ?,
                   form_html = NULL,
                   parsed_at = datetime('now'), updated_at = datetime('now') WHERE id = ?""",
                (form_fields, form_category, report_id),
            )
        self.conn.commit()

    def set_report_indexed(self, report_id: int):
        self.conn.execute(
            "UPDATE reports SET indexed_at = datetime('now') WHERE id = ?",
            (report_id,),
        )
        self.conn.commit()

    def get_reports_needing_index(self, limit: int = 0, reprocess: bool = False, since: str = "",
                                   company_ids: list[str] | None = None) -> list[dict]:
        """Reports that need (re)indexing: parsed but never indexed,
        OR already indexed but have attachments extracted since last index.
        When reprocess=True, returns all parsed reports."""
        params = []
        if reprocess:
            sql = """SELECT r.*,
                     (SELECT COUNT(*) FROM attachments a WHERE a.report_id = r.id) as att_count
                     FROM reports r
                     WHERE r.parsed_at IS NOT NULL"""
        else:
            sql = """SELECT r.*,
                     (SELECT COUNT(*) FROM attachments a WHERE a.report_id = r.id) as att_count
                     FROM reports r
                     WHERE r.parsed_at IS NOT NULL
                       AND (r.indexed_at IS NULL
                            OR EXISTS (
                                SELECT 1 FROM attachments a
                                WHERE a.report_id = r.id
                                  AND a.extracted_at IS NOT NULL
                                  AND a.indexed_at IS NULL
                            ))"""
        if since:
            sql += " AND r.report_date >= ?"
            params.append(since)
        if company_ids:
            placeholders = ", ".join("?" for _ in company_ids)
            sql += f" AND r.company_id IN ({placeholders})"
            params.extend(company_ids)
        sql += " ORDER BY r.id"
        if limit:
            sql += f" LIMIT {limit}"
        return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def report_has_pending_attachments(self, report_id: int) -> bool:
        """True if report has attachments not yet downloaded or extracted."""
        cur = self.conn.execute(
            """SELECT 1 FROM attachments
               WHERE report_id = ? AND (download_status != 'downloaded' OR extracted_at IS NULL)
               LIMIT 1""",
            (report_id,),
        )
        return cur.fetchone() is not None

    # ── Attachments ──────────────────────────────────────────────

    def upsert_attachment(self, report_id: int, reference_number: str,
                          filename: str, url: str) -> int | None:
        """Insert attachment, skip if duplicate. Returns id or None."""
        try:
            cur = self.conn.execute(
                """INSERT INTO attachments (report_id, reference_number, filename, url)
                   VALUES (?, ?, ?, ?)""",
                (report_id, reference_number, filename, url),
            )
            self.conn.commit()
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return None

    def update_attachment_count(self, report_id: int):
        """Recompute attachment_count on the report."""
        self.conn.execute(
            """UPDATE reports SET attachment_count =
               (SELECT COUNT(*) FROM attachments WHERE report_id = ?)
               WHERE id = ?""",
            (report_id, report_id),
        )
        self.conn.commit()

    def get_pending_attachments(self, reprocess: bool = False, since: str = "",
                                company_ids: list[str] | None = None) -> list[dict]:
        params = []
        if reprocess:
            sql = """SELECT a.*, r.company_name, r.form_type FROM attachments a
                     JOIN reports r ON r.id = a.report_id
                     WHERE 1=1"""
        else:
            sql = """SELECT a.*, r.company_name, r.form_type FROM attachments a
                     JOIN reports r ON r.id = a.report_id
                     WHERE a.download_status = 'pending'"""
        if since:
            sql += " AND r.report_date >= ?"
            params.append(since)
        if company_ids:
            placeholders = ", ".join("?" for _ in company_ids)
            sql += f" AND r.company_id IN ({placeholders})"
            params.extend(company_ids)
        sql += " ORDER BY a.id"
        cur = self.conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    def mark_downloaded(self, att_id: int, local_path: str):
        self.conn.execute(
            """UPDATE attachments SET download_status = 'downloaded',
               local_path = ?, downloaded_at = datetime('now') WHERE id = ?""",
            (local_path, att_id),
        )
        self.conn.commit()

    def mark_failed(self, att_id: int):
        self.conn.execute(
            "UPDATE attachments SET download_status = 'failed' WHERE id = ?",
            (att_id,),
        )
        self.conn.commit()

    def get_downloaded_unextracted(self, reprocess: bool = False, since: str = "",
                                    company_ids: list[str] | None = None) -> list[dict]:
        """Attachments downloaded but text not yet extracted (or all downloaded if reprocess)."""
        params = []
        if reprocess:
            sql = """SELECT a.* FROM attachments a
                     JOIN reports r ON r.id = a.report_id
                     WHERE a.download_status = 'downloaded'"""
        else:
            sql = """SELECT a.* FROM attachments a
                     JOIN reports r ON r.id = a.report_id
                     WHERE a.download_status = 'downloaded' AND a.extracted_at IS NULL"""
        if since:
            sql += " AND r.report_date >= ?"
            params.append(since)
        if company_ids:
            placeholders = ", ".join("?" for _ in company_ids)
            sql += f" AND r.company_id IN ({placeholders})"
            params.extend(company_ids)
        sql += " ORDER BY a.id"
        cur = self.conn.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    def set_attachment_extracted(self, att_id: int, page_count: int = 0):
        self.conn.execute(
            "UPDATE attachments SET extracted_at = datetime('now'), page_count = ? WHERE id = ?",
            (page_count, att_id),
        )
        self.conn.commit()

    def set_attachment_indexed(self, att_id: int):
        self.conn.execute(
            "UPDATE attachments SET indexed_at = datetime('now') WHERE id = ?",
            (att_id,),
        )
        self.conn.commit()

    def get_attachments_page(self, page: int = 1, size: int = 50,
                             status: str = "", report_id: int = 0) -> dict:
        conditions = []
        params = []
        if status:
            if status == "extracted":
                conditions.append("a.extracted_at IS NOT NULL")
            elif status == "indexed":
                conditions.append("a.indexed_at IS NOT NULL")
            else:
                conditions.append("a.download_status = ?")
                params.append(status)
        if report_id:
            conditions.append("a.report_id = ?")
            params.append(report_id)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        total = self.conn.execute(
            f"SELECT COUNT(*) FROM attachments a {where}", params
        ).fetchone()[0]

        offset = (page - 1) * size
        rows = self.conn.execute(
            f"""SELECT a.*, r.company_name, r.form_type, r.subject as report_subject
                FROM attachments a
                JOIN reports r ON r.id = a.report_id
                {where}
                ORDER BY a.id DESC LIMIT ? OFFSET ?""",
            params + [size, offset],
        ).fetchall()
        return {
            "items": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "size": size,
        }

    def get_report_attachments(self, report_id: int) -> list[dict]:
        cur = self.conn.execute(
            "SELECT * FROM attachments WHERE report_id = ? ORDER BY id", (report_id,)
        )
        return [dict(r) for r in cur.fetchall()]

    def get_extracted_attachments_for_report(self, report_id: int) -> list[dict]:
        """Get attachments with extracted text for a report."""
        cur = self.conn.execute(
            """SELECT a.id, a.filename, a.reference_number, d.text_content, d.char_count
               FROM attachments a
               JOIN doc_texts d ON d.attachment_id = a.id
               WHERE a.report_id = ? AND d.char_count > 0
               ORDER BY a.id""",
            (report_id,),
        )
        return [dict(r) for r in cur.fetchall()]

    # ── Doc Texts ────────────────────────────────────────────────

    def insert_doc_text(self, attachment_id: int, text_content: str, char_count: int):
        self.conn.execute(
            "INSERT OR REPLACE INTO doc_texts (attachment_id, text_content, char_count) VALUES (?, ?, ?)",
            (attachment_id, text_content, char_count),
        )
        self.conn.commit()

    def is_extracted(self, attachment_id: int) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM doc_texts WHERE attachment_id = ?", (attachment_id,)
        )
        return cur.fetchone() is not None

    # ── Stats ────────────────────────────────────────────────────

    def stats(self) -> dict:
        """Pipeline funnel metrics."""
        r = self.conn.execute
        return {
            "reports": {
                "total": r("SELECT COUNT(*) FROM reports").fetchone()[0],
                "html_fetched": r("SELECT COUNT(*) FROM reports WHERE html_fetched_at IS NOT NULL").fetchone()[0],
                "parsed": r("SELECT COUNT(*) FROM reports WHERE parsed_at IS NOT NULL").fetchone()[0],
                "indexed": r("SELECT COUNT(*) FROM reports WHERE indexed_at IS NOT NULL").fetchone()[0],
            },
            "attachments": {
                "total": r("SELECT COUNT(*) FROM attachments").fetchone()[0],
                "downloaded": r("SELECT COUNT(*) FROM attachments WHERE download_status = 'downloaded'").fetchone()[0],
                "failed": r("SELECT COUNT(*) FROM attachments WHERE download_status = 'failed'").fetchone()[0],
                "extracted": r("SELECT COUNT(*) FROM attachments WHERE extracted_at IS NOT NULL").fetchone()[0],
                "indexed": r("SELECT COUNT(*) FROM attachments WHERE indexed_at IS NOT NULL").fetchone()[0],
            },
        }

    # ── Watermarks ─────────────────────────────────────────────

    def get_watermark(self, company_id: str) -> str | None:
        """Return the scraped_through ISO date for a company, or None."""
        cur = self.conn.execute(
            "SELECT scraped_through FROM scrape_watermarks WHERE company_id = ?",
            (company_id,),
        )
        row = cur.fetchone()
        return row["scraped_through"] if row else None

    def set_watermark(self, company_id: str, scraped_through: str):
        """Upsert the high-water mark for a company."""
        self.conn.execute(
            """INSERT INTO scrape_watermarks (company_id, scraped_through, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(company_id) DO UPDATE SET
                   scraped_through = excluded.scraped_through,
                   updated_at = datetime('now')""",
            (company_id, scraped_through),
        )
        self.conn.commit()

    # ── Companies ──────────────────────────────────────────────

    def _seed_companies(self):
        """Seed companies table from ta125_magna.json."""
        from config import COMPANY_LIST_PATH
        if not COMPANY_LIST_PATH.exists():
            log.warning(f"Cannot seed companies: {COMPANY_LIST_PATH} not found")
            return
        companies = json.loads(COMPANY_LIST_PATH.read_text(encoding="utf-8"))
        for c in companies:
            self.conn.execute(
                """INSERT OR IGNORE INTO companies
                   (magna_id, name, magna_name, english_name, symbol, tase_number, isin, weight, market_cap, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'ta125')""",
                (str(c["magna_id"]), c.get("name", ""), c.get("magna_name"),
                 c.get("english_name"), c.get("symbol"), c.get("tase_number"),
                 c.get("isin"), c.get("weight"), c.get("market_cap")),
            )
        self.conn.commit()
        log.info(f"Seeded {len(companies)} companies from ta125_magna.json")

    def company_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

    def get_companies(self, company_ids: list[str] | None = None) -> list[dict]:
        """Return all companies, or filtered by magna_id list."""
        if company_ids:
            placeholders = ", ".join("?" for _ in company_ids)
            cur = self.conn.execute(
                f"SELECT * FROM companies WHERE magna_id IN ({placeholders}) ORDER BY name",
                company_ids,
            )
        else:
            cur = self.conn.execute("SELECT * FROM companies ORDER BY name")
        return [dict(r) for r in cur.fetchall()]

    def add_company(self, magna_id: str, name: str, magna_name: str | None = None,
                    english_name: str | None = None, symbol: str | None = None,
                    tase_number: str | None = None, isin: str | None = None,
                    source: str = "manual") -> bool:
        """Insert a company. Returns True if inserted, False if already exists."""
        try:
            self.conn.execute(
                """INSERT INTO companies (magna_id, name, magna_name, english_name, symbol, tase_number, isin, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (magna_id, name, magna_name, english_name, symbol, tase_number, isin, source),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_company(self, magna_id: str) -> bool:
        """Delete a company by magna_id. Returns True if deleted."""
        cur = self.conn.execute("DELETE FROM companies WHERE magna_id = ?", (magna_id,))
        self.conn.commit()
        return cur.rowcount > 0

    def form_type_counts(self) -> list[dict]:
        cur = self.conn.execute(
            """SELECT form_type, form_name, COUNT(*) as cnt FROM reports
               WHERE form_type IS NOT NULL AND form_type != ''
               GROUP BY form_type ORDER BY cnt DESC"""
        )
        return [dict(r) for r in cur.fetchall()]
