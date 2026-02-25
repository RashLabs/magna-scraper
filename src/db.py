"""SQLite database layer for MAGNA reports."""

import sqlite3
import logging
from pathlib import Path

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "magna.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reference_number TEXT UNIQUE NOT NULL,
    report_date TEXT,
    report_time TEXT,
    reporter_name TEXT,
    form_name TEXT,
    report_name TEXT,
    report_url TEXT,
    subject TEXT,
    scraped_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_reference TEXT NOT NULL,
    filename TEXT,
    url TEXT NOT NULL,
    scraped_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (report_reference) REFERENCES reports(reference_number),
    UNIQUE(report_reference, url)
);

CREATE INDEX IF NOT EXISTS idx_report_date ON reports(report_date);
CREATE INDEX IF NOT EXISTS idx_reporter ON reports(reporter_name);
"""

MIGRATIONS = [
    # Add download tracking columns to attachments
    """ALTER TABLE attachments ADD COLUMN local_path TEXT""",
    """ALTER TABLE attachments ADD COLUMN download_status TEXT DEFAULT 'pending'""",
    """ALTER TABLE attachments ADD COLUMN downloaded_at TEXT""",
    # Bulk scrape: track which entity/company each report belongs to
    """ALTER TABLE reports ADD COLUMN entity_id TEXT""",
    """ALTER TABLE reports ADD COLUMN company_name TEXT""",
    """CREATE INDEX IF NOT EXISTS idx_entity_id ON reports(entity_id)""",
    # Phase 3: text extraction and embeddings tables
    """CREATE TABLE IF NOT EXISTS doc_texts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        attachment_id INTEGER NOT NULL UNIQUE,
        full_text TEXT,
        char_count INTEGER,
        extracted_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (attachment_id) REFERENCES attachments(id)
    )""",
    """CREATE TABLE IF NOT EXISTS chunks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        attachment_id INTEGER NOT NULL,
        chunk_index INTEGER NOT NULL,
        chunk_text TEXT NOT NULL,
        chunk_chars INTEGER,
        FOREIGN KEY (attachment_id) REFERENCES attachments(id)
    )""",
    """CREATE TABLE IF NOT EXISTS embeddings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chunk_id INTEGER NOT NULL UNIQUE,
        embedding TEXT NOT NULL,
        model_name TEXT,
        dimensions INTEGER,
        embedded_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (chunk_id) REFERENCES chunks(id)
    )""",
]


class Database:
    def __init__(self, path: Path | None = None):
        self.path = path or DB_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self._run_migrations()

    def _run_migrations(self):
        for sql in MIGRATIONS:
            try:
                self.conn.execute(sql)
                self.conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.conn.close()

    def report_exists(self, reference_number: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM reports WHERE reference_number = ?",
            (reference_number,),
        )
        return cur.fetchone() is not None

    def has_reports_for_entity(self, entity_id: str) -> bool:
        """Check if we already have any reports for the given entity."""
        cur = self.conn.execute(
            "SELECT 1 FROM reports WHERE entity_id = ? LIMIT 1",
            (entity_id,),
        )
        return cur.fetchone() is not None

    def insert_report(self, **kwargs) -> bool:
        """Insert a report. Returns True if inserted, False if duplicate."""
        try:
            cols = ["reference_number", "report_date", "report_time", "reporter_name",
                    "form_name", "report_name", "report_url", "subject"]
            # Include optional columns if provided
            for opt in ("entity_id", "company_name"):
                if opt in kwargs:
                    cols.append(opt)
            placeholders = ", ".join(f":{c}" for c in cols)
            col_names = ", ".join(cols)
            self.conn.execute(
                f"INSERT INTO reports ({col_names}) VALUES ({placeholders})",
                kwargs,
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def insert_attachment(self, report_reference: str, filename: str, url: str) -> bool:
        """Insert an attachment. Returns True if inserted, False if duplicate."""
        try:
            self.conn.execute(
                """INSERT INTO attachments (report_reference, filename, url)
                   VALUES (?, ?, ?)""",
                (report_reference, filename, url),
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def count_reports(self) -> int:
        cur = self.conn.execute("SELECT COUNT(*) FROM reports")
        return cur.fetchone()[0]

    def count_attachments(self) -> int:
        cur = self.conn.execute("SELECT COUNT(*) FROM attachments")
        return cur.fetchone()[0]

    def get_reports(self, search: str = "", limit: int = 100, offset: int = 0) -> list[dict]:
        """Get reports with optional search, joined with attachment count."""
        if search:
            cur = self.conn.execute(
                """SELECT r.*, COUNT(a.id) as attachment_count
                   FROM reports r
                   LEFT JOIN attachments a ON a.report_reference = r.reference_number
                   WHERE r.report_name LIKE ? OR r.subject LIKE ?
                         OR r.reporter_name LIKE ? OR r.reference_number LIKE ?
                   GROUP BY r.id
                   ORDER BY r.report_date DESC, r.report_time DESC
                   LIMIT ? OFFSET ?""",
                (f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%",
                 limit, offset),
            )
        else:
            cur = self.conn.execute(
                """SELECT r.*, COUNT(a.id) as attachment_count
                   FROM reports r
                   LEFT JOIN attachments a ON a.report_reference = r.reference_number
                   GROUP BY r.id
                   ORDER BY r.report_date DESC, r.report_time DESC
                   LIMIT ? OFFSET ?""",
                (limit, offset),
            )
        return [dict(row) for row in cur.fetchall()]

    def get_attachments(self, reference_number: str) -> list[dict]:
        cur = self.conn.execute(
            "SELECT * FROM attachments WHERE report_reference = ?",
            (reference_number,),
        )
        return [dict(row) for row in cur.fetchall()]

    def company_report_counts(self) -> list[dict]:
        """Get report and attachment counts grouped by company."""
        cur = self.conn.execute(
            """SELECT entity_id, company_name,
                      COUNT(*) as report_count,
                      (SELECT COUNT(*) FROM attachments a
                       WHERE a.report_reference IN (
                           SELECT r2.reference_number FROM reports r2
                           WHERE r2.entity_id = reports.entity_id
                       )) as attachment_count
               FROM reports
               WHERE entity_id IS NOT NULL
               GROUP BY entity_id
               ORDER BY report_count DESC"""
        )
        return [dict(row) for row in cur.fetchall()]

    def get_pending_attachments(self) -> list[dict]:
        """Get attachments that haven't been downloaded yet."""
        cur = self.conn.execute(
            """SELECT * FROM attachments
               WHERE download_status IS NULL OR download_status = 'pending'
               ORDER BY id"""
        )
        return [dict(row) for row in cur.fetchall()]

    def get_all_attachments_with_status(self) -> list[dict]:
        """Get all attachments with download status."""
        cur = self.conn.execute(
            "SELECT * FROM attachments ORDER BY report_reference, id"
        )
        return [dict(row) for row in cur.fetchall()]

    def mark_downloaded(self, attachment_id: int, local_path: str):
        self.conn.execute(
            """UPDATE attachments
               SET download_status = 'downloaded', local_path = ?, downloaded_at = datetime('now')
               WHERE id = ?""",
            (local_path, attachment_id),
        )
        self.conn.commit()

    def mark_failed(self, attachment_id: int):
        self.conn.execute(
            "UPDATE attachments SET download_status = 'failed' WHERE id = ?",
            (attachment_id,),
        )
        self.conn.commit()

    def download_stats(self) -> dict:
        cur = self.conn.execute(
            """SELECT download_status, COUNT(*) as cnt
               FROM attachments GROUP BY download_status"""
        )
        return {row["download_status"] or "pending": row["cnt"] for row in cur.fetchall()}

    def get_all_reports_with_attachments(self) -> list[dict]:
        """Get all reports with their attachments nested."""
        reports = self.get_reports(limit=10000)
        for r in reports:
            r["attachments"] = self.get_attachments(r["reference_number"])
        return reports

    # --- Phase 3: text extraction & embeddings ---

    def reset_extraction_tables(self):
        """DROP and recreate doc_texts, chunks, embeddings for a fresh start."""
        self.conn.executescript("""
            DROP TABLE IF EXISTS embeddings;
            DROP TABLE IF EXISTS chunks;
            DROP TABLE IF EXISTS doc_texts;

            CREATE TABLE doc_texts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                attachment_id INTEGER NOT NULL UNIQUE,
                full_text TEXT,
                char_count INTEGER,
                extracted_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (attachment_id) REFERENCES attachments(id)
            );

            CREATE TABLE chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                attachment_id INTEGER NOT NULL,
                page_number INTEGER NOT NULL,
                chunk_index INTEGER NOT NULL,
                chunk_text TEXT NOT NULL,
                word_count INTEGER,
                metadata TEXT,
                FOREIGN KEY (attachment_id) REFERENCES attachments(id)
            );

            CREATE TABLE embeddings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_id INTEGER NOT NULL UNIQUE,
                embedding TEXT NOT NULL,
                model_name TEXT,
                dimensions INTEGER,
                embedded_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (chunk_id) REFERENCES chunks(id)
            );
        """)
        log.info("Reset doc_texts, chunks, embeddings tables")

    def get_downloaded_attachments(self) -> list[dict]:
        cur = self.conn.execute(
            "SELECT * FROM attachments WHERE download_status = 'downloaded' ORDER BY id"
        )
        return [dict(row) for row in cur.fetchall()]

    def is_extracted(self, attachment_id: int) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM doc_texts WHERE attachment_id = ?", (attachment_id,)
        )
        return cur.fetchone() is not None

    def insert_doc_text(self, attachment_id: int, full_text: str, char_count: int):
        self.conn.execute(
            "INSERT INTO doc_texts (attachment_id, full_text, char_count) VALUES (?, ?, ?)",
            (attachment_id, full_text, char_count),
        )
        self.conn.commit()

    def get_unembedded_attachments(self) -> list[dict]:
        """Get attachments that have extracted text but no embeddings yet."""
        cur = self.conn.execute(
            """SELECT dt.attachment_id, dt.full_text, dt.char_count
               FROM doc_texts dt
               WHERE dt.attachment_id NOT IN (
                   SELECT DISTINCT c.attachment_id FROM chunks c
                   INNER JOIN embeddings e ON e.chunk_id = c.id
               )
               AND dt.char_count > 0
               ORDER BY dt.attachment_id"""
        )
        return [dict(row) for row in cur.fetchall()]

    def insert_chunks_and_embeddings(self, attachment_id: int, chunks_data: list[dict]):
        """Insert chunks and their embeddings in a transaction.
        chunks_data: [{'page_number': int, 'chunk_index': int, 'text': str,
                       'word_count': int, 'metadata': str (JSON), 'embedding': str (JSON)}]
        """
        for cd in chunks_data:
            cur = self.conn.execute(
                """INSERT INTO chunks (attachment_id, page_number, chunk_index,
                   chunk_text, word_count, metadata) VALUES (?, ?, ?, ?, ?, ?)""",
                (attachment_id, cd["page_number"], cd["chunk_index"],
                 cd["text"], cd["word_count"], cd["metadata"]),
            )
            chunk_id = cur.lastrowid
            self.conn.execute(
                "INSERT INTO embeddings (chunk_id, embedding, model_name, dimensions) VALUES (?, ?, ?, ?)",
                (chunk_id, cd["embedding"], "gemini-embedding-001", 1536),
            )
        self.conn.commit()

    def get_all_embeddings(self) -> list[dict]:
        """Get all embeddings with chunk text and source info for search."""
        cur = self.conn.execute(
            """SELECT e.embedding, c.chunk_text, c.chunk_index, c.page_number,
                      c.attachment_id, a.report_reference, a.filename
               FROM embeddings e
               JOIN chunks c ON c.id = e.chunk_id
               JOIN attachments a ON a.id = c.attachment_id
               ORDER BY c.attachment_id, c.page_number, c.chunk_index"""
        )
        return [dict(row) for row in cur.fetchall()]

    def extraction_stats(self) -> dict:
        """Get extraction and embedding statistics."""
        extracted = self.conn.execute("SELECT COUNT(*) FROM doc_texts").fetchone()[0]
        total_chars = self.conn.execute("SELECT COALESCE(SUM(char_count), 0) FROM doc_texts").fetchone()[0]
        empty = self.conn.execute("SELECT COUNT(*) FROM doc_texts WHERE char_count = 0").fetchone()[0]
        n_chunks = self.conn.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        n_embedded = self.conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]

        # Page stats from doc_texts JSON
        import json
        total_pages = 0
        pages_per_doc = []
        cur = self.conn.execute("SELECT full_text FROM doc_texts WHERE char_count > 0")
        for row in cur.fetchall():
            try:
                data = json.loads(row["full_text"])
                n = len(data.get("pages", []))
                total_pages += n
                pages_per_doc.append(n)
            except (json.JSONDecodeError, TypeError):
                pass

        return {
            "extracted": extracted,
            "total_chars": total_chars,
            "empty_docs": empty,
            "chunks": n_chunks,
            "embedded": n_embedded,
            "total_pages": total_pages,
            "pages_per_doc": pages_per_doc,
        }
