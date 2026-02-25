"""Streamlit admin dashboard for MAGNA TA-125 pipeline."""

import sqlite3
import subprocess
import sys
import threading
import time
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "magna.db"
SRC_DIR = Path(__file__).resolve().parent
COMPANY_LIST = Path(__file__).resolve().parent.parent / "data" / "ta125_magna.json"

PIPELINE_STAGES = ("scrape", "download", "extract", "embed")

# ---------------------------------------------------------------------------
# DB helpers (raw sqlite3, no import of db.py needed)
# ---------------------------------------------------------------------------

@st.cache_resource
def get_connection():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def q(sql: str, params: tuple = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, get_connection(), params=params)


def q_scalar(sql: str, params: tuple = ()):
    cur = get_connection().execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Background process runner
# ---------------------------------------------------------------------------

def _run_process(cmd: list[str], stage: str, env: dict | None = None):
    """Run a subprocess and stream its output into session_state."""
    import os
    if env is None:
        env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    st.session_state[f"{stage}_status"] = "running"
    st.session_state[f"{stage}_log"] = ""
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            cwd=str(SRC_DIR),
            bufsize=1,
            env=env,
        )
        st.session_state[f"{stage}_proc"] = proc
        for line in proc.stdout:
            st.session_state[f"{stage}_log"] += line
        proc.wait()
        if proc.returncode == 0:
            st.session_state[f"{stage}_status"] = "done"
        else:
            st.session_state[f"{stage}_status"] = "error"
    except Exception as exc:
        st.session_state[f"{stage}_log"] += f"\nERROR: {exc}\n"
        st.session_state[f"{stage}_status"] = "error"


def launch_pipeline(cmd: list[str], stage: str, env: dict | None = None):
    ctx = get_script_run_ctx()
    t = threading.Thread(target=_run_process, args=(cmd, stage, env), daemon=True)
    add_script_run_ctx(t, ctx)
    t.start()


def stop_pipeline(stage: str):
    proc = st.session_state.get(f"{stage}_proc")
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    st.session_state[f"{stage}_status"] = "idle"
    st.session_state[f"{stage}_log"] += "\n--- stopped by user ---\n"


def status_badge(stage: str) -> str:
    s = st.session_state.get(f"{stage}_status", "idle")
    return {"idle": ":gray[idle]", "running": ":orange[running...]",
            "done": ":green[done]", "error": ":red[error]"}[s]


def init_stage(stage: str):
    for key in (f"{stage}_status", f"{stage}_log", f"{stage}_proc"):
        if key not in st.session_state:
            st.session_state[key] = "idle" if key.endswith("_status") else ("" if key.endswith("_log") else None)


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(page_title="MAGNA Pipeline", page_icon=">>", layout="wide")

st.markdown(
    """<style>
    td, th { direction: rtl; text-align: right; }
    .rtl-preview { direction: rtl; text-align: right; unicode-bidi: plaintext;
                   font-size: 0.85em; line-height: 1.6;
                   background: var(--secondary-background-color);
                   padding: 1em; border-radius: 0.5em; }
    </style>""",
    unsafe_allow_html=True,
)

for stage in PIPELINE_STAGES:
    init_stage(stage)

# ---------------------------------------------------------------------------
# Sidebar: Pipeline Controls
# ---------------------------------------------------------------------------

with st.sidebar:
    st.header("Pipeline Controls")

    # --- Scrape Reports ---
    st.subheader("1. Scrape Reports")
    st.markdown(status_badge("scrape"))
    scrape_mode = st.radio("Mode", ["Single (NewMed)", "Bulk (ta125_magna)"], horizontal=True, key="scrape_mode")
    scrape_since = st.date_input("Since", value=date(2024, 1, 1), key="scrape_since")
    scrape_headless = st.checkbox("Headless", value=True, key="scrape_headless")
    scrape_run_col, scrape_stop_col = st.columns(2)
    with scrape_run_col:
        if st.button("Run Scrape", disabled=st.session_state.get("scrape_status") == "running"):
            cmd = [sys.executable, "-m", "scraper", "--since", str(scrape_since)]
            if scrape_headless:
                cmd.append("--headless")
            if scrape_mode.startswith("Bulk"):
                cmd.extend(["--company-list", str(COMPANY_LIST)])
            launch_pipeline(cmd, "scrape")
            st.rerun()
    with scrape_stop_col:
        if st.button("Stop", key="stop_scrape", disabled=st.session_state.get("scrape_status") != "running"):
            stop_pipeline("scrape")
            st.rerun()
    with st.expander("Scrape log", expanded=st.session_state.get("scrape_status") == "running"):
        st.code(st.session_state.get("scrape_log", "") or "(no output yet)", language="log")

    st.divider()

    # --- Download Attachments ---
    st.subheader("2. Download Attachments")
    st.markdown(status_badge("download"))
    try:
        dl_pending = q_scalar("SELECT COUNT(*) FROM attachments WHERE download_status IS NULL OR download_status = 'pending'")
        dl_done = q_scalar("SELECT COUNT(*) FROM attachments WHERE download_status = 'downloaded'")
        dl_failed = q_scalar("SELECT COUNT(*) FROM attachments WHERE download_status = 'failed'")
        st.caption(f"Pending: {dl_pending} | Downloaded: {dl_done} | Failed: {dl_failed}")
    except Exception:
        st.caption("(stats unavailable)")
    dl_headless = st.checkbox("Headless", value=True, key="dl_headless")
    dl_run_col, dl_stop_col = st.columns(2)
    with dl_run_col:
        if st.button("Run Download", disabled=st.session_state.get("download_status") == "running"):
            cmd = [sys.executable, "-m", "downloader"]
            if dl_headless:
                cmd.append("--headless")
            launch_pipeline(cmd, "download")
            st.rerun()
    with dl_stop_col:
        if st.button("Stop", key="stop_download", disabled=st.session_state.get("download_status") != "running"):
            stop_pipeline("download")
            st.rerun()
    with st.expander("Download log", expanded=st.session_state.get("download_status") == "running"):
        st.code(st.session_state.get("download_log", "") or "(no output yet)", language="log")

    st.divider()

    # --- Extract Text ---
    st.subheader("3. Extract Text")
    st.markdown(status_badge("extract"))
    try:
        ext_downloaded = q_scalar("SELECT COUNT(*) FROM attachments WHERE download_status = 'downloaded'")
        ext_extracted = q_scalar("SELECT COUNT(*) FROM doc_texts")
        st.caption(f"Downloaded: {ext_downloaded} | Extracted: {ext_extracted} | Remaining: {max(0, ext_downloaded - ext_extracted)}")
    except Exception:
        st.caption("(stats unavailable)")
    extract_reset = st.checkbox("Reset tables (wipe doc_texts, chunks, embeddings)", key="extract_reset")
    ext_run_col, ext_stop_col = st.columns(2)
    with ext_run_col:
        if st.button("Run Extraction", disabled=st.session_state.get("extract_status") == "running"):
            cmd = [sys.executable, "-m", "extractor"]
            if extract_reset:
                cmd.append("--reset")
            launch_pipeline(cmd, "extract")
            st.rerun()
    with ext_stop_col:
        if st.button("Stop", key="stop_extract", disabled=st.session_state.get("extract_status") != "running"):
            stop_pipeline("extract")
            st.rerun()
    with st.expander("Extraction log", expanded=st.session_state.get("extract_status") == "running"):
        st.code(st.session_state.get("extract_log", "") or "(no output yet)", language="log")

    st.divider()

    # --- Generate Embeddings ---
    st.subheader("4. Generate Embeddings")
    st.markdown(status_badge("embed"))
    try:
        n_unembedded = q_scalar(
            """SELECT COUNT(*) FROM doc_texts dt
               WHERE dt.char_count > 0
               AND dt.attachment_id NOT IN (
                   SELECT DISTINCT c.attachment_id FROM chunks c
                   INNER JOIN embeddings e ON e.chunk_id = c.id
               )"""
        )
        st.caption(f"Unembedded docs: {n_unembedded}")
    except Exception:
        st.caption("(stats unavailable)")
    embed_api_key = st.text_input("Gemini API Key (or set GEMINI_API_KEY env)", type="password", key="embed_api_key")
    emb_run_col, emb_stop_col = st.columns(2)
    with emb_run_col:
        if st.button("Run Embedder", disabled=st.session_state.get("embed_status") == "running"):
            import os
            cmd = [sys.executable, "-m", "embedder"]
            env = os.environ.copy()
            if embed_api_key:
                env["GEMINI_API_KEY"] = embed_api_key
            launch_pipeline(cmd, "embed", env=env)
            st.rerun()
    with emb_stop_col:
        if st.button("Stop", key="stop_embed", disabled=st.session_state.get("embed_status") != "running"):
            stop_pipeline("embed")
            st.rerun()
    with st.expander("Embedder log", expanded=st.session_state.get("embed_status") == "running"):
        st.code(st.session_state.get("embed_log", "") or "(no output yet)", language="log")


# ---------------------------------------------------------------------------
# Main area: Data Tables
# ---------------------------------------------------------------------------

st.title("MAGNA Pipeline Dashboard")

tab_overview, tab_reports, tab_attachments, tab_doc_texts, tab_chunks, tab_embeddings = st.tabs(
    ["Overview", "Reports", "Attachments", "Doc Texts", "Chunks", "Embeddings"]
)

# ── Overview ────────────────────────────────────────────────────────────

with tab_overview:
    if st.button("Refresh", key="refresh_overview"):
        st.rerun()

    try:
        n_reports = q_scalar("SELECT COUNT(*) FROM reports")
        n_companies = q_scalar("SELECT COUNT(DISTINCT company_name) FROM reports WHERE company_name IS NOT NULL")
        n_att = q_scalar("SELECT COUNT(*) FROM attachments")
        n_att_pending = q_scalar("SELECT COUNT(*) FROM attachments WHERE download_status IS NULL OR download_status = 'pending'")
        n_att_downloaded = q_scalar("SELECT COUNT(*) FROM attachments WHERE download_status = 'downloaded'")
        n_att_failed = q_scalar("SELECT COUNT(*) FROM attachments WHERE download_status = 'failed'")
        n_extracted = q_scalar("SELECT COUNT(*) FROM doc_texts")
        n_extracted_ok = q_scalar("SELECT COUNT(*) FROM doc_texts WHERE char_count > 0")
        n_extracted_empty = q_scalar("SELECT COUNT(*) FROM doc_texts WHERE char_count = 0")
        n_chunks = q_scalar("SELECT COUNT(*) FROM chunks")
        n_embeddings = q_scalar("SELECT COUNT(*) FROM embeddings")
    except Exception:
        n_reports = n_companies = n_att = n_att_pending = n_att_downloaded = n_att_failed = 0
        n_extracted = n_extracted_ok = n_extracted_empty = n_chunks = n_embeddings = 0

    # Pipeline funnel metrics
    st.subheader("Pipeline Status")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Reports", f"{n_reports:,}", help=f"{n_companies} companies")
    c2.metric("Attachments", f"{n_att:,}", help=f"{n_att_pending} pending / {n_att_failed} failed")
    c3.metric("Downloaded", f"{n_att_downloaded:,}", help=f"{n_att_downloaded}/{n_att} total")
    c4.metric("Extracted", f"{n_extracted_ok:,}", help=f"{n_extracted_empty} empty / {n_extracted} total")
    c5.metric("Embedded", f"{n_embeddings:,}", help=f"{n_chunks} chunks")

    # Progress bars
    st.caption("Download progress")
    st.progress(n_att_downloaded / n_att if n_att else 0, text=f"{n_att_downloaded:,} / {n_att:,}")
    st.caption("Extraction progress")
    st.progress(n_extracted / n_att_downloaded if n_att_downloaded else 0, text=f"{n_extracted:,} / {n_att_downloaded:,}")
    st.caption("Embedding progress (chunks)")
    st.progress(n_embeddings / n_chunks if n_chunks else 0, text=f"{n_embeddings:,} / {n_chunks:,}")

    # DB schema diagram
    st.subheader("Database Schema")
    st.code("""
┌─────────────────────────────────┐
│            reports              │
├─────────────────────────────────┤
│ id               INTEGER PK    │
│ reference_number TEXT UNIQUE    │──┐
│ report_date      TEXT           │  │
│ report_time      TEXT           │  │
│ reporter_name    TEXT           │  │
│ form_name        TEXT           │  │
│ report_name      TEXT           │  │
│ report_url       TEXT           │  │
│ subject          TEXT           │  │
│ entity_id        TEXT           │  │
│ company_name     TEXT           │  │
│ scraped_at       TEXT           │  │
└─────────────────────────────────┘  │
                                     │ report_reference
┌─────────────────────────────────┐  │
│          attachments            │  │
├─────────────────────────────────┤  │
│ id               INTEGER PK    │──┐
│ report_reference TEXT FK        │←─┘
│ filename         TEXT           │  │
│ url              TEXT           │  │
│ download_status  TEXT           │  │
│ local_path       TEXT           │  │
│ downloaded_at    TEXT           │  │
│ scraped_at       TEXT           │  │
└─────────────────────────────────┘  │
                                     │ attachment_id
┌─────────────────────────────────┐  │
│           doc_texts             │  │
├─────────────────────────────────┤  │
│ id               INTEGER PK    │  │
│ attachment_id    INTEGER FK UQ  │←─┤
│ full_text        TEXT (JSON)    │  │
│ char_count       INTEGER        │  │
│ extracted_at     TEXT           │  │
└─────────────────────────────────┘  │
                                     │ attachment_id
┌─────────────────────────────────┐  │
│            chunks               │  │
├─────────────────────────────────┤  │
│ id               INTEGER PK    │──┐
│ attachment_id    INTEGER FK     │←─┘
│ page_number      INTEGER        │  │
│ chunk_index      INTEGER        │  │
│ chunk_text       TEXT           │  │
│ word_count       INTEGER        │  │
│ metadata         TEXT (JSON)    │  │
└─────────────────────────────────┘  │
                                     │ chunk_id
┌─────────────────────────────────┐  │
│          embeddings             │  │
├─────────────────────────────────┤  │
│ id               INTEGER PK    │  │
│ chunk_id         INTEGER FK UQ  │←─┘
│ embedding        TEXT (JSON)    │
│ model_name       TEXT           │
│ dimensions       INTEGER        │
│ embedded_at      TEXT           │
└─────────────────────────────────┘
    """, language=None)

# ── Reports ──────────────────────────────────────────────────────────────

PAGE_SIZE = 50

with tab_reports:
    try:
        total_reports = q_scalar("SELECT COUNT(*) FROM reports")
    except Exception:
        total_reports = 0

    col_m, col_f, col_r = st.columns([1, 3, 1])
    col_m.metric("Total Reports", f"{total_reports:,}")
    report_filter = col_f.text_input("Filter (subject / company / reference)", key="report_filter")
    if col_r.button("Refresh", key="refresh_reports"):
        st.rerun()

    page_num = st.number_input("Page", min_value=1, value=1, step=1, key="report_page")
    offset = (page_num - 1) * PAGE_SIZE

    if report_filter:
        like = f"%{report_filter}%"
        reports_df = q(
            """SELECT reference_number, report_date, company_name, form_name, subject, report_url
               FROM reports
               WHERE subject LIKE ? OR company_name LIKE ? OR reference_number LIKE ?
               ORDER BY report_date DESC
               LIMIT ? OFFSET ?""",
            (like, like, like, PAGE_SIZE, offset),
        )
    else:
        reports_df = q(
            """SELECT reference_number, report_date, company_name, form_name, subject, report_url
               FROM reports ORDER BY report_date DESC LIMIT ? OFFSET ?""",
            (PAGE_SIZE, offset),
        )

    st.dataframe(
        reports_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "report_url": st.column_config.LinkColumn("URL", display_text="Open"),
        },
    )

# ── Attachments ──────────────────────────────────────────────────────────

with tab_attachments:
    try:
        total_att = q_scalar("SELECT COUNT(*) FROM attachments")
    except Exception:
        total_att = 0

    col_m, col_f, col_r = st.columns([1, 2, 1])
    col_m.metric("Total Attachments", f"{total_att:,}")
    att_status_filter = col_f.selectbox(
        "Filter by status",
        ["All", "pending", "downloaded", "failed"],
        key="att_status_filter",
    )
    if col_r.button("Refresh", key="refresh_att"):
        st.rerun()

    att_page = st.number_input("Page", min_value=1, value=1, step=1, key="att_page")
    att_offset = (att_page - 1) * PAGE_SIZE

    if att_status_filter == "All":
        att_df = q(
            """SELECT report_reference, filename, url, download_status, local_path, downloaded_at
               FROM attachments ORDER BY id DESC LIMIT ? OFFSET ?""",
            (PAGE_SIZE, att_offset),
        )
    elif att_status_filter == "pending":
        att_df = q(
            """SELECT report_reference, filename, url, download_status, local_path, downloaded_at
               FROM attachments
               WHERE download_status IS NULL OR download_status = 'pending'
               ORDER BY id DESC LIMIT ? OFFSET ?""",
            (PAGE_SIZE, att_offset),
        )
    else:
        att_df = q(
            """SELECT report_reference, filename, url, download_status, local_path, downloaded_at
               FROM attachments WHERE download_status = ? ORDER BY id DESC LIMIT ? OFFSET ?""",
            (att_status_filter, PAGE_SIZE, att_offset),
        )

    st.dataframe(att_df, use_container_width=True, hide_index=True)

# ── Doc Texts ────────────────────────────────────────────────────────────

with tab_doc_texts:
    try:
        total_docs = q_scalar("SELECT COUNT(*) FROM doc_texts")
    except Exception:
        total_docs = 0

    col_m, _, col_r = st.columns([1, 3, 1])
    col_m.metric("Extracted Docs", f"{total_docs:,}")
    if col_r.button("Refresh", key="refresh_docs"):
        st.rerun()

    doc_page = st.number_input("Page", min_value=1, value=1, step=1, key="doc_page")
    doc_offset = (doc_page - 1) * PAGE_SIZE

    docs_df = q(
        """SELECT dt.id, dt.attachment_id, dt.char_count, dt.extracted_at,
                  CASE WHEN dt.char_count > 0 THEN 'yes' ELSE 'no' END AS has_text,
                  a.filename
           FROM doc_texts dt
           LEFT JOIN attachments a ON a.id = dt.attachment_id
           ORDER BY dt.id DESC LIMIT ? OFFSET ?""",
        (PAGE_SIZE, doc_offset),
    )

    selected_rows = st.dataframe(
        docs_df,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    # Preview panel for selected row
    sel = selected_rows.get("selection", {}).get("rows", [])
    if sel:
        row = docs_df.iloc[sel[0]]
        with st.expander(f"Preview — {row['filename']} (attachment {row['attachment_id']})", expanded=True):
            import json as _json
            raw = q(
                "SELECT full_text FROM doc_texts WHERE id = ?",
                (int(row["id"]),),
            )
            if not raw.empty:
                full_text = raw.iloc[0]["full_text"]
                try:
                    doc = _json.loads(full_text)
                    pages = doc.get("pages", [])
                    st.caption(f"{len(pages)} pages, {row['char_count']:,} chars")
                    for p in pages:
                        st.markdown(f"**Page {p['page_number']}** ({p['word_count']} words)")
                        st.markdown(
                            f'<div class="rtl-preview">{p["content"][:3000]}</div>',
                            unsafe_allow_html=True,
                        )
                        st.divider()
                except (_json.JSONDecodeError, TypeError):
                    st.markdown(
                        f'<div class="rtl-preview">{(full_text[:5000] if full_text else "(empty)")}</div>',
                        unsafe_allow_html=True,
                    )

# ── Chunks ───────────────────────────────────────────────────────────────

with tab_chunks:
    try:
        total_chunks = q_scalar("SELECT COUNT(*) FROM chunks")
    except Exception:
        total_chunks = 0

    col_m, col_f, col_r = st.columns([1, 2, 1])
    col_m.metric("Total Chunks", f"{total_chunks:,}")
    chunk_att_filter = col_f.text_input("Filter by attachment_id", key="chunk_att_filter")
    if col_r.button("Refresh", key="refresh_chunks"):
        st.rerun()

    chunk_page = st.number_input("Page", min_value=1, value=1, step=1, key="chunk_page")
    chunk_offset = (chunk_page - 1) * PAGE_SIZE

    if chunk_att_filter:
        try:
            att_id = int(chunk_att_filter)
            chunks_df = q(
                """SELECT id, attachment_id, page_number, chunk_index, word_count
                   FROM chunks WHERE attachment_id = ? ORDER BY chunk_index LIMIT ? OFFSET ?""",
                (att_id, PAGE_SIZE, chunk_offset),
            )
        except ValueError:
            st.warning("Enter a valid integer attachment_id")
            chunks_df = pd.DataFrame()
    else:
        chunks_df = q(
            """SELECT id, attachment_id, page_number, chunk_index, word_count
               FROM chunks ORDER BY id DESC LIMIT ? OFFSET ?""",
            (PAGE_SIZE, chunk_offset),
        )

    st.dataframe(chunks_df, use_container_width=True, hide_index=True)

# ── Embeddings ───────────────────────────────────────────────────────────

with tab_embeddings:
    try:
        total_emb = q_scalar("SELECT COUNT(*) FROM embeddings")
    except Exception:
        total_emb = 0

    col_m, _, col_r = st.columns([1, 3, 1])
    col_m.metric("Total Embeddings", f"{total_emb:,}")
    if col_r.button("Refresh", key="refresh_emb"):
        st.rerun()

    emb_page = st.number_input("Page", min_value=1, value=1, step=1, key="emb_page")
    emb_offset = (emb_page - 1) * PAGE_SIZE

    emb_df = q(
        """SELECT e.id, e.chunk_id, e.model_name, e.dimensions, e.embedded_at
           FROM embeddings e ORDER BY e.id DESC LIMIT ? OFFSET ?""",
        (PAGE_SIZE, emb_offset),
    )
    st.dataframe(emb_df, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Auto-refresh while any pipeline is running (must be LAST so UI renders first)
# ---------------------------------------------------------------------------

if any(st.session_state.get(f"{s}_status") == "running" for s in PIPELINE_STAGES):
    time.sleep(1.5)
    st.rerun()
