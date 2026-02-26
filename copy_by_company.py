"""
Copy downloaded PDF attachments into per-company subfolders.

Usage:
    python copy_by_company.py [--dest DEST_DIR] [--company COMPANY_NAME] [--dry-run]

Defaults:
    --dest  data/by_company
"""

import argparse
import shutil
import sqlite3
import sys
from pathlib import Path

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding="utf-8")

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "data" / "magna_v2.db"


def sanitize_folder_name(name: str) -> str:
    """Replace characters that are invalid in Windows folder names."""
    for ch in r'\/:*?"<>|':
        name = name.replace(ch, "_")
    return name.strip()


def resolve_company_filter(db_path: Path, prefix: str) -> str:
    """Resolve a prefix to a full company name. Raise if ambiguous or no match."""
    conn = sqlite3.connect(str(db_path))
    rows = conn.execute(
        "SELECT DISTINCT company_name FROM reports WHERE company_name LIKE ?",
        (prefix + "%",),
    ).fetchall()
    conn.close()
    names = [r[0] for r in rows]
    if len(names) == 0:
        raise SystemExit(f"No company matching '{prefix}*'")
    if len(names) == 1:
        return names[0]
    # Multiple matches — show them and exit
    listing = "\n".join(f"  - {n}" for n in names)
    raise SystemExit(f"'{prefix}' matches {len(names)} companies — be more specific:\n{listing}")


def get_pdf_company_map(db_path: Path, company_name: str | None = None):
    """Return list of (company_name, form_type, form_name, local_path) for downloaded PDFs."""
    conn = sqlite3.connect(str(db_path))
    query = """
        SELECT r.company_name, r.form_type, r.form_name, a.local_path
        FROM attachments a
        JOIN reports r ON a.report_id = r.id
        WHERE a.download_status = 'downloaded'
          AND a.local_path IS NOT NULL
          AND a.local_path LIKE '%.pdf'
    """
    params = []
    if company_name:
        query += " AND r.company_name = ?"
        params.append(company_name)
    query += " ORDER BY r.company_name"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return rows


def main():
    parser = argparse.ArgumentParser(description="Copy PDFs into per-company folders")
    parser.add_argument("--dest", default="data/by_company",
                        help="Destination root folder (default: data/by_company)")
    parser.add_argument("--company", default=None,
                        help="Company name or prefix (e.g. 'אל על' or 'אל')")
    parser.add_argument("--by-form", action="store_true",
                        help="Create subfolders per form_type (e.g. ת053)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be copied without copying")
    args = parser.parse_args()

    dest_root = (PROJECT_ROOT / args.dest).resolve()
    company_name = None
    if args.company:
        company_name = resolve_company_filter(DB_PATH, args.company)
        print(f"Company: {company_name}")
    rows = get_pdf_company_map(DB_PATH, company_name)

    if not rows:
        print("No matching PDFs found.")
        return

    copied = 0
    skipped = 0
    missing = 0

    for company_name, form_type, form_name, local_path in rows:
        src = PROJECT_ROOT / local_path
        if not src.exists():
            missing += 1
            continue

        company_folder = dest_root / sanitize_folder_name(company_name)
        if args.by_form:
            form_label = "\u200e" + (form_type or "unknown")
            if form_name:
                form_label += " - " + form_name[:50].strip()
            company_folder = company_folder / sanitize_folder_name(form_label)
        # Prepend reference_number to filename to avoid collisions
        ref_number = Path(local_path).parent.name
        dst = company_folder / f"{ref_number}_{src.name}"

        if dst.exists():
            skipped += 1
            continue

        if args.dry_run:
            print(f"  {src}  ->  {dst}")
            copied += 1
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied += 1

    action = "Would copy" if args.dry_run else "Copied"
    print(f"{action}: {copied}  |  Skipped (exists): {skipped}  |  Missing source: {missing}")


if __name__ == "__main__":
    main()
