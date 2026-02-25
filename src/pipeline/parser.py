"""Stage 2: Parse form HTML to extract FieldAlias elements into structured JSON.

Uses regex-based extraction instead of HTMLParser to handle MAGNA's mixed
quoting styles (quoted and unquoted attributes) and nested tags reliably.
"""

import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db_v2 import Database
from pipeline.form_configs import get_category

log = logging.getLogger(__name__)

EMPTY_SENTINEL = "_________"  # 9 underscores — MAGNA empty field marker

# Regex to find elements with FieldAlias attribute (handles quoted and unquoted values)
# Captures: (tag_name, full_attributes, inner_content_if_not_self_closing)
FIELD_ALIAS_RE = re.compile(
    r'<(\w+)\s+([^>]*?(?:fieldalias|std-fieldalias)[^>]*?)(?:/>|>([\s\S]*?)</\1>)',
    re.IGNORECASE,
)

# Extract attribute value — handles: attr="val", attr='val', attr=val, attr (boolean)
def _get_attr(attrs_str: str, attr_name: str) -> str | None:
    """Extract an attribute value from an HTML attributes string."""
    # Quoted: attr="value" or attr='value'
    m = re.search(
        rf'(?i)\b{re.escape(attr_name)}\s*=\s*["\']([^"\']*)["\']',
        attrs_str,
    )
    if m:
        return m.group(1)
    # Unquoted: attr=value (ends at space or >)
    m = re.search(
        rf'(?i)\b{re.escape(attr_name)}\s*=\s*([^\s>]+)',
        attrs_str,
    )
    if m:
        return m.group(1)
    return None


def _strip_tags(html: str) -> str:
    """Remove HTML tags, keeping text content."""
    return re.sub(r'<[^>]+>', '', html).strip()


def _extract_fields(html: str) -> list[tuple[str, str, str]]:
    """Extract all FieldAlias elements from HTML.

    Returns list of (alias, value, type) where type is 'field' or 'std'.
    Handles:
    - <input> / <select>: reads value attribute; for select, also checks for selected option
    - <span> / <textarea> / <div>: reads inner text content (strips nested tags)
    - Both quoted and unquoted attribute values
    """
    results = []

    for m in FIELD_ALIAS_RE.finditer(html):
        tag = m.group(1).lower()
        attrs_str = m.group(2)
        inner = m.group(3)  # None for self-closing tags

        alias = _get_attr(attrs_str, "fieldalias")
        std_alias = _get_attr(attrs_str, "std-fieldalias")

        if not alias and not std_alias:
            continue

        # Determine value based on tag type
        if tag in ("input",):
            value = _get_attr(attrs_str, "value") or ""
        elif tag == "select":
            # Try to find selected option in inner content
            value = _get_attr(attrs_str, "value") or ""
            if inner:
                sel_match = re.search(r'<option[^>]+selected[^>]*>([^<]*)</option>', inner, re.IGNORECASE)
                if sel_match:
                    value = sel_match.group(1).strip()
                elif not value:
                    # Fallback: first option with a value
                    opt_match = re.search(r'<option[^>]+value=["\']?([^"\'>\s]+)', inner, re.IGNORECASE)
                    if opt_match:
                        value = opt_match.group(1)
        else:
            # span, textarea, div, etc. — extract inner text
            value = _strip_tags(inner) if inner else ""

        if alias:
            results.append((alias, value, "field"))
        if std_alias:
            results.append((std_alias, value, "std"))

    return results


# Pattern for Row{N}_{FieldName}
ROW_PATTERN = re.compile(r"^Row(\d+)_(.+)$")

# Pattern to detect repeated-alias table sections (same alias appearing many times)
# Used for forms like ת077 where rows don't use Row{N}_ prefix


def parse_form_html(html: str) -> dict:
    """Parse HTML and return structured form_fields JSON.

    Returns:
        {
            "fields": {alias: value, ...},
            "tables": {table_name: [{field: value}, ...], ...},
            "std_fields": {alias: value, ...}
        }
    """
    raw_fields = _extract_fields(html)

    fields: dict[str, str] = {}
    tables: dict[str, dict[int, dict[str, str]]] = {}
    std_fields: dict[str, str] = {}

    # Count occurrences per alias to detect repeated fields (table rows without Row prefix)
    alias_counts: dict[str, int] = {}
    for alias, value, ftype in raw_fields:
        if ftype == "field":
            alias_counts[alias] = alias_counts.get(alias, 0) + 1

    # Aliases that appear many times are likely table columns
    repeated_aliases = {a for a, c in alias_counts.items() if c > 3}

    # Track per-alias occurrence index for repeated fields
    alias_occurrence: dict[str, int] = {}

    for alias, value, ftype in raw_fields:
        # Skip empties
        if not value or value.strip() == "" or value.strip() == EMPTY_SENTINEL:
            continue

        if ftype == "std":
            std_fields[alias] = value
            continue

        # Check for Row{N}_ prefix pattern
        m = ROW_PATTERN.match(alias)
        if m:
            row_num = int(m.group(1))
            field_name = m.group(2)
            table_name = "rows"
            if table_name not in tables:
                tables[table_name] = {}
            if row_num not in tables[table_name]:
                tables[table_name][row_num] = {}
            tables[table_name][row_num][field_name] = value
        elif alias in repeated_aliases:
            # Repeated alias without Row prefix — group into "repeated" table
            occ = alias_occurrence.get(alias, 0)
            alias_occurrence[alias] = occ + 1
            table_name = "repeated"
            if table_name not in tables:
                tables[table_name] = {}
            if occ not in tables[table_name]:
                tables[table_name][occ] = {}
            tables[table_name][occ][alias] = value
        else:
            # Regular field — last non-empty value wins
            fields[alias] = value

    # Convert tables from {row_num -> dict} to sorted list of dicts
    tables_list = {}
    for tname, rows in tables.items():
        tables_list[tname] = [rows[k] for k in sorted(rows.keys())]

    return {
        "fields": fields,
        "tables": tables_list,
        "std_fields": std_fields,
    }


def run(reprocess: bool = False, since: str = "", cancel_check=None, progress_cb=None):
    """Parse all reports that have HTML but haven't been parsed yet.
    When reprocess=True, re-parse all reports with HTML."""
    db = Database()
    reports = db.get_reports_needing_parse(reprocess=reprocess, since=since)
    total = len(reports)

    if not reports:
        log.info("No reports to parse.")
        db.close()
        return

    log.info(f"Parsing {total} reports...")
    parsed = 0
    errors = 0

    for i, report in enumerate(reports):
        if cancel_check and cancel_check():
            log.info("Parse cancelled.")
            break

        try:
            result = parse_form_html(report["form_html"])
            category = get_category(report["form_type"] or "")
            form_fields_json = json.dumps(result, ensure_ascii=False)
            db.set_form_fields(report["id"], form_fields_json, category)
            parsed += 1

            if parsed % 100 == 0 or parsed == total:
                log.info(f"  Parsed {parsed}/{total}")

        except Exception as e:
            errors += 1
            log.warning(f"  Failed to parse report {report['reference_number']}: {e}")

        if progress_cb:
            progress_cb(parsed + errors, total)

    log.info(f"Parse complete: {parsed} parsed, {errors} errors")
    db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
    run()
