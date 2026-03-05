"""Tests for pipeline.indexer — form content serialization and chunk preparation."""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

# Ensure src/ is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeline.indexer import _serialize_form_content, _prepare_report_chunks
from pipeline.form_configs import get_config


# ---------------------------------------------------------------------------
# _serialize_form_content
# ---------------------------------------------------------------------------

class TestSerializeFormContent:
    """Test the form content serialization helper."""

    def test_scalar_fields_included(self):
        form_fields = {
            "fields": {
                "ShareholderName": "Acme Corp Ltd",
                "SharePercent": "15.5",
            },
        }
        config = {"narrative_fields": [], "skip_fields": [], "metadata_fields": []}
        result = _serialize_form_content(form_fields, config)
        assert "ShareholderName: Acme Corp Ltd" in result
        assert "SharePercent: 15.5" in result

    def test_excluded_fields_skipped(self):
        form_fields = {
            "fields": {
                "TextHofshi": "some narrative text",
                "ShareholderName": "Acme Corp",
                "Shem": "skip me",
            },
        }
        config = {
            "narrative_fields": ["TextHofshi"],
            "skip_fields": ["Shem"],
            "metadata_fields": [],
        }
        result = _serialize_form_content(form_fields, config)
        assert "TextHofshi" not in result
        assert "Shem" not in result
        assert "ShareholderName: Acme Corp" in result

    def test_tables_serialized(self):
        form_fields = {
            "fields": {},
            "tables": {
                "Shareholders": [
                    {"Name": "Alice", "Percent": "10"},
                    {"Name": "Bob", "Percent": "20"},
                ],
            },
        }
        config = {"narrative_fields": [], "skip_fields": [], "metadata_fields": []}
        result = _serialize_form_content(form_fields, config)
        assert "[Shareholders]" in result
        assert "Name: Alice" in result
        assert "Percent: 20" in result

    def test_empty_form_fields(self):
        result = _serialize_form_content({}, {"narrative_fields": [], "skip_fields": [], "metadata_fields": []})
        assert result == ""

    def test_short_values_skipped(self):
        form_fields = {
            "fields": {"A": "x", "B": "  ", "C": "ok value"},
        }
        config = {"narrative_fields": [], "skip_fields": [], "metadata_fields": []}
        result = _serialize_form_content(form_fields, config)
        assert "A:" not in result
        assert "B:" not in result
        assert "C: ok value" in result

    def test_std_fields_included(self):
        form_fields = {
            "fields": {},
            "std_fields": {"CompanyName": "Test Company"},
        }
        config = {"narrative_fields": [], "skip_fields": [], "metadata_fields": []}
        result = _serialize_form_content(form_fields, config)
        assert "CompanyName: Test Company" in result

    def test_t077_config_excludes_only_metadata(self):
        """ת077 has no narrative_fields and no skip_fields — form content should pass through."""
        config = get_config("ת077")
        form_fields = {
            "fields": {
                "TaarichDivuach": "2024-01-15",  # metadata — excluded
                "ShareholderName": "Test Holder",
                "SharePercent": "5.0",
            },
            "tables": {
                "BaaleiMenayot": [
                    {"Name": "Holder A", "Shares": "1000", "Percent": "5.0"},
                ],
            },
        }
        result = _serialize_form_content(form_fields, config)
        assert "TaarichDivuach" not in result  # metadata field excluded
        assert "ShareholderName: Test Holder" in result
        assert "Holder A" in result


# ---------------------------------------------------------------------------
# _prepare_report_chunks — integration-style tests with mocked DB
# ---------------------------------------------------------------------------

def _make_report(**overrides):
    """Build a minimal report dict."""
    base = {
        "id": 1,
        "reference_number": "2024-01-00012345",
        "report_date": "2024-06-15",
        "company_id": "520",
        "company_name": "Test Company Ltd",
        "form_type": "ת077",
        "form_name": "דוח שינויים בהון",
        "subject": "Test subject line",
        "form_fields": None,
    }
    base.update(overrides)
    return base


def _mock_db(extracted_atts=None):
    db = MagicMock()
    db.get_extracted_attachments_for_report.return_value = extracted_atts or []
    return db


class TestPrepareReportChunks:
    """Test _prepare_report_chunks with form content and metadata fallback."""

    def test_form_content_produces_chunks(self):
        """Report with tables but no narrative_fields should produce form content chunks."""
        form_fields = {
            "fields": {
                "ShareholderName": "Test Holder with enough words to pass minimum",
            },
            "tables": {
                "BaaleiMenayot": [
                    {"Name": f"Holder {i}", "Shares": str(i * 100), "Percent": f"{i}.0"}
                    for i in range(1, 15)  # enough rows to exceed MIN_PAGE_WORDS
                ],
            },
        }
        report = _make_report(form_fields=json.dumps(form_fields))
        db = _mock_db()

        texts, payloads, att_ids = _prepare_report_chunks(db, report)

        assert len(texts) > 0, "Should produce at least one chunk from form content"
        # All should be form source_type
        form_chunks = [p for p in payloads if p["field_name"] == "_form_content"]
        assert len(form_chunks) > 0
        assert form_chunks[0]["source_type"] == "form"
        assert form_chunks[0]["page_number"] == 1

    def test_metadata_fallback_when_empty(self):
        """Report with no content at all should produce a metadata fallback point."""
        report = _make_report(form_fields=json.dumps({"fields": {}, "tables": {}}))
        db = _mock_db()

        texts, payloads, att_ids = _prepare_report_chunks(db, report)

        assert len(texts) == 1, "Should produce exactly one metadata fallback"
        assert payloads[0]["field_name"] == "_metadata"
        assert payloads[0]["source_type"] == "form"
        assert payloads[0]["page_number"] == 1
        assert "Test Company Ltd" in payloads[0]["chunk_text"]
        assert "ת077" in payloads[0]["chunk_text"]

    def test_metadata_fallback_with_null_form_fields(self):
        """Report with NULL form_fields should produce metadata fallback."""
        report = _make_report(form_fields=None)
        db = _mock_db()

        texts, payloads, att_ids = _prepare_report_chunks(db, report)

        assert len(texts) == 1
        assert payloads[0]["field_name"] == "_metadata"

    def test_narrative_fields_still_work(self):
        """Reports with narrative content should still produce narrative chunks (existing behavior)."""
        long_text = " ".join(["word"] * 50)  # 50 words, well above MIN_PAGE_WORDS
        form_fields = {
            "fields": {"TextHofshi": long_text},
        }
        report = _make_report(
            form_type="ת053",
            form_fields=json.dumps(form_fields),
        )
        db = _mock_db()

        texts, payloads, att_ids = _prepare_report_chunks(db, report)

        narrative_chunks = [p for p in payloads if p["field_name"] == "TextHofshi"]
        assert len(narrative_chunks) > 0, "Narrative field should produce chunks"

    def test_no_duplicate_content_between_narrative_and_form(self):
        """Fields in narrative_fields should NOT appear in _form_content chunks."""
        long_text = " ".join(["word"] * 50)
        form_fields = {
            "fields": {
                "TextHofshi": long_text,
                "OtherField": " ".join(["data"] * 20),
            },
        }
        report = _make_report(
            form_type="ת053",
            form_fields=json.dumps(form_fields),
        )
        db = _mock_db()

        texts, payloads, att_ids = _prepare_report_chunks(db, report)

        form_content_chunks = [p for p in payloads if p["field_name"] == "_form_content"]
        for fc in form_content_chunks:
            # TextHofshi is a narrative field for ת053, should not appear in form content
            assert "TextHofshi" not in fc["chunk_text"]
