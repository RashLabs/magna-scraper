"""Tests for pipeline extractor — Libre2 integration."""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

import httpx
import pytest

# Patch sys.path so pipeline imports resolve
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeline.extractor import _extract_via_libre, extract_pages


# -- Fixtures ------------------------------------------------------------------

LIBRE_SUCCESS_RESPONSE = {
    "task_type": "pdf",
    "filename": "report.pdf",
    "success": True,
    "pages": [
        {"page": 1, "total_pages": 2, "word_count": 120, "content": "Page one text content here."},
        {"page": 2, "total_pages": 2, "word_count": 85, "content": "Page two text content."},
    ],
    "stats": {"total_pages": 2, "total_words": 205, "total_tokens": 0, "post_ocr": False},
    "error": None,
    "message": "",
    "metadata": {},
}

LIBRE_FAILURE_RESPONSE = {
    "task_type": "pdf",
    "filename": "bad.pdf",
    "success": False,
    "pages": [],
    "stats": {"total_pages": 0, "total_words": 0, "total_tokens": 0, "post_ocr": False},
    "error": {"type": "processing_error", "details": "PDF file has no pages"},
    "message": "",
    "metadata": {},
}


def _mock_httpx_response(json_body: dict, status_code: int = 200) -> httpx.Response:
    """Create a mock httpx.Response with the given JSON body."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_body
    resp.raise_for_status.return_value = None
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    return resp


# -- _extract_via_libre --------------------------------------------------------

class TestExtractViaLibre:

    @patch("pipeline.extractor.httpx.post")
    def test_success_maps_pages_correctly(self, mock_post, tmp_path):
        """Libre2 success response maps to expected page dict structure."""
        mock_post.return_value = _mock_httpx_response(LIBRE_SUCCESS_RESPONSE)

        pdf_file = tmp_path / "report.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 fake content")

        pages = _extract_via_libre(pdf_file)

        assert len(pages) == 2
        assert pages[0] == {"content": "Page one text content here.", "word_count": 120, "page_number": 1}
        assert pages[1] == {"content": "Page two text content.", "word_count": 85, "page_number": 2}

        # Verify POST was called to /extract
        call_args = mock_post.call_args
        assert "/extract" in call_args[0][0] or "/extract" in str(call_args)

    @patch("pipeline.extractor.httpx.post")
    def test_failure_raises_runtime_error(self, mock_post, tmp_path):
        """Libre2 success=false raises RuntimeError with error details."""
        mock_post.return_value = _mock_httpx_response(LIBRE_FAILURE_RESPONSE)

        pdf_file = tmp_path / "bad.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 fake")

        with pytest.raises(RuntimeError, match="PDF file has no pages"):
            _extract_via_libre(pdf_file)

    @patch("pipeline.extractor.httpx.post")
    def test_http_401_raises(self, mock_post, tmp_path):
        """401 from Libre2 (auth misconfigured) raises HTTPStatusError."""
        mock_post.return_value = _mock_httpx_response({}, status_code=401)

        pdf_file = tmp_path / "report.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 fake")

        with pytest.raises(httpx.HTTPStatusError):
            _extract_via_libre(pdf_file)

    @patch("pipeline.extractor.httpx.post")
    def test_connection_error_propagates(self, mock_post, tmp_path):
        """Connection error (Libre2 not running) propagates as-is."""
        mock_post.side_effect = httpx.ConnectError("Connection refused")

        pdf_file = tmp_path / "report.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 fake")

        with pytest.raises(httpx.ConnectError):
            _extract_via_libre(pdf_file)


# -- extract_pages -------------------------------------------------------------

class TestExtractPages:

    @patch("pipeline.extractor._extract_via_libre")
    def test_pdf_delegates_to_libre(self, mock_libre, tmp_path):
        """PDF files are routed to _extract_via_libre."""
        mock_libre.return_value = [{"content": "text", "word_count": 1, "page_number": 1}]

        pdf_file = tmp_path / "test.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 fake")

        with patch("pipeline.extractor.PROJECT_ROOT", tmp_path):
            pages = extract_pages("test.pdf")

        assert pages == [{"content": "text", "word_count": 1, "page_number": 1}]
        mock_libre.assert_called_once()

    def test_txt_utf8(self, tmp_path):
        """TXT files are read locally with UTF-8."""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("Hello world from text file", encoding="utf-8")

        with patch("pipeline.extractor.PROJECT_ROOT", tmp_path):
            pages = extract_pages("test.txt")

        assert len(pages) == 1
        assert pages[0]["content"] == "Hello world from text file"
        assert pages[0]["word_count"] == 5
        assert pages[0]["page_number"] == 1

    def test_txt_windows1255_fallback(self, tmp_path):
        """TXT files fall back to Windows-1255 encoding for Hebrew."""
        txt_file = tmp_path / "hebrew.txt"
        txt_file.write_bytes("שלום עולם".encode("windows-1255"))

        with patch("pipeline.extractor.PROJECT_ROOT", tmp_path):
            pages = extract_pages("hebrew.txt")

        assert len(pages) == 1
        assert pages[0]["word_count"] == 2

    def test_unsupported_extension_raises(self, tmp_path):
        """Unsupported file types raise ValueError."""
        docx_file = tmp_path / "test.docx"
        docx_file.write_bytes(b"fake")

        with patch("pipeline.extractor.PROJECT_ROOT", tmp_path):
            with pytest.raises(ValueError, match="Unsupported file type"):
                extract_pages("test.docx")

    def test_missing_file_raises(self, tmp_path):
        """Missing file raises FileNotFoundError."""
        with patch("pipeline.extractor.PROJECT_ROOT", tmp_path):
            with pytest.raises(FileNotFoundError):
                extract_pages("nonexistent.pdf")
