"""Tests for src.uploads.text_extractor — text extraction from uploaded files."""

import io

import pdfplumber
import pytest
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

from src.uploads.text_extractor import extract_text


def _make_pdf(*pages: str) -> bytes:
    """Create a minimal PDF with one text string per page."""
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    for page_text in pages:
        c.drawString(72, 720, page_text)
        c.showPage()
    c.save()
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Supported text formats
# ---------------------------------------------------------------------------


class TestTextExtraction:
    """UTF-8 text files (.txt, .csv, .json) are extracted successfully."""

    def test_txt_extraction(self) -> None:
        content = b"Hello, this is a plain text file."
        result = extract_text("readme.txt", content)
        assert result == "Hello, this is a plain text file."

    def test_csv_extraction(self) -> None:
        content = b"hour,kwh\n0,100\n1,200\n"
        result = extract_text("data.csv", content)
        assert result == "hour,kwh\n0,100\n1,200\n"

    def test_json_extraction(self) -> None:
        content = b'{"key": "value"}'
        result = extract_text("config.json", content)
        assert result == '{"key": "value"}'

    def test_uppercase_extension(self) -> None:
        result = extract_text("NOTES.TXT", b"some text")
        assert result == "some text"

    def test_mixed_case_extension(self) -> None:
        result = extract_text("Data.Csv", b"a,b\n1,2\n")
        assert result == "a,b\n1,2\n"


# ---------------------------------------------------------------------------
# Unsupported formats → None
# ---------------------------------------------------------------------------


class TestUnsupportedFormats:
    """Non-text files return None."""

    @pytest.mark.parametrize(
        "filename",
        ["doc.docx", "image.png", "archive.zip", "binary.exe"],
    )
    def test_unsupported_extension_returns_none(self, filename: str) -> None:
        result = extract_text(filename, b"some bytes")
        assert result is None

    def test_no_extension_returns_none(self) -> None:
        result = extract_text("Makefile", b"all: build")
        assert result is None


# ---------------------------------------------------------------------------
# Truncation
# ---------------------------------------------------------------------------


class TestTruncation:
    """Files exceeding 50,000 characters are truncated."""

    def test_exactly_50k_chars_no_truncation(self) -> None:
        content = ("x" * 50_000).encode("utf-8")
        result = extract_text("big.txt", content)
        assert result is not None
        assert len(result) == 50_000
        assert "[... truncated" not in result

    def test_50001_chars_truncated(self) -> None:
        content = ("x" * 50_001).encode("utf-8")
        result = extract_text("big.txt", content)
        assert result is not None
        assert result.endswith("[... truncated at 50,000 characters]")
        # First 50k chars preserved + suffix
        assert result[:50_000] == "x" * 50_000

    def test_large_file_truncated(self) -> None:
        content = ("a" * 100_000).encode("utf-8")
        result = extract_text("huge.csv", content)
        assert result is not None
        assert "[... truncated at 50,000 characters]" in result


# ---------------------------------------------------------------------------
# Decode errors → None
# ---------------------------------------------------------------------------


class TestDecodeErrors:
    """Invalid UTF-8 bytes return None."""

    def test_invalid_utf8_returns_none(self) -> None:
        # Invalid UTF-8 sequence
        content = b"\xff\xfe\x00\x01"
        result = extract_text("bad.txt", content)
        assert result is None

    def test_empty_file_returns_empty_string(self) -> None:
        result = extract_text("empty.txt", b"")
        assert result == ""


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------


class TestPdfExtraction:
    """PDF files are extracted via pdfplumber."""

    def test_single_page_pdf(self) -> None:
        pdf_bytes = _make_pdf("Hello from a PDF")
        result = extract_text("report.pdf", pdf_bytes)
        assert result is not None
        assert "Hello from a PDF" in result

    def test_multi_page_pdf(self) -> None:
        pdf_bytes = _make_pdf("Page one text", "Page two text")
        result = extract_text("report.pdf", pdf_bytes)
        assert result is not None
        assert "Page one text" in result
        assert "Page two text" in result

    def test_uppercase_pdf_extension(self) -> None:
        pdf_bytes = _make_pdf("Case test")
        result = extract_text("REPORT.PDF", pdf_bytes)
        assert result is not None
        assert "Case test" in result

    def test_corrupt_pdf_returns_none(self) -> None:
        result = extract_text("bad.pdf", b"this is not a pdf at all")
        assert result is None

    def test_empty_pdf_returns_none(self) -> None:
        # A valid PDF with no text content (blank page)
        buf = io.BytesIO()
        c = canvas.Canvas(buf, pagesize=letter)
        c.showPage()
        c.save()
        result = extract_text("blank.pdf", buf.getvalue())
        assert result is None

    def test_pdf_truncation(self) -> None:
        # Create a PDF with text exceeding 50k characters
        long_text = "x" * 51_000
        pdf_bytes = _make_pdf(long_text)
        result = extract_text("big.pdf", pdf_bytes)
        assert result is not None
        assert result.endswith("[... truncated at 50,000 characters]")
