"""Extract readable text from uploaded files for orchestrator context injection."""

import io

import pdfplumber

_MAX_CHARS = 50_000
_TRUNCATION_SUFFIX = "\n\n[... truncated at 50,000 characters]"

# Extensions that can be decoded as UTF-8 text
_TEXT_EXTENSIONS = {".txt", ".csv", ".json"}


def _truncate(text: str) -> str:
    """Apply 50,000-character truncation if needed."""
    if len(text) > _MAX_CHARS:
        return text[:_MAX_CHARS] + _TRUNCATION_SUFFIX
    return text


def _extract_pdf(content: bytes) -> str | None:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            pages = [page.extract_text() or "" for page in pdf.pages]
        text = "\n".join(pages).strip()
        if not text:
            return None
        return _truncate(text)
    except Exception:
        return None


def extract_text(filename: str, content: bytes) -> str | None:
    """Extract text from a file's raw bytes.

    Supports ``.txt``, ``.csv``, ``.json``, and ``.pdf`` files.  Returns
    ``None`` for unsupported extensions or if decoding/extraction fails.

    Args:
        filename: Original filename (used for extension check).
        content: Raw file bytes.

    Returns:
        Extracted text (truncated to 50,000 characters if needed), or None.
    """
    # Check extension
    lower = filename.lower()
    dot_idx = lower.rfind(".")
    if dot_idx == -1:
        return None

    ext = lower[dot_idx:]

    if ext == ".pdf":
        return _extract_pdf(content)

    if ext not in _TEXT_EXTENSIONS:
        return None

    try:
        text = content.decode("utf-8")
    except (UnicodeDecodeError, ValueError):
        return None

    return _truncate(text)
