"""PyMuPDF 抽出・チャンク・OCR 判定のテスト."""

from __future__ import annotations

from pathlib import Path

import fitz
import pytest

from ir_research.pdf_processor import (
    chunk_text,
    content_hash,
    extract_text_pymupdf,
    is_ocr_needed,
)


@pytest.fixture
def dummy_pdf_path(tmp_path: Path) -> Path:
    """最小 PDF（PyMuPDF で生成。CJK はフォント依存のため ASCII で検証）。"""
    p = tmp_path / "sample.pdf"
    doc = fitz.open()
    try:
        page = doc.new_page()
        page.insert_text((72, 72), "IR_SAMPLE_YUHO_REPORT_TEXT")
        doc.save(str(p))
    finally:
        doc.close()
    return p


def test_extract_text_pymupdf_returns_text(dummy_pdf_path: Path) -> None:
    text, page_count = extract_text_pymupdf(dummy_pdf_path)
    assert len(text) > 0
    assert page_count >= 1
    assert "YUHO" in text


def test_chunk_text_splits_correctly() -> None:
    text = "あ" * 2000
    chunks = chunk_text(text, chunk_size=500, overlap=50)
    assert len(chunks) >= 4
    assert all(len(c["text"]) <= 500 for c in chunks)
    assert chunks[0]["chunk_index"] == 0
    assert chunks[1]["chunk_index"] == 1


def test_chunk_text_preserves_order() -> None:
    text = "セクションA。" * 100 + "セクションB。" * 100
    chunks = chunk_text(text, chunk_size=200, overlap=20)
    combined = "".join(c["text"] for c in chunks)
    assert "セクションA" in combined
    assert "セクションB" in combined


def test_is_ocr_needed() -> None:
    assert is_ocr_needed("", 1) is True
    assert is_ocr_needed("   ", 0) is True
    assert is_ocr_needed("x" * 50, 1, threshold=100) is True
    assert is_ocr_needed("x" * 150, 1, threshold=100) is False


def test_content_hash_deterministic() -> None:
    assert content_hash("abc") == content_hash("abc")
    assert content_hash("a") != content_hash("b")
