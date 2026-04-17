"""
IR 文書 PDF のテキスト抽出とチャンク分割（Phase B Task 1）。

Phase B Task 2 以降で Cloud Vision OCR・Embedding・DB 連携を追加。
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF


def extract_text_pymupdf(pdf_path: str | Path) -> tuple[str, int]:
    """PyMuPDF で PDF 全ページのテキストを抽出する。

    Returns:
        (連結テキスト, ページ数)
    """
    path = Path(pdf_path)
    doc = fitz.open(path)
    try:
        pages_text: list[str] = []
        for page in doc:
            pages_text.append(page.get_text("text"))
        return "\n".join(pages_text), len(pages_text)
    finally:
        doc.close()


def is_ocr_needed(text: str, page_count: int, threshold: int = 100) -> bool:
    """1 ページあたりの平均文字数が threshold 未満なら OCR が必要とみなす。"""
    if page_count <= 0:
        return True
    avg_chars = len(text.strip()) / page_count
    return avg_chars < threshold


def chunk_text(
    text: str,
    chunk_size: int = 800,
    overlap: int = 100,
) -> list[dict[str, Any]]:
    """テキストを固定長チャンクに分割する。

    Returns:
        [{"chunk_index", "text", "chunk_start", "chunk_end"}, ...]
    """
    if chunk_size <= 0:
        return []
    step = max(1, chunk_size - overlap)
    chunks: list[dict[str, Any]] = []
    start = 0
    idx = 0
    n = len(text)
    while start < n:
        end = min(start + chunk_size, n)
        chunks.append(
            {
                "chunk_index": idx,
                "text": text[start:end],
                "chunk_start": start,
                "chunk_end": end,
            }
        )
        idx += 1
        if end >= n:
            break
        start += step
    return chunks


def content_hash(text: str) -> str:
    """テキストの SHA-256 ヘキスト（重複検知・再処理判定用）。"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
