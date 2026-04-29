"""
IR 文書 PDF のテキスト抽出 →（必要時 OCR）→ チャンク → DB 保存。

DB 負荷対策:
- ir_chunks への INSERT は embedding 計算後に 1 回だけ batch INSERT
- 1 行ずつ UPDATE は禁止（HNSW rebuild が重い）
- content_hash で冪等処理（同一 hash ならスキップ）
- chunk_size=1500, overlap=200 で chunk 数を削減
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF
from google.cloud import documentai_v1 as documentai
from openai import AsyncOpenAI


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
    chunk_size: int = 1500,
    overlap: int = 200,
) -> list[dict[str, Any]]:
    """テキストを固定長チャンクに分割する。

    chunk_size=1500, overlap=200 で従来（800/100）比 chunk 数を約半減。
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


def extract_text_document_ai(
    pdf_bytes: bytes,
    project_id: str | None = None,
    location: str = "us",
    processor_id: str | None = None,
) -> str:
    """Google Document AI で PDF を OCR。"""
    project_id = project_id or os.environ.get("GOOGLE_CLOUD_PROJECT")
    processor_id = processor_id or os.environ.get("DOCUMENT_AI_PROCESSOR_ID")

    client = documentai.DocumentProcessorServiceClient()
    resource_name = client.processor_path(project_id, location, processor_id)

    raw_document = documentai.RawDocument(
        content=pdf_bytes, mime_type="application/pdf"
    )
    request = documentai.ProcessRequest(name=resource_name, raw_document=raw_document)

    result = client.process_document(request=request)
    doc = result.document
    if not doc:
        return ""
    return doc.text or ""


def _check_hash_skip(supabase: Any, document_id: int, new_hash: str) -> bool:
    """既存の content_hash と一致すればスキップ（冪等処理）。"""
    res = (
        supabase.table("ir_documents")
        .select("content_hash")
        .eq("id", document_id)
        .limit(1)
        .execute()
    )
    if res.data and res.data[0].get("content_hash") == new_hash:
        return True
    return False


def process_pdf(
    pdf_path: str | Path,
    supabase: Any,
    document_id: int,
    force_ocr: bool = False,
) -> dict[str, Any]:
    """PyMuPDF で抽出し、不足時は Document AI にフォールバック。

    注意: ir_chunks への INSERT はしない（process_pdf_with_embedding で一括投入）。
    """
    path = Path(pdf_path)
    text, page_count = extract_text_pymupdf(path)
    method = "pymupdf"

    if force_ocr or is_ocr_needed(text, page_count):
        pdf_bytes = path.read_bytes()
        text = extract_text_document_ai(pdf_bytes)
        method = "document_ai"

    c_hash = content_hash(text)

    # 冪等チェック: 同一 hash ならスキップ
    if _check_hash_skip(supabase, document_id, c_hash):
        return {
            "text_length": len(text),
            "page_count": page_count,
            "chunk_count": 0,
            "method": method,
            "skipped": True,
        }

    chunks = chunk_text(text)

    # ir_documents のメタ更新のみ（テキストは保存しない = DB 軽量化）
    supabase.table("ir_documents").update(
        {
            "extraction_method": method,
            "extraction_status": "完了",
            "content_hash": c_hash,
            "page_count": page_count,
        }
    ).eq("id", document_id).execute()

    return {
        "text_length": len(text),
        "page_count": page_count,
        "chunk_count": len(chunks),
        "method": method,
        "skipped": False,
        "_chunks": chunks,  # process_pdf_with_embedding で使う
    }


async def generate_embeddings(
    chunks: list[dict[str, Any]],
    model: str = "text-embedding-3-small",
    batch_size: int = 100,
) -> list[dict[str, Any]]:
    """チャンクに OpenAI embedding を付与して返す（元の dict に ``embedding`` を追加）。"""
    if not chunks:
        return []

    client = AsyncOpenAI()
    result: list[dict[str, Any]] = []

    for i in range(0, len(chunks), batch_size):
        batch = chunks[i : i + batch_size]
        texts = [c["text"] for c in batch]
        response = await client.embeddings.create(input=texts, model=model)
        for c, emb_data in zip(batch, response.data):
            result.append({**c, "embedding": emb_data.embedding})

    return result


async def process_pdf_with_embedding(
    pdf_path: str | Path,
    supabase: Any,
    document_id: int,
    force_ocr: bool = False,
) -> dict[str, Any]:
    """テキスト抽出 → embedding 計算 → ir_chunks に batch INSERT 1 発。

    1 行ずつ UPDATE は行わない（HNSW rebuild 負荷を回避）。
    """
    stats = process_pdf(pdf_path, supabase, document_id, force_ocr=force_ocr)

    if stats.get("skipped"):
        stats["embeddings_generated"] = 0
        return stats

    chunks = stats.pop("_chunks", [])
    if not chunks:
        stats["embeddings_generated"] = 0
        return stats

    # 既存 chunks を削除（冪等処理のため）
    supabase.table("ir_chunks").delete().eq("document_id", document_id).execute()

    # embedding 計算
    embedded = await generate_embeddings(chunks)

    # embedding 付き batch INSERT（1 発）
    rows = [
        {
            "document_id": document_id,
            "chunk_index": e["chunk_index"],
            "chunk_text": e["text"],
            "chunk_start": e["chunk_start"],
            "chunk_end": e["chunk_end"],
            "embedding": e["embedding"],
            "embedding_model": "text-embedding-3-small",
        }
        for e in embedded
    ]
    # 100 件ずつ batch INSERT
    for i in range(0, len(rows), 100):
        supabase.table("ir_chunks").insert(rows[i : i + 100]).execute()

    stats["embeddings_generated"] = len(embedded)
    return stats
