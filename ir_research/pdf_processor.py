"""
IR 文書 PDF のテキスト抽出 →（必要時 OCR）→ チャンク → DB 保存（Phase B）。

Task 3 で Embedding を追加。
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
    """1 ページあたりの平均文字数が threshold 未満なら OCR が必要とみなす。

    True のとき ``process_pdf`` は Google Document AI（Layout Parser 想定）へフォールバックする。
    """
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


def extract_text_document_ai(
    pdf_bytes: bytes,
    project_id: str | None = None,
    location: str = "us",
    processor_id: str | None = None,
) -> str:
    """Google Document AI で PDF を OCR。Layout Parser プロセッサで表組み・レイアウト構造を保持。

    認証は ``GOOGLE_APPLICATION_CREDENTIALS`` 等、google-cloud 標準に従う。
    プロセッサ ID は ``DOCUMENT_AI_PROCESSOR_ID``、プロジェクトは ``GOOGLE_CLOUD_PROJECT`` で指定可能。
    """
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


def process_pdf(
    pdf_path: str | Path,
    supabase: Any,
    document_id: int,
    force_ocr: bool = False,
) -> dict[str, Any]:
    """PyMuPDF で抽出し、不足時は Document AI にフォールバックしてチャンク化し DB を更新する。

    Returns:
        text_length, page_count, chunk_count, method
    """
    path = Path(pdf_path)
    text, page_count = extract_text_pymupdf(path)
    method = "pymupdf"

    if force_ocr or is_ocr_needed(text, page_count):
        pdf_bytes = path.read_bytes()
        text = extract_text_document_ai(pdf_bytes)
        method = "document_ai"

    c_hash = content_hash(text)
    chunks = chunk_text(text)

    supabase.table("ir_documents").update(
        {
            "markdown_content": text,
            "extraction_method": method,
            "extraction_status": "完了",
            "content_hash": c_hash,
            "page_count": page_count,
        }
    ).eq("id", document_id).execute()

    supabase.table("ir_chunks").delete().eq("document_id", document_id).execute()

    chunk_rows = [
        {
            "document_id": document_id,
            "chunk_index": c["chunk_index"],
            "chunk_text": c["text"],
            "chunk_start": c["chunk_start"],
            "chunk_end": c["chunk_end"],
        }
        for c in chunks
    ]
    if chunk_rows:
        supabase.table("ir_chunks").insert(chunk_rows).execute()

    return {
        "text_length": len(text),
        "page_count": page_count,
        "chunk_count": len(chunks),
        "method": method,
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
    """process_pdf に続けて embedding を生成し ``ir_chunks`` を更新する。"""
    stats = process_pdf(pdf_path, supabase, document_id, force_ocr=force_ocr)

    res = (
        supabase.table("ir_chunks")
        .select("id, chunk_index, chunk_text")
        .eq("document_id", document_id)
        .order("chunk_index")
        .execute()
    )
    rows = res.data or []
    chunks_from_db = [
        {"chunk_index": r["chunk_index"], "text": r["chunk_text"], "id": r["id"]}
        for r in rows
    ]

    if chunks_from_db:
        embedded = await generate_embeddings(chunks_from_db)
        for e in embedded:
            supabase.table("ir_chunks").update(
                {
                    "embedding": e["embedding"],
                    "embedding_model": "text-embedding-3-small",
                }
            ).eq("id", e["id"]).execute()

    stats["embeddings_generated"] = len(chunks_from_db)
    return stats
