"""
EDINET API から有価証券報告書・四半期報告書のメタデータを取得し、
ir_documents テーブルに upsert する。

EDINET v2 API:
  https://disclosure.edinet-fsa.go.jp/api/v2/documents.json?date=YYYY-MM-DD&type=2

Phase A では PDF はダウンロードしない（メタのみ）。Phase B で pdf_processor が
storage_path / markdown_content を埋める。
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import httpx

EDINET_BASE = "https://disclosure.edinet-fsa.go.jp/api/v2"

# docTypeCode（EDINET 仕様）— 有報・四半期・半期・訂正・臨時
YUHO_CODES = {"120", "130", "140", "150", "160", "170", "180"}

DOC_TYPE_TO_OUR_TYPE = {
    "120": "有価証券報告書",
    "130": "訂正有価証券報告書",
    "140": "四半期報告書",
    "150": "訂正四半期報告書",
    "160": "半期報告書",
    "170": "訂正半期報告書",
    "180": "臨時報告書",
}


@dataclass(frozen=True)
class EdinetDoc:
    doc_id: str
    edinet_code: str
    company_name: str
    doc_type_code: str
    doc_type: str
    filer_name: str
    submit_date_time: str
    pdf_url: str


def is_yuho_or_kessan(doc: EdinetDoc) -> bool:
    return doc.doc_type_code in YUHO_CODES


def _doc_type_label(r: dict[str, Any]) -> str:
    code = str(r.get("docTypeCode") or "")
    return DOC_TYPE_TO_OUR_TYPE.get(code, str(r.get("docDescription") or ""))


def parse_edinet_documents_response(payload: dict[str, Any]) -> list[EdinetDoc]:
    out: list[EdinetDoc] = []
    for r in payload.get("results", []):
        doc_id = r.get("docID")
        edinet_code = r.get("edinetCode") or ""
        if not doc_id or not edinet_code:
            continue
        code = str(r.get("docTypeCode") or "")
        out.append(
            EdinetDoc(
                doc_id=str(doc_id),
                edinet_code=str(edinet_code),
                company_name=str(r.get("filerName") or ""),
                doc_type_code=code,
                doc_type=_doc_type_label(r),
                filer_name=str(r.get("filerName") or ""),
                submit_date_time=str(r.get("submitDateTime") or ""),
                pdf_url=f"{EDINET_BASE}/documents/{doc_id}?type=2",
            )
        )
    return out


async def fetch_documents_for_date(
    target: date,
    api_key: str | None = None,
    timeout: float = 30.0,
) -> list[EdinetDoc]:
    key = api_key or os.environ.get("EDINET_API_KEY")
    if not key:
        msg = "EDINET_API_KEY is not set"
        raise RuntimeError(msg)
    url = f"{EDINET_BASE}/documents.json"
    params = {"date": target.isoformat(), "type": "2", "Subscription-Key": key}
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()
    parsed = parse_edinet_documents_response(payload)
    return [d for d in parsed if is_yuho_or_kessan(d)]


async def fetch_documents_for_range(
    start: date,
    end: date,
    api_key: str | None = None,
    sleep_seconds: float = 1.0,
) -> list[EdinetDoc]:
    """指定期間 (start..end inclusive) の有報・四半期メタを取得。各日の後にスリープ."""
    out: list[EdinetDoc] = []
    cursor = start
    while cursor <= end:
        try:
            out.extend(await fetch_documents_for_date(cursor, api_key=api_key))
        except httpx.HTTPStatusError as e:
            print(f"[edinet] {cursor}: HTTP {e.response.status_code}")
        await asyncio.sleep(sleep_seconds)
        cursor += timedelta(days=1)
    return out


async def upsert_edinet_docs(supabase: object, docs: list[EdinetDoc]) -> int:
    """edinet_code から listed_companies.id を解決し、ir_documents に insert."""

    def _run() -> int:
        if not docs:
            return 0
        edinet_codes = list({d.edinet_code for d in docs})
        res = (
            supabase.table("listed_companies")
            .select("id, edinet_code")
            .in_("edinet_code", edinet_codes)
            .execute()
        )
        code_to_id = {r["edinet_code"]: r["id"] for r in (res.data or [])}

        payload: list[dict[str, Any]] = []
        for d in docs:
            company_id = code_to_id.get(d.edinet_code)
            if company_id is None:
                continue
            published: str | None = None
            if d.submit_date_time:
                published = d.submit_date_time[:10]
            payload.append(
                {
                    "company_id": company_id,
                    "document_type": d.doc_type,
                    "document_title": f"{d.company_name} {d.doc_type}",
                    "document_url": d.pdf_url,
                    "published_at": published,
                    "extraction_status": "未抽出",
                }
            )
        if not payload:
            return 0
        supabase.table("ir_documents").insert(payload).execute()
        return len(payload)

    return await asyncio.to_thread(_run)
