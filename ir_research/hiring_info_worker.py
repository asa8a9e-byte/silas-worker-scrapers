"""Serper で Indeed / Wantedly 採用件数を検索し ir_hiring_data に保存."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import httpx

SERPER_SEARCH_URL = "https://google.serper.dev/search"


def _build_query(company_name: str) -> str:
    return f'"{company_name}" 求人 site:indeed.com OR site:wantedly.com'


async def search_hiring_info(company_name: str) -> dict[str, Any]:
    """Serper Google API で採用関連の検索結果件数・概要を返す."""
    key = os.environ.get("SERPER_API_KEY")
    if not key:
        msg = "SERPER_API_KEY is not set"
        raise RuntimeError(msg)

    query = _build_query(company_name)
    payload = {"q": query, "num": 20}
    headers = {"X-API-KEY": key, "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.post(SERPER_SEARCH_URL, headers=headers, json=payload)
        resp.raise_for_status()
        body = resp.json()

    organic = body.get("organic") or []
    job_count = len(organic)
    snippets: list[str] = []
    for item in organic[:10]:
        if isinstance(item, dict):
            s = (item.get("snippet") or item.get("title") or "").strip()
            if s:
                snippets.append(s)
    snippet_text = "\n---\n".join(snippets)[:8000]

    return {
        "job_count": job_count,
        "search_query": query,
        "search_result_snippet": snippet_text or None,
        "hiring_status": "active" if job_count > 0 else "unknown",
        "raw_organic_count": job_count,
        "serper_raw": body,
    }


async def save_hiring_info(supabase: Any, company_id: int, data: dict[str, Any]) -> None:
    """ir_hiring_data に upsert（source=serper は Indeed/Wantedly 横断検索の集計）."""

    def _run() -> None:
        job_types = json.dumps(
            {"sources": ["indeed.com", "wantedly.com"], "engine": "serper"},
            ensure_ascii=False,
        )
        row = {
            "company_id": company_id,
            "source": "serper",
            "job_count": data.get("job_count"),
            "search_query": data.get("search_query"),
            "search_result_snippet": data.get("search_result_snippet"),
            "hiring_status": data.get("hiring_status"),
            "job_types": job_types,
        }
        supabase.table("ir_hiring_data").upsert(
            row,
            on_conflict="company_id,source",
        ).execute()

    await asyncio.to_thread(_run)
