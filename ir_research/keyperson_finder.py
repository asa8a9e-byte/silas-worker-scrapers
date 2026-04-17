"""Serper API でキーパーソンの SNS プロフィール URL を検索."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import httpx

SERPER_API_URL = "https://google.serper.dev/search"

PLATFORM_SITES = {
    "linkedin": "site:linkedin.com/in/",
    "twitter": "site:x.com OR site:twitter.com",
    "facebook": "site:facebook.com",
    "instagram": "site:instagram.com",
}

PLATFORM_DISPLAY = {
    "linkedin": "LinkedIn",
    "twitter": "X",
    "facebook": "Facebook",
    "instagram": "Instagram",
}


@dataclass
class KeyPersonResult:
    person_name: str
    platform: str       # LinkedIn / X / Facebook / Instagram
    profile_url: str
    title: str
    snippet: str
    confidence: float   # 0.0〜1.0


async def _call_serper(query: str, num: int = 5) -> dict:
    api_key = os.environ.get("SERPER_API_KEY")
    if not api_key:
        raise RuntimeError("SERPER_API_KEY not set")
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            SERPER_API_URL,
            json={"q": query, "num": num, "gl": "jp", "hl": "ja"},
            headers={"X-API-KEY": api_key},
        )
        resp.raise_for_status()
        return resp.json()


def _detect_platform(url: str) -> str | None:
    url_lower = url.lower()
    if "linkedin.com" in url_lower:
        return "LinkedIn"
    if "x.com" in url_lower or "twitter.com" in url_lower:
        return "X"
    if "facebook.com" in url_lower:
        return "Facebook"
    if "instagram.com" in url_lower:
        return "Instagram"
    return None


def _calc_confidence(person_name: str, company_name: str, title: str, snippet: str) -> float:
    score = 0.0
    text = f"{title} {snippet}".lower()
    if person_name.replace(" ", "") in text.replace(" ", ""):
        score += 0.4
    if company_name[:4] in text:
        score += 0.4
    if any(kw in text for kw in ["代表", "取締役", "ceo", "cio", "cto", "部長", "執行役員"]):
        score += 0.2
    return min(score, 1.0)


async def search_keyperson_sns(
    person_name: str,
    company_name: str,
    platforms: list[str] | None = None,
) -> list[KeyPersonResult]:
    """Serper API で各 SNS プラットフォームを横断検索."""
    if platforms is None:
        platforms = list(PLATFORM_SITES.keys())

    results: list[KeyPersonResult] = []
    for platform_key in platforms:
        site_filter = PLATFORM_SITES.get(platform_key, "")
        query = f'"{person_name}" "{company_name}" {site_filter}'
        try:
            data = await _call_serper(query, num=3)
        except Exception:
            continue

        for item in data.get("organic", []):
            url = item.get("link", "")
            detected = _detect_platform(url)
            if not detected:
                continue
            confidence = _calc_confidence(
                person_name, company_name,
                item.get("title", ""), item.get("snippet", ""),
            )
            if confidence >= 0.3:
                results.append(KeyPersonResult(
                    person_name=person_name,
                    platform=detected,
                    profile_url=url,
                    title=item.get("title", ""),
                    snippet=item.get("snippet", ""),
                    confidence=confidence,
                ))
    return results


async def search_and_save_keypersons(
    supabase: Any,
    company_id: int,
    ticker_code: str,
    company_name: str,
    tenant_id: str,
    executives: list[dict],
    platforms: list[str] | None = None,
    title_keywords: list[str] | None = None,
) -> int:
    """役員リストから SNS 検索 → keypersons テーブルに保存."""
    if title_keywords:
        executives = [
            e for e in executives
            if any(kw in (e.get("title", "") + " " + e.get("role_category", "")) for kw in title_keywords)
        ]

    saved = 0
    for exec_info in executives:
        name = exec_info.get("name", "")
        if not name:
            continue
        results = await search_keyperson_sns(name, company_name, platforms)
        for r in results:
            supabase.table("keypersons").upsert({
                "tenant_id": tenant_id,
                "company_id": company_id,
                "ticker_code": ticker_code,
                "company_name": company_name,
                "person_name": r.person_name,
                "role": exec_info.get("title", ""),
                "platform": r.platform,
                "profile_url": r.profile_url,
                "confidence": r.confidence,
                "search_type": exec_info.get("role_category", "executive"),
                "context": r.snippet,
            }, on_conflict="tenant_id,ticker_code,person_name,platform").execute()
            saved += 1
    return saved
