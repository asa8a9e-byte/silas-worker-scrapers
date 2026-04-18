"""適時開示テキストとテナント別キーワードアラートの照合."""

from __future__ import annotations

import asyncio
from typing import Any


def _haystack(d: dict[str, Any]) -> str:
    parts = [str(d.get("title") or ""), str(d.get("extracted_summary") or "")]
    return " ".join(parts)


def _company_allowed(
    company_id: int | None,
    company_filter: Any,
) -> bool:
    if company_filter is None:
        return True
    if isinstance(company_filter, list):
        if not company_filter:
            return True
        return company_id is not None and company_id in company_filter
    if isinstance(company_filter, dict):
        ids = company_filter.get("company_ids") or company_filter.get("ids")
        if isinstance(ids, list):
            if not ids:
                return True
            return company_id is not None and company_id in ids
    return True


def _keyword_matches(text: str, keyword: str) -> bool:
    if not keyword.strip():
        return False
    return keyword.casefold() in text.casefold()


async def match_disclosures_to_alerts(supabase: Any, disclosure_ids: list[int]) -> int:
    """開示文 × 全テナントのアクティブアラートを照合し ir_keyword_alert_matches に登録。挿入件数を返す."""
    if not disclosure_ids:
        return 0

    def _load() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        dres = (
            supabase.table("ir_tdnet_disclosures")
            .select("id, company_id, title, extracted_summary")
            .in_("id", disclosure_ids)
            .execute()
        )
        disclosures = list(dres.data or [])
        ares = (
            supabase.table("ir_keyword_alerts")
            .select(
                "id, tenant_id, keyword, is_active, company_filter, sector_filter",
            )
            .eq("is_active", True)
            .execute()
        )
        alerts = list(ares.data or [])
        return disclosures, alerts

    disclosures, alerts = await asyncio.to_thread(_load)
    if not disclosures or not alerts:
        return 0

    matches: list[dict[str, Any]] = []
    for d in disclosures:
        text = _haystack(d)
        if not text.strip():
            continue
        cid = d.get("company_id")
        if isinstance(cid, str) and cid.isdigit():
            cid = int(cid)
        elif cid is not None:
            cid = int(cid)
        else:
            cid = None

        disc_id = int(d["id"])
        for a in alerts:
            kw = str(a.get("keyword") or "")
            if not _keyword_matches(text, kw):
                continue
            if not _company_allowed(cid, a.get("company_filter")):
                continue
            tenant_id = a.get("tenant_id")
            if tenant_id is None:
                continue
            matches.append(
                {
                    "tenant_id": str(tenant_id),
                    "disclosure_id": disc_id,
                    "alert_id": int(a["id"]),
                },
            )

    if not matches:
        return 0

    def _upsert() -> int:
        supabase.table("ir_keyword_alert_matches").upsert(
            matches,
            on_conflict="disclosure_id,alert_id",
        ).execute()
        return len(matches)

    return await asyncio.to_thread(_upsert)
