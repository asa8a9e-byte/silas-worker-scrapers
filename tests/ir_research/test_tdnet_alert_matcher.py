"""TDnet 開示 × キーワードアラート マッチャー."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ir_research.tdnet_alert_matcher import match_disclosures_to_alerts


def _supabase_for_match(
    disclosures: list[dict],
    alerts: list[dict],
    upsert_rows: list | None = None,
):
    supa = MagicMock()

    def table(name: str):
        root = MagicMock()
        if name == "ir_tdnet_disclosures":
            ex = MagicMock()
            ex.data = disclosures
            root.select.return_value.in_.return_value.execute.return_value = ex
        elif name == "ir_keyword_alerts":
            ex = MagicMock()
            ex.data = alerts
            root.select.return_value.eq.return_value.execute.return_value = ex
        elif name == "ir_keyword_alert_matches":

            def upsert(rows, on_conflict=None):
                if upsert_rows is not None:
                    upsert_rows.extend(rows)
                u = MagicMock()
                u.execute.return_value = MagicMock()
                return u

            root.upsert.side_effect = upsert
        return root

    supa.table.side_effect = table
    return supa


@pytest.mark.asyncio
async def test_match_disclosures_to_alerts_finds_keyword():
    captured: list = []
    disc = [
        {
            "id": 1,
            "company_id": 100,
            "title": "新工場への投資決議",
            "extracted_summary": None,
        },
    ]
    al = [
        {
            "id": 50,
            "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
            "keyword": "投資",
            "is_active": True,
            "company_filter": None,
            "sector_filter": None,
        },
    ]
    supa = _supabase_for_match(disc, al, captured)
    n = await match_disclosures_to_alerts(supa, [1])
    assert n == 1
    assert len(captured) == 1
    assert captured[0]["disclosure_id"] == 1
    assert captured[0]["alert_id"] == 50


@pytest.mark.asyncio
async def test_match_disclosures_to_alerts_no_match():
    captured: list = []
    disc = [
        {
            "id": 2,
            "company_id": 200,
            "title": "定款一部変更",
            "extracted_summary": "議決権の件",
        },
    ]
    al = [
        {
            "id": 51,
            "tenant_id": "650e8400-e29b-41d4-a716-446655440001",
            "keyword": "半導体",
            "is_active": True,
            "company_filter": None,
            "sector_filter": None,
        },
    ]
    supa = _supabase_for_match(disc, al, captured)
    n = await match_disclosures_to_alerts(supa, [2])
    assert n == 0
    assert captured == []
