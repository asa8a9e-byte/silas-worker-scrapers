"""株価ワーカー: Stooq CSV パース・Kabutan PER/PBR."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from ir_research.stock_price_worker import (
    fetch_market_data_kabutan,
    parse_stooq_csv,
)


def test_parse_stooq_csv_extracts_prices():
    csv_text = """Date,Open,High,Low,Close,Volume
2026-04-18,3500,3550,3480,3520,1234567
2026-04-17,3400,3510,3390,3500,987654"""
    prices = parse_stooq_csv(csv_text)
    assert len(prices) == 2
    assert prices[0].close_price == 3520
    assert prices[0].volume == 1234567


def test_parse_stooq_csv_empty():
    assert parse_stooq_csv("") == []
    assert parse_stooq_csv("   \n") == []


def test_parse_stooq_csv_skips_bad_rows():
    csv_text = """Date,Open,High,Low,Close,Volume
2026-04-18,3500,3550,3480,3520,1234567
not-a-date,bad,high,low,close,1
2026-04-17,3400,3510,3390,3500,100"""
    prices = parse_stooq_csv(csv_text)
    assert len(prices) == 2
    assert prices[1].close_price == 3500


@pytest.mark.asyncio
async def test_fetch_market_data_kabutan_returns_dict():
    mock_html = '<td class="per">15.2</td><td class="pbr">1.8</td>'
    with patch(
        "ir_research.stock_price_worker._fetch_html",
        new=AsyncMock(return_value=mock_html),
    ):
        data = await fetch_market_data_kabutan("7203")
    assert data is not None
    assert data["per"] == 15.2
    assert data["pbr"] == 1.8
