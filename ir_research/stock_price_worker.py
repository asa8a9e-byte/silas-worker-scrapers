"""Stooq / Kabutan から株価・PER/PBR を取得し ir_stock_prices / ir_market_data に保存."""

from __future__ import annotations

import asyncio
import csv
import io
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx

STOOQ_URL = "https://stooq.com/q/d/l/?s={ticker}.jp&d1={start}&d2={end}&i=d"


@dataclass
class StockPrice:
    price_date: str
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: int


def parse_stooq_csv(csv_text: str) -> list[StockPrice]:
    if not csv_text.strip():
        return []
    reader = csv.DictReader(io.StringIO(csv_text))
    results: list[StockPrice] = []
    for row in reader:
        try:
            results.append(
                StockPrice(
                    price_date=str(row["Date"]).strip(),
                    open_price=float(row["Open"]),
                    high_price=float(row["High"]),
                    low_price=float(row["Low"]),
                    close_price=float(row["Close"]),
                    volume=int(float(row["Volume"])),
                )
            )
        except (KeyError, ValueError, TypeError):
            continue
    return results


async def _fetch_html(url: str, timeout: float = 15.0) -> str:
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        resp = await client.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; IRResearchWorker/1.0)"},
        )
        resp.raise_for_status()
        return resp.text


def _parse_per_pbr_from_html(html: str) -> dict[str, float]:
    data: dict[str, float] = {}
    per_td = re.search(r'class="per"[^>]*>\s*([\d,.]+)', html)
    pbr_td = re.search(r'class="pbr"[^>]*>\s*([\d,.]+)', html)
    if per_td:
        data["per"] = float(per_td.group(1).replace(",", ""))
    if pbr_td:
        data["pbr"] = float(pbr_td.group(1).replace(",", ""))
    if data:
        return data
    per_m = re.search(r"PER[^\d]{0,12}([\d.]+)", html)
    pbr_m = re.search(r"PBR[^\d]{0,12}([\d.]+)", html)
    out: dict[str, float] = {}
    if per_m:
        out["per"] = float(per_m.group(1))
    if pbr_m:
        out["pbr"] = float(pbr_m.group(1))
    return out


async def fetch_stock_prices(
    ticker: str,
    start: str = "20250101",
    end: str = "20261231",
) -> list[StockPrice]:
    d1 = start.replace("-", "")
    d2 = end.replace("-", "")
    url = STOOQ_URL.format(ticker=ticker, start=d1, end=d2)
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return parse_stooq_csv(resp.text)


async def fetch_market_data_kabutan(ticker: str) -> dict[str, Any] | None:
    url = f"https://kabutan.jp/stock/?code={ticker}"
    try:
        html = await _fetch_html(url)
    except Exception:
        return None
    nums = _parse_per_pbr_from_html(html)
    return dict(nums) if nums else None


async def update_stock_data(supabase: Any, company_id: int, ticker: str) -> int:
    prices = await fetch_stock_prices(ticker)
    if not prices:
        return 0
    prices.sort(key=lambda p: p.price_date, reverse=True)
    market_data: dict[str, Any] = await fetch_market_data_kabutan(ticker) or {}
    latest = prices[0]
    now = datetime.now(timezone.utc).isoformat()

    def _run() -> int:
        rows = [
            {
                "company_id": company_id,
                "price_date": p.price_date,
                "open_price": p.open_price,
                "high_price": p.high_price,
                "low_price": p.low_price,
                "close_price": p.close_price,
                "volume": p.volume,
            }
            for p in prices
        ]
        supabase.table("ir_stock_prices").upsert(
            rows,
            on_conflict="company_id,price_date",
        ).execute()

        supabase.table("ir_market_data").upsert(
            {
                "company_id": company_id,
                "current_price": latest.close_price,
                "per": market_data.get("per"),
                "pbr": market_data.get("pbr"),
                "last_updated": now,
                "source": "stooq+kabutan",
            },
            on_conflict="company_id",
        ).execute()
        return len(rows)

    return await asyncio.to_thread(_run)
