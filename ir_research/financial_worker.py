"""Kabutan 財務ページから数値を抽出し ir_financials / ir_dividends に保存."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import httpx

KABUTAN_FINANCE_URL = "https://kabutan.jp/stock/finance?code={code}"


def _parse_number_jp(text: str) -> float | None:
    t = text.strip().replace(",", "")
    if not t or t == "-":
        return None
    try:
        return float(t)
    except ValueError:
        return None


def parse_financial_html(html: str) -> dict[str, Any]:
    """Kabutan 財務HTMLから主要指標を抽出（ページ構造変更に弱いヒューリスティック）."""
    if not html or not html.strip():
        return {}

    fy_m = re.search(r"(\d{4})年\s*\d{1,2}月期", html)
    fiscal_year = fy_m.group(1) if fy_m else None

    def grab(label: str) -> float | None:
        m = re.search(
            rf"{re.escape(label)}[^0-9\-−]*([\-−]?[\d,]+(?:\.[\d]+)?)",
            html,
        )
        if not m:
            return None
        raw = m.group(1).replace("−", "-")
        return _parse_number_jp(raw)

    revenue = grab("売上高")
    operating_income = grab("営業利益")
    net_income = grab("当期純利益") or grab("純利益")
    roe = grab("ROE")
    roa = grab("ROA")

    operating_margin: float | None = None
    if revenue and operating_income is not None and revenue != 0:
        operating_margin = round((operating_income / revenue) * 100.0, 4)

    div_yield = grab("配当利回り")
    annual_div = grab("1株配当") or grab("年間配当")

    out: dict[str, Any] = {}
    if fiscal_year:
        out["fiscal_year"] = fiscal_year
    if revenue is not None:
        out["revenue"] = revenue
    if operating_income is not None:
        out["operating_income"] = operating_income
    if net_income is not None:
        out["net_income"] = net_income
    if roe is not None:
        out["roe"] = roe
    if roa is not None:
        out["roa"] = roa
    if operating_margin is not None:
        out["operating_margin"] = operating_margin
    if div_yield is not None:
        out["dividend_yield"] = div_yield
    if annual_div is not None:
        out["annual_dividend_per_share"] = annual_div

    out["source"] = "kabutan"
    return out


async def _fetch_finance_html(ticker: str, timeout: float = 20.0) -> str:
    url = KABUTAN_FINANCE_URL.format(code=ticker)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        resp = await client.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; IRResearchWorker/1.0)"},
        )
        resp.raise_for_status()
        return resp.text


async def fetch_and_save_financials(supabase: Any, company_id: int, ticker: str) -> bool:
    """財務・配当を取得して ir_financials / ir_dividends に upsert."""
    html = await _fetch_finance_html(ticker)
    data = parse_financial_html(html)
    if not data:
        return False

    fiscal_year = str(data.get("fiscal_year") or "")
    if not fiscal_year:
        return False

    def _run() -> None:
        fin_row = {
            "company_id": company_id,
            "fiscal_year": fiscal_year,
            "fiscal_quarter": "FY",
            "revenue": data.get("revenue"),
            "operating_income": data.get("operating_income"),
            "net_income": data.get("net_income"),
            "roe": data.get("roe"),
            "roa": data.get("roa"),
            "operating_margin": data.get("operating_margin"),
            "source": data.get("source", "kabutan"),
        }
        supabase.table("ir_financials").upsert(
            fin_row,
            on_conflict="company_id,fiscal_year,fiscal_quarter",
        ).execute()

        div_row = {
            "company_id": company_id,
            "fiscal_year": fiscal_year,
            "dividend_yield": data.get("dividend_yield"),
            "annual_dividend": data.get("annual_dividend_per_share"),
        }
        if any(
            div_row[k] is not None
            for k in ("dividend_yield", "annual_dividend")
        ):
            supabase.table("ir_dividends").upsert(
                div_row,
                on_conflict="company_id,fiscal_year",
            ).execute()

    await asyncio.to_thread(_run)
    return True
