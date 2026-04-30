"""Kabutan 財務ページから数値を抽出し ir_financials / ir_dividends に保存."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

KABUTAN_FINANCE_URL = "https://kabutan.jp/stock/finance?code={code}"

# 通期業績推移テーブルに必須の列ヘッダ
_TARGET_HEADERS = ["決算期", "売上高", "営業益", "経常益", "最終益"]
_COL_REVENUE = 0
_COL_OP_INCOME = 1
_COL_ORDINARY = 2
_COL_NET_INCOME = 3
_COL_EPS = 4
_COL_DPS = 5


def _parse_number_jp(text: str) -> float | None:
    t = text.strip().replace(",", "").replace("−", "-")
    if not t or t in ("-", "－"):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def _norm_fy(fy: str) -> str | None:
    """'2024.03' / 'I2024.03' → '2024'。前期比/予 などは None."""
    if not fy:
        return None
    s = fy.strip()
    if "前" in s or "比" in s or s == "予":
        return None
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else None


def parse_financial_html(html: str) -> dict[str, Any]:
    """Kabutan 通期業績推移テーブルから最新FYの主要指標を抽出."""
    if not html or not html.strip():
        return {}

    soup = BeautifulSoup(html, "html.parser")
    for table in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in table.find_all("th")]
        if not all(h in headers for h in _TARGET_HEADERS):
            continue

        latest: dict[str, Any] | None = None
        for tr in table.find_all("tr"):
            ths = [c.get_text(strip=True) for c in tr.find_all("th")]
            tds = [c.get_text(strip=True) for c in tr.find_all("td")]
            if not tds:
                continue
            fy_raw = ths[0] if ths else (tds[0] if tds else "")
            data = tds if ths else tds[1:]
            if "予" in fy_raw:
                continue
            fy = _norm_fy(fy_raw)
            if not fy:
                continue
            if len(data) < 4:
                continue

            revenue = _parse_number_jp(data[_COL_REVENUE])
            if revenue is None:
                continue
            op_inc = _parse_number_jp(data[_COL_OP_INCOME]) if len(data) > _COL_OP_INCOME else None
            ord_inc = _parse_number_jp(data[_COL_ORDINARY]) if len(data) > _COL_ORDINARY else None
            net_inc = _parse_number_jp(data[_COL_NET_INCOME]) if len(data) > _COL_NET_INCOME else None
            eps = _parse_number_jp(data[_COL_EPS]) if len(data) > _COL_EPS else None
            dps = _parse_number_jp(data[_COL_DPS]) if len(data) > _COL_DPS else None

            row: dict[str, Any] = {
                "fiscal_year": fy,
                "revenue": revenue,
                "operating_income": op_inc,
                "ordinary_income": ord_inc,
                "net_income": net_inc,
                "eps": eps,
                "dps": dps,
            }
            if latest is None or fy > latest["fiscal_year"]:
                latest = row

        if latest is not None:
            op_inc = latest.get("operating_income")
            rev = latest.get("revenue")
            if op_inc is not None and rev:
                margin = op_inc / rev * 100.0
                # ir_financials.operating_margin = NUMERIC(8,4)
                if -9999.0 < margin < 9999.0:
                    latest["operating_margin"] = round(margin, 4)
            latest["source"] = "kabutan"
            return latest

    return {}


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
            "ordinary_income": data.get("ordinary_income"),
            "net_income": data.get("net_income"),
            "eps": data.get("eps"),
            "dps": data.get("dps"),
            "operating_margin": data.get("operating_margin"),
            "source": data.get("source", "kabutan"),
        }
        supabase.table("ir_financials").upsert(
            fin_row,
            on_conflict="company_id,fiscal_year,fiscal_quarter",
        ).execute()

    await asyncio.to_thread(_run)
    return True
