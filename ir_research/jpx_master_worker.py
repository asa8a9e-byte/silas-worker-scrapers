"""
JPX 上場銘柄一覧 Excel をダウンロード・パースして listed_companies テーブルに upsert する。

JPX 銘柄一覧 URL:
  https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls

実行頻度: 月1回（毎月1日 02:00 JST）
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, TypeVar

import httpx
import pandas as pd

JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

COL_TICKER = "コード"
COL_NAME = "銘柄名"
COL_MARKET = "市場・商品区分"
COL_SECTOR_33 = "33業種区分"
COL_SECTOR_17 = "17業種区分"
COL_SCALE = "規模区分"

_T = TypeVar("_T")


@dataclass(frozen=True)
class JpxRow:
    ticker_code: str
    company_name: str
    market: str
    sector: str
    sector_17: str | None
    scale: str | None


def _normalize_ticker(raw: object) -> str | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip()
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    if not s.isdigit() or len(s) != 4:
        return None
    return s


def parse_jpx_xls(path: str | Path) -> list[JpxRow]:
    """JPX Excel（.xls / .xlsx）をパースして JpxRow のリストを返す."""
    path = Path(path)
    dtype = {COL_TICKER: str}
    df = pd.read_excel(path, dtype=dtype)
    df.columns = [str(c).strip() for c in df.columns]

    required = (COL_TICKER, COL_NAME, COL_MARKET, COL_SECTOR_33)
    for col in required:
        if col not in df.columns:
            msg = f"Missing column {col!r} in {path}"
            raise ValueError(msg)

    has_17 = COL_SECTOR_17 in df.columns
    has_scale = COL_SCALE in df.columns

    out: list[JpxRow] = []
    for _, row in df.iterrows():
        ticker = _normalize_ticker(row.get(COL_TICKER))
        if ticker is None:
            continue

        name = row.get(COL_NAME)
        market = row.get(COL_MARKET)
        sector33 = row.get(COL_SECTOR_33)

        company_name = "" if name is None or pd.isna(name) else str(name).strip()
        market_s = "" if market is None or pd.isna(market) else str(market).strip()
        sector_s = "" if sector33 is None or pd.isna(sector33) else str(sector33).strip()

        sec17: str | None
        if has_17:
            v = row.get(COL_SECTOR_17)
            sec17 = None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip() or None
        else:
            sec17 = None

        scale: str | None
        if has_scale:
            v = row.get(COL_SCALE)
            scale = None if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip() or None
        else:
            scale = None

        out.append(
            JpxRow(
                ticker_code=ticker,
                company_name=company_name,
                market=market_s,
                sector=sector_s,
                sector_17=sec17,
                scale=scale,
            )
        )
    return out


async def download_jpx_xls(dest: str | Path, url: str = JPX_URL, timeout: float = 60.0) -> Path:
    """JPX Excel をダウンロードしてファイルに保存."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        dest.write_bytes(resp.content)
    return dest


def chunked(it: Iterable[_T], size: int) -> Iterator[list[_T]]:
    if size <= 0:
        msg = "size must be positive"
        raise ValueError(msg)
    buf: list[_T] = []
    for x in it:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf
