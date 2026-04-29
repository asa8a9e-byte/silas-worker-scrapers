"""
TDnet 適時開示一覧をスクレイピングして ir_tdnet_disclosures に upsert する。

TDnet: https://www.release.tdnet.info/inbs/I_main_00.html
日次の開示一覧ページ:
  https://www.release.tdnet.info/inbs/I_list_001_YYYYMMDD.html
  ページネーションあり (001, 002, ...)。1ページあたり最大100件。

実行頻度: 日次（毎日 18:00 JST、当日 + 直近数日を再スキャンして増分取得）
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import httpx
from bs4 import BeautifulSoup, Tag

TDNET_BASE = "https://www.release.tdnet.info/inbs"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"
)
PAGE_SIZE = 100  # TDnet 1ページあたりの開示件数 (上限)
MAX_PAGES = 50  # 1日あたり最大ページ数 (安全のため上限)


@dataclass(frozen=True)
class TdnetRow:
    disclosure_id: str  # 例: "20260429_140098000020230315"
    disclosure_date: date
    disclosure_time: str  # "HH:MM"
    ticker_code: str  # 4-5桁の会社コード（数字+末尾0が多い）
    company_name: str
    title: str
    pdf_url: str | None
    xbrl_url: str | None
    disclosure_type: str | None  # 種別が取れる場合（東証 etc）


def _normalize_ticker(raw: str) -> str:
    s = (raw or "").strip()
    # TDnet の会社コードは末尾 0 が付く 5桁形式 ("12345" → "1234")
    if re.fullmatch(r"\d{5}", s) and s.endswith("0"):
        return s[:-1]
    return s


def _build_list_url(target_date: date, page: int) -> str:
    """日付 + ページ番号からURL を組み立てる。

    例: I_list_001_20260429.html (1ページ目), I_list_002_20260429.html (2ページ目)
    """
    return f"{TDNET_BASE}/I_list_{page:03d}_{target_date.strftime('%Y%m%d')}.html"


async def _fetch_html(client: httpx.AsyncClient, url: str) -> str | None:
    try:
        r = await client.get(url, timeout=30.0)
    except (httpx.HTTPError, httpx.TimeoutException):
        return None
    if r.status_code == 404:
        return None
    r.raise_for_status()
    # TDnet は Shift_JIS。content-type にエンコーディングが入っている場合あり
    if r.encoding is None or r.encoding.lower() in ("iso-8859-1", "ascii"):
        try:
            return r.content.decode("cp932", errors="replace")
        except UnicodeDecodeError:
            return r.text
    return r.text


def _extract_url(td: Tag, base_url: str) -> str | None:
    a = td.find("a") if td else None
    if not isinstance(a, Tag):
        return None
    href = a.get("href")
    if not isinstance(href, str) or not href.strip():
        return None
    if href.startswith("http"):
        return href
    # 相対パスは TDNET_BASE/ に対して解決
    return f"{TDNET_BASE}/{href.lstrip('/')}"


def parse_tdnet_list_html(html: str, target_date: date) -> list[TdnetRow]:
    """1日分の TDnet 開示一覧 HTML を parse して TdnetRow のリストを返す。"""
    soup = BeautifulSoup(html, "html.parser")
    rows: list[TdnetRow] = []
    # TDnet の開示一覧テーブルは id="main-list-table" or class マッチで取れる
    table = soup.find("table", id="main-list-table") or soup.find("table")
    if not isinstance(table, Tag):
        return rows
    base_url = f"{TDNET_BASE}/"
    for tr in table.find_all("tr"):
        if not isinstance(tr, Tag):
            continue
        tds = tr.find_all("td")
        if not tds or len(tds) < 5:
            continue
        # 期待カラム: 時刻 / コード / 会社名 / タイトル(+PDF) / XBRL / 関連情報 / 取引所
        time_str = tds[0].get_text(strip=True) if isinstance(tds[0], Tag) else ""
        code_raw = tds[1].get_text(strip=True) if isinstance(tds[1], Tag) else ""
        company_name = tds[2].get_text(strip=True) if isinstance(tds[2], Tag) else ""
        title_td = tds[3] if isinstance(tds[3], Tag) else None
        if title_td is None:
            continue
        title = title_td.get_text(strip=True)
        pdf_url = _extract_url(title_td, base_url)
        xbrl_td = tds[4] if isinstance(tds[4], Tag) and len(tds) > 4 else None
        xbrl_url = _extract_url(xbrl_td, base_url) if xbrl_td else None
        exchange = ""
        if len(tds) >= 7 and isinstance(tds[6], Tag):
            exchange = tds[6].get_text(strip=True)
        if not (time_str and code_raw and title):
            continue
        ticker = _normalize_ticker(code_raw)
        if not ticker:
            continue
        # disclosure_id は PDF ファイル名から抽出。無ければ日付+時刻+code で生成
        disclosure_id: str | None = None
        if pdf_url:
            m = re.search(r"/([0-9a-zA-Z]+)\.pdf", pdf_url)
            if m:
                disclosure_id = m.group(1)
        if not disclosure_id:
            disclosure_id = (
                f"{target_date.strftime('%Y%m%d')}_{time_str.replace(':', '')}_{ticker}"
            )
        rows.append(
            TdnetRow(
                disclosure_id=disclosure_id,
                disclosure_date=target_date,
                disclosure_time=time_str,
                ticker_code=ticker,
                company_name=company_name,
                title=title,
                pdf_url=pdf_url,
                xbrl_url=xbrl_url,
                disclosure_type=exchange or None,
            )
        )
    return rows


async def fetch_disclosures_for_date(
    target_date: date, *, client: httpx.AsyncClient | None = None
) -> list[TdnetRow]:
    """指定日の開示を全ページ取得。"""
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient(headers={"User-Agent": USER_AGENT})
    try:
        all_rows: list[TdnetRow] = []
        for page in range(1, MAX_PAGES + 1):
            url = _build_list_url(target_date, page)
            html = await _fetch_html(client, url)  # type: ignore[arg-type]
            if html is None:
                break
            rows = parse_tdnet_list_html(html, target_date)
            if not rows:
                break
            all_rows.extend(rows)
            # 1ページ満杯でなければ最終ページ
            if len(rows) < PAGE_SIZE:
                break
        return all_rows
    finally:
        if own_client and client is not None:
            await client.aclose()


async def fetch_disclosures_for_range(
    start: date, end: date
) -> list[TdnetRow]:
    """[start, end] 期間の全開示を取得。"""
    if start > end:
        return []
    days: list[date] = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    out: list[TdnetRow] = []
    async with httpx.AsyncClient(headers={"User-Agent": USER_AGENT}) as client:
        for day in days:
            day_rows = await fetch_disclosures_for_date(day, client=client)
            out.extend(day_rows)
            # サーバ負荷配慮で軽いインターバル
            await asyncio.sleep(0.5)
    return out


async def upsert_tdnet_rows(
    supabase: Any,
    rows: list[TdnetRow],
    *,
    batch_size: int = 500,
) -> int:
    """ir_tdnet_disclosures に upsert (disclosure_id UNIQUE)。company_id は別途解決。"""
    if not rows:
        return 0
    # ticker_code → company_id 解決のため listed_companies から一括取得
    tickers = sorted({r.ticker_code for r in rows})
    company_map: dict[str, int] = {}

    def _fetch_companies() -> None:
        # IN 句で 1000 件ずつ
        for i in range(0, len(tickers), 1000):
            chunk = tickers[i : i + 1000]
            res = (
                supabase.table("listed_companies")
                .select("id, ticker_code")
                .in_("ticker_code", chunk)
                .execute()
            )
            for r in (res.data or []):
                tk = r.get("ticker_code")
                cid = r.get("id")
                if tk and cid is not None:
                    company_map[str(tk)] = int(cid)

    await asyncio.to_thread(_fetch_companies)

    payload: list[dict[str, Any]] = []
    for r in rows:
        payload.append(
            {
                "disclosure_id": r.disclosure_id,
                "disclosure_date": r.disclosure_date.isoformat(),
                "disclosure_time": r.disclosure_time,
                "ticker_code": r.ticker_code,
                "company_id": company_map.get(r.ticker_code),
                "company_name": r.company_name,
                "title": r.title,
                "pdf_url": r.pdf_url,
                "xbrl_url": r.xbrl_url,
                "disclosure_type": r.disclosure_type,
            }
        )

    total = 0

    def _run_batch(batch: list[dict[str, Any]]) -> None:
        supabase.table("ir_tdnet_disclosures").upsert(
            batch, on_conflict="disclosure_id"
        ).execute()

    for i in range(0, len(payload), batch_size):
        chunk = payload[i : i + batch_size]
        await asyncio.to_thread(_run_batch, chunk)
        total += len(chunk)
    return total
