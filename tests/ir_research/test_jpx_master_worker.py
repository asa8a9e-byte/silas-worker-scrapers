"""JPX マスタ Excel パーサーのテスト."""

from __future__ import annotations

from pathlib import Path

from ir_research.jpx_master_worker import JpxRow, parse_jpx_xls

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "jpx_sample.xlsx"


def test_parse_jpx_xls_returns_rows():
    """JPX Excel をパースして JpxRow のリストを返す."""
    rows = parse_jpx_xls(FIXTURE_PATH)
    assert len(rows) > 0
    assert isinstance(rows[0], JpxRow)


def test_jpx_row_has_required_fields():
    rows = parse_jpx_xls(FIXTURE_PATH)
    r = rows[0]
    assert r.ticker_code
    assert r.company_name
    assert r.market in (
        "プライム（内国株式）",
        "スタンダード（内国株式）",
        "グロース（内国株式）",
        "プライム（外国株式）",
        "スタンダード（外国株式）",
        "グロース（外国株式）",
    )
    assert r.sector
