"""財務ワーカー: Kabutan HTML パース."""

from __future__ import annotations

from ir_research.financial_worker import parse_financial_html


def test_parse_financial_html_extracts_metrics():
    html = """
    <html><body>
    <h2>2025年3月期 通期</h2>
    <table>
    <tr><th>売上高</th><td>1,234,567</td></tr>
    <tr><th>営業利益</th><td>123,456</td></tr>
    <tr><th>当期純利益</th><td>99,999</td></tr>
    <tr><th>ROE</th><td>12.50</td></tr>
    <tr><th>ROA</th><td>5.25</td></tr>
    <tr><th>配当利回り</th><td>2.10</td></tr>
    <tr><th>1株配当</th><td>50.0</td></tr>
    </table>
    </body></html>
    """
    d = parse_financial_html(html)
    assert d["fiscal_year"] == "2025"
    assert d["revenue"] == 1234567.0
    assert d["operating_income"] == 123456.0
    assert d["net_income"] == 99999.0
    assert d["roe"] == 12.50
    assert d["roa"] == 5.25
    assert d["operating_margin"] is not None
    assert abs(d["operating_margin"] - (123456.0 / 1234567.0 * 100.0)) < 0.01
    assert d["dividend_yield"] == 2.10
    assert d["annual_dividend_per_share"] == 50.0
    assert d["source"] == "kabutan"


def test_parse_financial_html_empty():
    assert parse_financial_html("") == {}
    assert parse_financial_html("   ") == {}
