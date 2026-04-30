"""財務ワーカー: Kabutan 通期業績推移テーブルのパース."""

from __future__ import annotations

from ir_research.financial_worker import parse_financial_html


def _kabutan_like(rows: list[tuple[str, ...]]) -> str:
    """Kabutan の通期業績推移テーブル風 HTML を組み立てる."""
    body = ""
    for cells in rows:
        body += "<tr>"
        body += f"<th>{cells[0]}</th>"
        for v in cells[1:]:
            body += f"<td>{v}</td>"
        body += "</tr>"
    return f"""
    <html><body>
    <table>
      <thead>
        <tr>
          <th>決算期</th><th>売上高</th><th>営業益</th><th>経常益</th>
          <th>最終益</th><th>修正1株益</th><th>修正1株配</th><th>発表日</th>
        </tr>
      </thead>
      <tbody>
        {body}
      </tbody>
    </table>
    </body></html>
    """


def test_parse_financial_html_extracts_latest_fy():
    html = _kabutan_like(
        [
            ("2023.03", "1,000,000", "100,000", "98,000", "60,000", "300.0", "50"),
            ("2024.03", "1,200,000", "150,000", "145,000", "90,000", "420.5", "60"),
            ("2025.03", "1,234,567", "123,456", "120,000", "99,999", "500.0", "70"),
        ]
    )
    d = parse_financial_html(html)
    assert d["fiscal_year"] == "2025"
    assert d["revenue"] == 1234567.0
    assert d["operating_income"] == 123456.0
    assert d["net_income"] == 99999.0
    assert d["eps"] == 500.0
    assert d["dps"] == 70.0
    assert abs(d["operating_margin"] - (123456.0 / 1234567.0 * 100.0)) < 0.01
    assert d["source"] == "kabutan"


def test_parse_financial_html_skips_forecast_and_growth_rows():
    html = _kabutan_like(
        [
            ("2024.03", "1,000,000", "100,000", "98,000", "60,000", "300", "50"),
            ("2025.03予", "1,300,000", "130,000", "125,000", "80,000", "400", "60"),
            ("前期比", "+30.0", "+30.0", "+27.6", "+33.3", "+33.3", ""),
        ]
    )
    d = parse_financial_html(html)
    assert d["fiscal_year"] == "2024"
    assert d["revenue"] == 1000000.0


def test_parse_financial_html_handles_ifrs_no_operating_income():
    """IFRS 銘柄では営業益が '－' のことがある。売上高は取れるべき."""
    html = _kabutan_like(
        [
            ("I2024.03", "6,756,500", "－", "57,801", "-227,646", "-42.8", "11"),
            ("I2025.03", "7,243,752", "－", "1,704,721", "1,153,332", "195.2", "11"),
        ]
    )
    d = parse_financial_html(html)
    assert d["fiscal_year"] == "2025"
    assert d["revenue"] == 7243752.0
    assert d["operating_income"] is None
    assert "operating_margin" not in d


def test_parse_financial_html_clamps_extreme_margin():
    """NUMERIC(8,4) に入らない異常値の営業利益率は格納しない."""
    html = _kabutan_like(
        [
            ("2025.03", "1", "1,000,000", "1,000,000", "500,000", "100", "0"),
        ]
    )
    d = parse_financial_html(html)
    assert d["revenue"] == 1.0
    assert "operating_margin" not in d


def test_parse_financial_html_empty():
    assert parse_financial_html("") == {}
    assert parse_financial_html("   ") == {}
    assert parse_financial_html("<html><body>no table</body></html>") == {}
