"""JPX → listed_companies upsert のユニットテスト."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ir_research.jpx_master_worker import JpxRow, upsert_jpx_rows


@pytest.mark.asyncio
async def test_upsert_jpx_rows_calls_supabase():
    rows = [
        JpxRow(
            ticker_code="7203",
            company_name="トヨタ自動車",
            market="プライム（内国株式）",
            sector="輸送用機器",
            sector_17=None,
            scale="TOPIX Core30",
        ),
        JpxRow(
            ticker_code="9984",
            company_name="ソフトバンクグループ",
            market="プライム（内国株式）",
            sector="情報・通信業",
            sector_17=None,
            scale="TOPIX Large70",
        ),
    ]
    sb = MagicMock()
    sb.table = MagicMock(return_value=sb)
    sb.upsert = MagicMock(return_value=sb)
    sb.execute = MagicMock(return_value=MagicMock(data=[{"id": 1}, {"id": 2}]))

    inserted = await upsert_jpx_rows(sb, rows)
    assert inserted == 2
    sb.table.assert_called_with("listed_companies")
    sb.upsert.assert_called_once()
