"""採用情報ワーカー: Serper 検索."""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ir_research.hiring_info_worker import search_hiring_info


@pytest.mark.asyncio
async def test_search_hiring_info_returns_organic_count():
    os.environ["SERPER_API_KEY"] = "test-key"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(
        return_value={
            "organic": [
                {"title": "Indeed 求人", "snippet": "エンジニア募集"},
                {"title": "Wantedly", "snippet": "カジュアル面談"},
            ],
        },
    )

    mock_client_inst = MagicMock()
    mock_client_inst.post = AsyncMock(return_value=mock_resp)
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_client_inst)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    with patch("ir_research.hiring_info_worker.httpx.AsyncClient", return_value=mock_cm):
        data = await search_hiring_info("テスト株式会社")

    assert data["job_count"] == 2
    assert "indeed.com" in data["search_query"]
    assert "テスト株式会社" in data["search_query"]
    assert "エンジニア募集" in (data["search_result_snippet"] or "")
    assert data["hiring_status"] == "active"


@pytest.mark.asyncio
async def test_search_hiring_info_empty_organic():
    os.environ["SERPER_API_KEY"] = "test-key"
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = MagicMock(return_value={"organic": []})

    mock_client_inst = MagicMock()
    mock_client_inst.post = AsyncMock(return_value=mock_resp)
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_client_inst)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    with patch("ir_research.hiring_info_worker.httpx.AsyncClient", return_value=mock_cm):
        data = await search_hiring_info("無名商事")

    assert data["job_count"] == 0
    assert data["hiring_status"] == "unknown"
