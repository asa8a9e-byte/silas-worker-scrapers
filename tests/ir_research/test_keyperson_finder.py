# tests/ir_research/test_keyperson_finder.py
import pytest
from unittest.mock import AsyncMock, patch
from ir_research.keyperson_finder import (
    search_keyperson_sns,
    PLATFORM_SITES,
)


def test_platform_sites_defined():
    assert "linkedin" in PLATFORM_SITES
    assert "twitter" in PLATFORM_SITES
    assert "facebook" in PLATFORM_SITES
    assert "instagram" in PLATFORM_SITES


@pytest.mark.asyncio
async def test_search_keyperson_sns_returns_results():
    mock_serper_response = {
        "organic": [
            {"title": "山田太郎 - CIO | LinkedIn", "link": "https://linkedin.com/in/yamada", "snippet": "トヨタ自動車 CIO"},
            {"title": "山田太郎 (@yamada) / X", "link": "https://x.com/yamada", "snippet": "CIO at Toyota"},
        ]
    }
    with patch("ir_research.keyperson_finder._call_serper", new=AsyncMock(return_value=mock_serper_response)):
        results = await search_keyperson_sns(
            person_name="山田太郎",
            company_name="トヨタ自動車",
            platforms=["linkedin", "twitter"],
        )
    assert len(results) >= 1
    assert any(r.platform == "LinkedIn" for r in results)


@pytest.mark.asyncio
async def test_search_keyperson_sns_empty_response():
    with patch("ir_research.keyperson_finder._call_serper", new=AsyncMock(return_value={"organic": []})):
        results = await search_keyperson_sns(
            person_name="存在しない人",
            company_name="存在しない会社",
            platforms=["linkedin"],
        )
    assert results == []
