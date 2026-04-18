import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ir_research.tenant_analyzer import analyze_company, parse_gemini_response


def test_parse_gemini_response_extracts_fields():
    raw_json = json.dumps(
        {
            "summary": "業績好調",
            "financial_highlights": {"revenue": 100000, "operating_margin": 8.5},
            "profit_factors": ["売上増"],
            "risks": ["為替リスク"],
            "outlook": "成長継続",
            "investment_points": ["DX投資"],
            "why_propose_now": "DX投資拡大中のため",
            "expected_challenges": "人材不足",
            "recommended_target": "CIO",
            "dm_catchcopy": "DX推進のご相談",
            "context_indicators": {"expansion": True, "structure": False, "innovation": True},
            "hypothesis_scenario": "DX投資拡大中",
        }
    )
    result = parse_gemini_response(raw_json)
    assert result["summary"] == "業績好調"
    assert result["why_propose_now"] == "DX投資拡大中のため"
    assert result["context_indicators"]["expansion"] is True


def test_parse_gemini_response_handles_invalid_json():
    result = parse_gemini_response("not valid json {{{")
    assert result == {}


@pytest.mark.asyncio
async def test_analyze_company_returns_report():
    mock_sb = MagicMock()
    # Mock ir_documents query
    docs_res = MagicMock()
    docs_res.data = [
        {
            "id": 1,
            "markdown_content": "売上高1000億円 DX投資計画 採用500名",
            "document_type": "有価証券報告書",
        }
    ]
    sel = MagicMock()
    sel.select = MagicMock(return_value=sel)
    sel.eq = MagicMock(return_value=sel)
    sel.order = MagicMock(return_value=sel)
    sel.limit = MagicMock(return_value=sel)
    sel.upsert = MagicMock(return_value=sel)
    sel.execute = MagicMock(return_value=docs_res)
    mock_sb.table = MagicMock(return_value=sel)

    mock_response = '{"summary":"テスト","financial_highlights":{},"profit_factors":[],"risks":[],"outlook":"","investment_points":[],"why_propose_now":"","expected_challenges":"","recommended_target":"","dm_catchcopy":"","context_indicators":{"expansion":false,"structure":false,"innovation":false},"hypothesis_scenario":""}'

    with patch("ir_research.tenant_analyzer.call_gemini", new=AsyncMock(return_value=mock_response)):
        report = await analyze_company(
            supabase=mock_sb,
            company_id=1,
            tenant_id="test-tenant",
            product_description="DXコンサル",
            target_signals=[{"name": "DX投資"}],
            target_roles=["CIO"],
        )
    assert report["summary"] == "テスト"
