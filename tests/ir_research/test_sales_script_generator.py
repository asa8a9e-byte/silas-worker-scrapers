# tests/ir_research/test_sales_script_generator.py
import pytest
from unittest.mock import AsyncMock, patch
from ir_research.sales_script_generator import generate_sales_scripts, parse_scripts_response


def test_parse_scripts_response_extracts_3_types():
    raw = '{"sns_dm":"LinkedIn用メッセージ","form":"フォーム営業文","teleapo":{"opening":"受付突破","talk":"30秒トーク","rebuttal":"リバット"}}'
    result = parse_scripts_response(raw)
    assert result["sns_dm"] is not None
    assert result["form"] is not None
    assert result["teleapo"] is not None


def test_parse_scripts_response_handles_bad_json():
    result = parse_scripts_response("invalid json {{{")
    assert result == {"sns_dm": "", "form": "", "teleapo": ""}


@pytest.mark.asyncio
async def test_generate_sales_scripts_returns_3_types():
    mock_analysis = {
        "company_name": "トヨタ自動車",
        "why_propose_now": "DX投資500億円",
        "dm_catchcopy": "DX推進のご相談",
        "hypothesis_scenario": "DX投資拡大中",
        "recommended_target": "CIO",
    }
    mock_response = '{"sns_dm":"テストDM","form":"テストフォーム","teleapo":{"opening":"受付突破","talk":"30秒トーク","rebuttal":"反論対応"}}'

    with patch("ir_research.sales_script_generator.call_gemini", new=AsyncMock(return_value=mock_response)):
        result = await generate_sales_scripts(
            analysis_report=mock_analysis,
            product_description="DXコンサル",
            target_role="CIO",
        )
    assert "sns_dm" in result
    assert "form" in result
    assert "teleapo" in result
