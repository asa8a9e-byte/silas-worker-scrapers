from ir_research.prompt_templates import (
    FALLBACK_PRODUCT_DESCRIPTION,
    build_financial_analysis_prompt,
    build_risk_analysis_prompt,
    build_strength_analysis_prompt,
)


def test_financial_prompt_includes_tenant_config():
    prompt = build_financial_analysis_prompt(
        ir_text="売上高1000億円",
        product_description="DXコンサル",
        target_signals=["DX投資", "採用強化"],
        target_roles=["CIO", "DX推進室長"],
    )
    assert "DXコンサル" in prompt
    assert "DX投資" in prompt
    assert "CIO" in prompt
    assert "売上高1000億円" in prompt
    assert "summary" in prompt  # JSON出力項目


def test_financial_prompt_fallback_when_no_product():
    prompt = build_financial_analysis_prompt(
        ir_text="テスト",
        product_description=None,
        target_signals=[],
        target_roles=[],
    )
    assert FALLBACK_PRODUCT_DESCRIPTION in prompt


def test_strength_prompt_structure():
    prompt = build_strength_analysis_prompt(ir_text="テスト")
    assert "競争優位性" in prompt or "strengths" in prompt


def test_risk_prompt_structure():
    prompt = build_risk_analysis_prompt(ir_text="テスト")
    assert "リスク" in prompt or "risks" in prompt
