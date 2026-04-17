"""IR分析結果から営業スクリプト3種を Gemini で生成."""
from __future__ import annotations

import json
from typing import Any

from .tenant_analyzer import call_gemini

SCRIPT_PROMPT = """あなたはトップセールスの営業スクリプトライターです。
以下のIR分析結果と企業情報を元に、3種類の営業スクリプトをJSON形式で生成してください。

## 営業文脈
- 提案する商材: {product_description}
- 企業名: {company_name}
- 推奨アプローチ先: {target_role}
- なぜ今提案すべきか: {why_propose_now}
- DM件名案: {dm_catchcopy}
- 仮説シナリオ: {hypothesis_scenario}

## 生成する3種類

1. **sns_dm** (string): LinkedIn/Facebook用SNSメッセージ（100〜150文字）
   - 相手の役職に合わせた丁寧な敬語
   - 仮説に触れて興味を引く
   - 軽い提案（「情報交換させていただければ」等）

2. **form** (string): お問い合わせフォーム送信文（約200文字）
   - 自社紹介 + 仮説 + 具体的メリット + CTA
   - ビジネスメール体裁

3. **teleapo** (object):
   - opening (string): 受付突破トーク（30文字以内、「○○のご担当者様をお願いいたします」形式）
   - talk (string): 30秒トーク（100文字以内、要件+メリットを簡潔に）
   - rebuttal (string): よくある断り文句への対応（50文字以内）

## 出力形式
必ず有効なJSONのみを出力してください。
"""


def parse_scripts_response(raw: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
        teleapo = parsed.get("teleapo", {})
        if isinstance(teleapo, dict):
            teleapo_text = f"【受付突破】\n{teleapo.get('opening', '')}\n\n【30秒トーク】\n{teleapo.get('talk', '')}\n\n【リバット】\n{teleapo.get('rebuttal', '')}"
        else:
            teleapo_text = str(teleapo)
        return {
            "sns_dm": parsed.get("sns_dm", ""),
            "form": parsed.get("form", ""),
            "teleapo": teleapo_text,
        }
    except (json.JSONDecodeError, KeyError):
        return {"sns_dm": "", "form": "", "teleapo": ""}


async def generate_sales_scripts(
    analysis_report: dict,
    product_description: str = "",
    target_role: str = "",
) -> dict[str, str]:
    """3種営業スクリプトを Gemini で生成."""
    prompt = SCRIPT_PROMPT.format(
        product_description=product_description or "（未設定）",
        company_name=analysis_report.get("company_name", ""),
        target_role=target_role or analysis_report.get("recommended_target", "経営企画"),
        why_propose_now=analysis_report.get("why_propose_now", ""),
        dm_catchcopy=analysis_report.get("dm_catchcopy", ""),
        hypothesis_scenario=analysis_report.get("hypothesis_scenario", ""),
    )
    raw = await call_gemini(prompt)
    return parse_scripts_response(raw)


async def generate_and_cache_scripts(
    supabase: Any,
    tenant_id: str,
    company_id: int,
    analysis_report_id: int,
    analysis_report: dict,
    product_description: str = "",
    target_role: str = "",
    config_version: int = 1,
) -> dict[str, str]:
    """生成 + sales_scripts_cache に保存."""
    scripts = await generate_sales_scripts(
        analysis_report=analysis_report,
        product_description=product_description,
        target_role=target_role,
    )
    supabase.table("sales_scripts_cache").upsert({
        "tenant_id": tenant_id,
        "company_id": company_id,
        "analysis_report_id": analysis_report_id,
        "sns_dm_script": scripts["sns_dm"],
        "form_script": scripts["form"],
        "teleapo_script": scripts["teleapo"],
        "config_version": config_version,
    }, on_conflict="tenant_id,company_id,analysis_report_id").execute()
    return scripts
