"""テナント設定に基づく IR 文書の Gemini 解析."""
from __future__ import annotations

import json
import os
import time
from typing import Any

import httpx

from .prompt_templates import build_financial_analysis_prompt

GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
DEFAULT_MODEL = "gemini-2.5-flash"


async def call_gemini(prompt: str, model: str = DEFAULT_MODEL) -> str:
    """Gemini API 呼び出し. JSON テキストを返す."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    url = GEMINI_API_URL.format(model=model)
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseMimeType": "application/json"},
    }
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(f"{url}?key={api_key}", json=payload)
        resp.raise_for_status()
        data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]


def parse_gemini_response(raw: str) -> dict[str, Any]:
    """Gemini レスポンス JSON をパース. 失敗時は空 dict."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, KeyError):
        return {}


def _compute_3axis_scores(parsed: dict[str, Any]) -> dict[str, Any]:
    """3軸営業意欲スコアを計算 (仕様書 §7.5)."""
    ci = parsed.get("context_indicators", {})

    financial = 50.0  # ベース（財務データがあれば加算）
    fh = parsed.get("financial_highlights", {})
    if fh.get("revenue_growth") and float(fh["revenue_growth"]) > 5:
        financial += 20
    if fh.get("operating_margin_change") and float(fh["operating_margin_change"]) > 0:
        financial += 15
    if fh.get("operating_margin") and float(fh["operating_margin"]) > 10:
        financial += 15
    financial = min(financial, 100)

    action = 0.0
    pf = parsed.get("profit_factors", [])
    if pf:
        action += 20
    if ci.get("expansion"):
        action += 30
    if ci.get("structure"):
        action += 25
    if ci.get("innovation"):
        action += 25
    action = min(action, 100)

    context = 0.0
    if parsed.get("why_propose_now"):
        context += 40
    if parsed.get("hypothesis_scenario"):
        context += 30
    if ci.get("expansion") or ci.get("innovation"):
        context += 30
    context = min(context, 100)

    total = financial * 0.3 + action * 0.4 + context * 0.3
    rank = "高" if total >= 70 else ("中" if total >= 40 else "低")

    return {
        "financial_score": round(financial, 2),
        "action_score": round(action, 2),
        "context_score": round(context, 2),
        "sales_intent_score": round(total, 2),
        "intent_rank": rank,
        "intent_reason": parsed.get("why_propose_now", ""),
    }


async def analyze_company(
    supabase: Any,
    company_id: int,
    tenant_id: str,
    product_description: str | None = None,
    target_signals: list | None = None,
    target_roles: list | None = None,
    config_version: int = 1,
) -> dict[str, Any]:
    """1社の IR 文書を Gemini で解析し ir_analysis_reports に保存."""
    # 最新 IR 文書のテキスト取得
    res = (
        supabase.table("ir_documents")
        .select("id, markdown_content, document_type")
        .eq("company_id", company_id)
        .order("published_at", desc=True)
        .limit(1)
        .execute()
    )
    docs = res.data or []
    if not docs or not docs[0].get("markdown_content"):
        return {"error": "no IR text available"}

    ir_text = docs[0]["markdown_content"][:15000]  # Gemini 入力制限
    doc_type = docs[0].get("document_type", "")

    prompt = build_financial_analysis_prompt(
        ir_text=ir_text,
        product_description=product_description,
        target_signals=target_signals,
        target_roles=target_roles,
    )

    start = time.time()
    raw_response = await call_gemini(prompt)
    elapsed = time.time() - start

    parsed = parse_gemini_response(raw_response)
    scores = _compute_3axis_scores(parsed)

    fh = parsed.get("financial_highlights", {})
    report = {
        "tenant_id": tenant_id,
        "company_id": company_id,
        "analysis_type": "財務分析",
        "analysis_date": time.strftime("%Y-%m-%d"),
        "config_version": config_version,
        "source_doc_type": doc_type,
        "revenue": fh.get("revenue"),
        "operating_income": fh.get("operating_income"),
        "net_income": fh.get("net_income"),
        "revenue_growth": fh.get("revenue_growth"),
        "operating_margin": fh.get("operating_margin"),
        "operating_margin_change": fh.get("operating_margin_change"),
        "summary": parsed.get("summary"),
        "outlook": parsed.get("outlook"),
        "profit_factors": parsed.get("profit_factors"),
        "risks": parsed.get("risks"),
        "investment_points": parsed.get("investment_points"),
        "why_propose_now": parsed.get("why_propose_now"),
        "expected_challenges": parsed.get("expected_challenges"),
        "recommended_target": parsed.get("recommended_target"),
        "dm_catchcopy": parsed.get("dm_catchcopy"),
        "hypothesis_scenario": parsed.get("hypothesis_scenario"),
        "context_indicators": parsed.get("context_indicators"),
        "raw_json": parsed,
        "model_used": DEFAULT_MODEL,
        "processing_time_seconds": round(elapsed, 2),
        **scores,
    }

    # upsert ir_analysis_reports
    supabase.table("ir_analysis_reports").upsert(
        report, on_conflict="tenant_id,company_id,analysis_type,analysis_date"
    ).execute()

    return report
