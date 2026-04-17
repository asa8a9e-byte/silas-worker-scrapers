"""
IR 解析プロンプトテンプレート.
テナント設定（商材・シグナル・役職）を動的に差し込む.
ローカル版の人事給与ハードコードを撤廃し、完全にテナント駆動.
"""
from __future__ import annotations

FALLBACK_PRODUCT_DESCRIPTION = (
    "（商材を特定せず）一般的な B2B 営業観点で、企業の業績・投資意欲・組織課題を分析し、"
    "営業アプローチの可否を評価してください。recommended_target は経営企画・事業開発・"
    "管理本部など全社横断的な役職を想定してください。"
)


def _format_signals(signals: list[dict] | list[str] | None) -> str:
    if not signals:
        return "（指定なし — 一般的な営業シグナルを検出）"
    items = []
    for s in signals:
        if isinstance(s, dict):
            items.append(f"- {s.get('name', s)}: {s.get('description', '')}")
        else:
            items.append(f"- {s}")
    return "\n".join(items)


def _format_roles(roles: list[str] | None) -> str:
    if not roles:
        return "経営企画部長、管理本部長、事業開発"
    return "、".join(roles)


def build_financial_analysis_prompt(
    ir_text: str,
    product_description: str | None = None,
    target_signals: list | None = None,
    target_roles: list | None = None,
) -> str:
    product = product_description or FALLBACK_PRODUCT_DESCRIPTION
    signals = _format_signals(target_signals)
    roles = _format_roles(target_roles)

    return f"""あなたは経験豊富な投資アナリスト兼営業コンサルタントです。
以下のIR資料を分析し、JSON形式で結果を出力してください。

## 営業文脈
- 提案する商材: {product}
- 重要なシグナル:
{signals}
- 推奨アプローチ役職: {roles}

## 必須項目

1. summary (string): 業績サマリー（200-300文字）
2. financial_highlights (object):
   - revenue: 売上高（百万円）
   - operating_income: 営業利益（百万円）
   - net_income: 純利益（百万円）
   - revenue_growth: 売上高成長率（%）
   - operating_margin: 営業利益率（%）
   - operating_margin_change: 営業利益率の前年比変化（ポイント）
3. profit_factors (array): 利益率向上・低下の要因（3-5個）
4. risks (array): 事業リスク・課題（2-3個）
5. outlook (string): 今後の見通し（100-150文字）
6. investment_points (array): 注目ポイント（3-5個）

## 営業アドバイス項目（B2B営業担当者向け）

7. why_propose_now (string): なぜ今、提案すべきか？（100-150文字）
8. expected_challenges (string): 想定される課題（100-150文字）
9. recommended_target (string): {product}を提案する前提で、{roles}の中から最適な部署・役職を記載
10. dm_catchcopy (string): DMの件名案（30-50文字）
11. context_indicators (object):
    - expansion (boolean): 拡大意欲シグナル
    - structure (boolean): 組織強化シグナル
    - innovation (boolean): 革新意欲シグナル
12. hypothesis_scenario (string): {product}の提案に繋がる営業仮説（300文字以内）

## 出力形式
必ず有効なJSONのみを出力してください。説明文は不要です。

## IR資料
{ir_text}"""


def build_strength_analysis_prompt(
    ir_text: str,
    product_description: str | None = None,
) -> str:
    return f"""あなたは戦略コンサルタントです。
以下のIR資料を分析し、企業の競争優位性をJSON形式で出力してください。

## 必須項目

1. company_overview (string): 企業概要（100-150文字）
2. strengths (array): 企業の強み・競争優位性（5-7個、各50-80文字）
3. competitive_advantages (array): 他社との差別化ポイント（3-5個）
4. core_technologies (array): コア技術・ノウハウ（2-4個）
5. market_position (string): 市場でのポジション（50-100文字）
6. growth_drivers (array): 成長ドライバー（3-5個）

## 出力形式
必ず有効なJSONのみを出力してください。

## IR資料
{ir_text}"""


def build_risk_analysis_prompt(
    ir_text: str,
    product_description: str | None = None,
) -> str:
    return f"""あなたはリスク管理の専門家です。
以下のIR資料を分析し、事業リスクをJSON形式で出力してください。

## 必須項目

1. summary (string): リスク概要（100-150文字）
2. business_risks (array): 事業リスク（3-5個）
   - 各項目: {{"risk": "リスク内容", "impact": "高/中/低", "mitigation": "対策"}}
3. financial_risks (array): 財務リスク（2-3個）
4. market_risks (array): 市場・競合リスク（2-3個）
5. external_risks (array): 外部環境リスク（2-3個）
6. overall_risk_level (string): 総合リスクレベル（"高", "中", "低"）

## 出力形式
必ず有効なJSONのみを出力してください。

## IR資料
{ir_text}"""
