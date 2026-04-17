"""有報テキストから役員一覧を抽出する."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


@dataclass
class Executive:
    name: str
    title: str
    role_category: str  # 経営/DX/人事/営業/財務/法務/技術/その他
    career: str = ""


# 役職 → カテゴリ分類
_ROLE_KEYWORDS = {
    "経営": ["代表取締役", "社長", "CEO", "COO", "会長", "副社長"],
    "人事": ["人事", "CHRO", "HR", "採用", "労務"],
    "DX": ["CIO", "CTO", "デジタル", "DX", "情報システム", "IT"],
    "財務": ["CFO", "財務", "経理", "IR"],
    "営業": ["営業", "CSO", "販売", "マーケティング", "CMO"],
    "法務": ["法務", "コンプライアンス", "CLO"],
    "技術": ["技術", "研究", "R&D", "開発"],
}

# 役員行パターン: 「役職 氏名」 or 「役職 氏名 生年月日」
_EXEC_PATTERN = re.compile(
    r"((?:代表)?取締役(?:社長|副社長|会長)?|"
    r"常務取締役|専務取締役|取締役|"
    r"(?:常務|専務)?執行役員|監査役|社外取締役|社外監査役)"
    r"[　\s]*(?:兼[^\s　]*[　\s]*)?"
    r"([^\s　]{1,4})[　\s]+([^\s　]{1,4})"  # 姓 名
)


def _classify_role(title: str) -> str:
    for category, keywords in _ROLE_KEYWORDS.items():
        if any(kw in title for kw in keywords):
            return category
    return "その他"


def extract_executives_from_text(text: str) -> list[Executive]:
    """IR文書テキストから役員を抽出."""
    if not text.strip():
        return []

    results: list[Executive] = []
    seen_names: set[str] = set()

    for match in _EXEC_PATTERN.finditer(text):
        title = match.group(1).strip()
        # 姓名を結合
        family = match.group(2).strip()
        given = match.group(3).strip()
        name = f"{family} {given}"

        if name in seen_names:
            continue
        seen_names.add(name)

        # タイトル前後のコンテキストからカテゴリ判定
        context_start = max(0, match.start() - 50)
        context_end = min(len(text), match.end() + 100)
        context = text[context_start:context_end]
        role_category = _classify_role(context)

        results.append(Executive(
            name=name,
            title=title,
            role_category=role_category,
        ))

    return results


async def extract_and_save_executives(
    supabase: Any,
    company_id: int,
    document_id: int,
    ir_text: str,
    fiscal_year: int,
) -> int:
    """役員抽出 → executives テーブルに upsert. 戻り値は保存件数."""
    execs = extract_executives_from_text(ir_text)
    if not execs:
        return 0

    rows = [
        {
            "company_id": company_id,
            "name": e.name,
            "title": e.title,
            "role_category": e.role_category,
            "career": e.career,
            "fiscal_year": fiscal_year,
            "source_document_id": document_id,
        }
        for e in execs
    ]
    supabase.table("executives").upsert(
        rows, on_conflict="company_id,name,fiscal_year"
    ).execute()
    return len(rows)
