"""EDINET メタデータパース・フィルタのユニットテスト."""

from __future__ import annotations

import json
from pathlib import Path

from ir_research.edinet_worker import EdinetDoc, is_yuho_or_kessan, parse_edinet_documents_response

FIXTURE = Path(__file__).parent / "fixtures" / "edinet_documents_sample.json"


def test_parse_edinet_returns_docs():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    docs = parse_edinet_documents_response(payload)
    assert len(docs) > 0
    assert isinstance(docs[0], EdinetDoc)


def test_is_yuho_or_kessan_filter():
    yuho = EdinetDoc(
        doc_id="x",
        edinet_code="E00001",
        company_name="X",
        doc_type_code="120",
        doc_type="有価証券報告書",
        filer_name="X",
        submit_date_time="2026-04-15",
        pdf_url="...",
    )
    kessan = EdinetDoc(
        doc_id="y",
        edinet_code="E00001",
        company_name="Y",
        doc_type_code="140",
        doc_type="四半期報告書",
        filer_name="Y",
        submit_date_time="2026-04-15",
        pdf_url="...",
    )
    other = EdinetDoc(
        doc_id="z",
        edinet_code="E00001",
        company_name="Z",
        doc_type_code="999",
        doc_type="その他",
        filer_name="Z",
        submit_date_time="2026-04-15",
        pdf_url="...",
    )
    assert is_yuho_or_kessan(yuho) is True
    assert is_yuho_or_kessan(kessan) is True
    assert is_yuho_or_kessan(other) is False
