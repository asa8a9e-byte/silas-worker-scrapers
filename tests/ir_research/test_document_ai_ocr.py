"""Document AI OCR のモックテスト."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from ir_research.pdf_processor import extract_text_document_ai


@pytest.fixture
def document_ai_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("DOCUMENT_AI_PROCESSOR_ID", "test-processor-id")


def test_document_ai_returns_text(document_ai_env: object) -> None:
    mock_result = MagicMock()
    mock_result.document.text = "テスト売上高 1,234百万円 DX投資 500億円"

    with patch(
        "ir_research.pdf_processor.documentai.DocumentProcessorServiceClient"
    ) as mock_client_cls:
        instance = mock_client_cls.return_value
        instance.processor_path.return_value = (
            "projects/test-project/locations/us/processors/test-processor-id"
        )
        instance.process_document.return_value = mock_result
        text = extract_text_document_ai(b"fake-pdf-bytes")
    assert "売上高" in text
    assert "DX投資" in text


def test_document_ai_empty_response(document_ai_env: object) -> None:
    mock_result = MagicMock()
    mock_result.document = None

    with patch(
        "ir_research.pdf_processor.documentai.DocumentProcessorServiceClient"
    ) as mock_client_cls:
        instance = mock_client_cls.return_value
        instance.processor_path.return_value = (
            "projects/test-project/locations/us/processors/test-processor-id"
        )
        instance.process_document.return_value = mock_result
        text = extract_text_document_ai(b"fake-pdf-bytes")
    assert text == ""
