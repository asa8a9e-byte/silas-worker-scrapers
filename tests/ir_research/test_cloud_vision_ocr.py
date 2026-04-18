"""Cloud Vision OCR のモックテスト."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from ir_research.pdf_processor import extract_text_cloud_vision


def test_cloud_vision_returns_text() -> None:
    mock_response = MagicMock()
    mock_response.full_text_annotation.text = "テスト売上高 1,234百万円 DX投資 500億円"

    with patch("ir_research.pdf_processor.vision.ImageAnnotatorClient") as mock_client:
        instance = mock_client.return_value
        instance.document_text_detection.return_value = mock_response
        text = extract_text_cloud_vision(b"fake-pdf-bytes")
    assert "売上高" in text
    assert "DX投資" in text


def test_cloud_vision_empty_response() -> None:
    mock_response = MagicMock()
    mock_response.full_text_annotation = None

    with patch("ir_research.pdf_processor.vision.ImageAnnotatorClient") as mock_client:
        instance = mock_client.return_value
        instance.document_text_detection.return_value = mock_response
        text = extract_text_cloud_vision(b"fake-pdf-bytes")
    assert text == ""
