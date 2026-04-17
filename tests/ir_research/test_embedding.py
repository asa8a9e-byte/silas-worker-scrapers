"""OpenAI embedding 生成のモックテスト."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ir_research.pdf_processor import generate_embeddings


@pytest.mark.asyncio
async def test_generate_embeddings_calls_openai() -> None:
    chunks = [
        {"chunk_index": 0, "text": "売上高1000億円", "chunk_start": 0, "chunk_end": 15},
        {"chunk_index": 1, "text": "DX投資500億円", "chunk_start": 15, "chunk_end": 29},
    ]
    mock_embedding = [0.1] * 1536

    with patch("ir_research.pdf_processor.AsyncOpenAI") as mock_cls:
        client = mock_cls.return_value
        response = MagicMock()
        response.data = [
            MagicMock(embedding=mock_embedding),
            MagicMock(embedding=mock_embedding),
        ]
        client.embeddings.create = AsyncMock(return_value=response)

        result = await generate_embeddings(chunks)

    assert len(result) == 2
    assert len(result[0]["embedding"]) == 1536
    assert result[0]["chunk_index"] == 0


@pytest.mark.asyncio
async def test_generate_embeddings_empty_input() -> None:
    result = await generate_embeddings([])
    assert result == []
