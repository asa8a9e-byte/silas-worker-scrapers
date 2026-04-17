"""テナント向け IR 分析（Gemini 呼び出し）."""
from __future__ import annotations

import os

import httpx

_GEMINI_GENERATE_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.0-flash:generateContent"
)


async def call_gemini(prompt: str) -> str:
    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY or GEMINI_API_KEY not set")
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            f"{_GEMINI_GENERATE_URL}?key={api_key}",
            json={"contents": [{"parts": [{"text": prompt}]}]},
        )
        resp.raise_for_status()
        data = resp.json()
        parts = data["candidates"][0]["content"]["parts"]
        return "".join(p.get("text", "") for p in parts)
