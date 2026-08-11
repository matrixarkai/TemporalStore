#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""OpenAI-compatible JSON extraction provider helpers."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

try:
    from tools.matrixark_mcp_errors import MatrixArkError
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_errors import MatrixArkError


Json = dict[str, Any]

EXTRACTION_LLM_MODEL = os.environ.get(
    "MATRIXARK_EXTRACTION_MODEL",
    os.environ.get("OPENAI_MODEL", "qwen2.5:1.5b"),
)
EXTRACTION_LLM_BASE_URL = os.environ.get(
    "MATRIXARK_EXTRACTION_BASE_URL",
    os.environ.get("OPENAI_BASE_URL", "http://127.0.0.1:8000/v1"),
).rstrip("/")
EXTRACTION_LLM_API_KEY_ENV = os.environ.get("MATRIXARK_EXTRACTION_API_KEY_ENV", "OPENAI_API_KEY")
EXTRACTION_LLM_TIMEOUT_SEC = float(os.environ.get("MATRIXARK_EXTRACTION_TIMEOUT_SEC", "30"))
EXTRACTION_LLM_MAX_TOKENS = int(os.environ.get("MATRIXARK_EXTRACTION_MAX_TOKENS", "1200"))


def parse_first_json_object(text: str) -> Json:
    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            value, _end = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise MatrixArkError("model response did not contain a JSON object")


def openai_compatible_json_call(*, system: str, user: str, model: str | None = None, max_tokens: int | None = None) -> Json:
    payload = {
        "model": model or EXTRACTION_LLM_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": max_tokens or EXTRACTION_LLM_MAX_TOKENS,
        "response_format": {"type": "json_object"},
    }
    headers = {"Content-Type": "application/json"}
    api_key = os.environ.get(EXTRACTION_LLM_API_KEY_ENV, "")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(
        f"{EXTRACTION_LLM_BASE_URL}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=EXTRACTION_LLM_TIMEOUT_SEC) as response:
            data = json.loads(response.read().decode("utf-8"))
        content = str(data.get("choices", [{}])[0].get("message", {}).get("content", "")).strip()
        if not content:
            raise MatrixArkError("OpenAI-compatible extraction provider returned empty content")
        return parse_first_json_object(content)
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
        OSError,
        json.JSONDecodeError,
        MatrixArkError,
    ) as exc:
        raise MatrixArkError(f"OpenAI-compatible extraction provider failed: {exc}") from exc
