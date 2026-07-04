#!/usr/bin/env python3
"""Dependency-free OpenAI-compatible reader for local benchmark unblocking.

This endpoint is intentionally small: it exposes the same `/v1/models` and
`/v1/chat/completions` shape as Ollama/vLLM/OpenAI-compatible servers, then
uses MatrixArk's existing extractive benchmark reader over the supplied context.
It is useful when a real Qwen/Ollama runtime is not installed, because the
benchmark can still prove the live endpoint path without deterministic fallback.
Do not market its scores as Qwen model quality.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

from run_locomo_ingest_once import extractive_reader_answer  # noqa: E402


def _compact(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def parse_reader_prompt(messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, str]]]:
    """Extract the benchmark question and context blocks from chat messages."""

    user_text = "\n\n".join(
        str(message.get("content") or "")
        for message in messages
        if str(message.get("role") or "").lower() != "system"
    )
    question = ""
    context = user_text
    match = re.search(r"Question:\s*(.*?)\n\s*Context:\s*(.*?)\n\s*Answer:\s*$", user_text, re.S | re.I)
    if match:
        question = _compact(match.group(1))
        context = match.group(2)
    else:
        question_match = re.search(r"Question:\s*(.*)", user_text, re.I)
        if question_match:
            question = _compact(question_match.group(1))
    if not question:
        question = _compact(user_text.splitlines()[0] if user_text.splitlines() else user_text)

    blocks: list[dict[str, str]] = []
    for chunk in re.split(r"\n{2,}|\n(?=(?:user|assistant|system|turn|D\d+:)\b)", context):
        body = _compact(chunk)
        if body:
            blocks.append({"body": body})
    if not blocks and context.strip():
        blocks.append({"body": _compact(context)})
    return question, blocks


class TinyReaderState:
    def __init__(self, model: str) -> None:
        self.model = model
        self.loaded_at_unix = int(time.time())
        self.calls = 0

    def complete(self, messages: list[dict[str, Any]]) -> str:
        question, blocks = parse_reader_prompt(messages)
        self.calls += 1
        return extractive_reader_answer(question, blocks)


class TinyReaderHandler(BaseHTTPRequestHandler):
    server_version = "MatrixArkTinyOSSReader/1.0"

    def do_GET(self) -> None:  # noqa: N802
        path = self.path.rstrip("/")
        if path == "/v1/models":
            state: TinyReaderState = self.server.state  # type: ignore[attr-defined]
            self.write_json(
                HTTPStatus.OK,
                {
                    "object": "list",
                    "data": [
                        {
                            "id": state.model,
                            "object": "model",
                            "created": state.loaded_at_unix,
                            "owned_by": "matrixark-temporalstore",
                        }
                    ],
                },
            )
            return
        if path in {"", "/healthz", "/v1/healthz"}:
            state: TinyReaderState = self.server.state  # type: ignore[attr-defined]
            self.write_json(HTTPStatus.OK, {"ok": True, "model": state.model, "calls": state.calls})
            return
        self.write_json(HTTPStatus.NOT_FOUND, {"error": {"message": f"unknown path {self.path}"}})

    def do_POST(self) -> None:  # noqa: N802
        if self.path.rstrip("/") != "/v1/chat/completions":
            self.write_json(HTTPStatus.NOT_FOUND, {"error": {"message": f"unknown path {self.path}"}})
            return
        try:
            body = self.read_json()
            messages = body.get("messages")
            if not isinstance(messages, list):
                raise ValueError("messages must be a list")
            state: TinyReaderState = self.server.state  # type: ignore[attr-defined]
            content = state.complete(messages)
            now = int(time.time())
            self.write_json(
                HTTPStatus.OK,
                {
                    "id": f"chatcmpl-matrixark-tiny-{int(time.time() * 1000)}",
                    "object": "chat.completion",
                    "created": now,
                    "model": str(body.get("model") or state.model),
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": content},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                },
            )
        except Exception as exc:  # noqa: BLE001
            self.write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": {"message": f"{type(exc).__name__}: {exc}"}})

    def read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        data = json.loads(self.rfile.read(length).decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("request body must be a JSON object")
        return data

    def write_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("MATRIXARK_TINY_READER_ACCESS_LOG"):
            super().log_message(fmt, *args)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local MatrixArk OpenAI-compatible tiny OSS reader.")
    parser.add_argument("--host", default=os.environ.get("MATRIXARK_TINY_READER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MATRIXARK_TINY_READER_PORT", "11434")))
    parser.add_argument("--model", default=os.environ.get("MATRIXARK_TINY_READER_MODEL", "matrixark-tiny-oss-reader"))
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), TinyReaderHandler)
    server.state = TinyReaderState(args.model)  # type: ignore[attr-defined]
    print(f"MatrixArk tiny OSS reader serving {args.model} at http://{args.host}:{args.port}/v1", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
