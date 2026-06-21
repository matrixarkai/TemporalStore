#!/usr/bin/env python3
"""Small OpenAI-compatible text reader backed by Hugging Face Transformers."""

from __future__ import annotations

import argparse
import json
import os
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class ModelState:
    def __init__(self, model_name: str, max_new_tokens: int) -> None:
        self.model_name = model_name
        self.max_new_tokens = max_new_tokens
        self.loaded_at_unix = int(time.time())
        self._tokenizer: Any | None = None
        self._model: Any | None = None

    def generate(self, messages: list[dict[str, Any]], max_tokens: int | None = None) -> str:
        self.ensure_loaded()
        prompt = render_prompt(messages)
        limit = max(1, min(int(max_tokens or self.max_new_tokens), self.max_new_tokens))
        inputs = self._tokenizer(prompt, return_tensors="pt", truncation=True, max_length=2048)
        outputs = self._model.generate(
            **inputs,
            max_new_tokens=limit,
            do_sample=False,
            num_beams=1,
        )
        text = self._tokenizer.decode(outputs[0], skip_special_tokens=True).strip()
        return text or "not enough information"

    def ensure_loaded(self) -> None:
        if self._tokenizer is not None and self._model is not None:
            return
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

        self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self._model = AutoModelForSeq2SeqLM.from_pretrained(self.model_name)
        self._model.eval()


def render_prompt(messages: list[dict[str, Any]]) -> str:
    system = ""
    user_parts: list[str] = []
    for message in messages:
        role = str(message.get("role") or "").lower()
        content = str(message.get("content") or "").strip()
        if not content:
            continue
        if role == "system":
            system = content
        else:
            user_parts.append(content)
    user = "\n\n".join(user_parts)
    if system:
        return f"{system}\n\n{user}\n\nAnswer:"
    return f"{user}\n\nAnswer:"


class ReaderHandler(BaseHTTPRequestHandler):
    server_version = "TemporalStoreHFReader/1.0"

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API.
        if self.path.rstrip("/") == "/v1/models":
            self.write_json(
                HTTPStatus.OK,
                {
                    "object": "list",
                    "data": [
                        {
                            "id": self.server.model_state.model_name,  # type: ignore[attr-defined]
                            "object": "model",
                            "created": self.server.model_state.loaded_at_unix,  # type: ignore[attr-defined]
                            "owned_by": "matrixark-temporalstore",
                        }
                    ],
                },
            )
            return
        if self.path.rstrip("/") in {"", "/healthz", "/v1/healthz"}:
            self.write_json(HTTPStatus.OK, {"ok": True})
            return
        self.write_json(HTTPStatus.NOT_FOUND, {"error": {"message": f"unknown path {self.path}"}})

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API.
        if self.path.rstrip("/") != "/v1/chat/completions":
            self.write_json(HTTPStatus.NOT_FOUND, {"error": {"message": f"unknown path {self.path}"}})
            return
        try:
            body = self.read_json()
            messages = body.get("messages")
            if not isinstance(messages, list):
                raise ValueError("messages must be a list")
            content = self.server.model_state.generate(messages, body.get("max_tokens"))  # type: ignore[attr-defined]
            response = {
                "id": f"chatcmpl-temporalstore-{int(time.time() * 1000)}",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": body.get("model") or self.server.model_state.model_name,  # type: ignore[attr-defined]
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                },
            }
            self.write_json(HTTPStatus.OK, response)
        except Exception as exc:  # noqa: BLE001 - endpoint must report model/load errors as JSON.
            self.write_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": {"message": str(exc)}})

    def read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length)
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict):
            raise ValueError("request body must be a JSON object")
        return data

    def write_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        if os.environ.get("TEMPORALSTORE_HF_READER_ACCESS_LOG", ""):
            super().log_message(fmt, *args)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local OpenAI-compatible HF reader endpoint.")
    parser.add_argument("--host", default=os.environ.get("TEMPORALSTORE_HF_READER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("TEMPORALSTORE_HF_READER_PORT", "8000")))
    parser.add_argument("--model", default=os.environ.get("TEMPORALSTORE_READER_MODEL", "google/flan-t5-small"))
    parser.add_argument("--max-new-tokens", type=int, default=int(os.environ.get("TEMPORALSTORE_HF_READER_MAX_NEW_TOKENS", "96")))
    parser.add_argument("--preload", action="store_true", default=os.environ.get("TEMPORALSTORE_HF_READER_PRELOAD", "1") != "0")
    args = parser.parse_args()

    state = ModelState(args.model, args.max_new_tokens)
    if args.preload:
        state.ensure_loaded()
    server = ThreadingHTTPServer((args.host, args.port), ReaderHandler)
    server.model_state = state  # type: ignore[attr-defined]
    print(f"TemporalStore HF OSS reader serving {args.model} at http://{args.host}:{args.port}/v1", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
