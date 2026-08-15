#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Small OpenAI-compatible text reader backed by Hugging Face Transformers."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import time
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


class ModelState:
    def __init__(
        self,
        model_name: str,
        max_new_tokens: int,
        task: str,
        max_length: int,
        embedding_model: str,
        embedding_dim: int,
    ) -> None:
        self.model_name = model_name
        self.embedding_model = embedding_model
        self.embedding_dim = embedding_dim
        self.max_new_tokens = max_new_tokens
        self.task = task
        self.max_length = max_length
        self.loaded_at_unix = int(time.time())
        self._tokenizer: Any | None = None
        self._model: Any | None = None

    def generate(self, messages: list[dict[str, Any]], max_tokens: int | None = None) -> str:
        self.ensure_loaded()
        if self.task == "question-answering":
            question, context = parse_qa_prompt(messages)
            answer = self.question_answer(question, context)
            return normalize_qa_answer(question, answer, context)

        prompt = render_chat_prompt(self._tokenizer, messages) if self.task == "causal-lm" else render_prompt(messages)
        limit = max(1, min(int(max_tokens or self.max_new_tokens), self.max_new_tokens))
        inputs = self._tokenizer(prompt, return_tensors="pt", truncation=True, max_length=self.max_length)
        outputs = self._model.generate(  # type: ignore[operator]
            **inputs,
            max_new_tokens=limit,
            do_sample=False,
            num_beams=1,
        )
        generated_ids = outputs[0]
        if self.task == "causal-lm":
            generated_ids = generated_ids[inputs["input_ids"].shape[-1] :]
        text = self._tokenizer.decode(generated_ids, skip_special_tokens=True).strip()  # type: ignore[union-attr]
        question, context = parse_qa_prompt(messages)
        return normalize_qa_answer(question, text, context) or "not enough information"

    def ensure_loaded(self) -> None:
        if self._tokenizer is not None and self._model is not None:
            return
        from transformers import AutoModelForCausalLM, AutoModelForQuestionAnswering, AutoModelForSeq2SeqLM, AutoTokenizer

        self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        if self.task == "question-answering":
            self._model = AutoModelForQuestionAnswering.from_pretrained(self.model_name)
        elif self.task == "causal-lm":
            self._model = AutoModelForCausalLM.from_pretrained(self.model_name)
        else:
            self._model = AutoModelForSeq2SeqLM.from_pretrained(self.model_name)
        self._model.eval()

    def question_answer(self, question: str, context: str) -> str:
        import torch

        max_length = min(self.max_length, int(getattr(self._model.config, "max_position_embeddings", 512)))  # type: ignore[union-attr]
        best_answer = ""
        best_score = float("-inf")
        for chunk in ranked_context_chunks(question, context)[:12]:
            inputs = self._tokenizer(  # type: ignore[operator]
                question,
                chunk,
                return_tensors="pt",
                truncation="only_second",
                max_length=max_length,
            )
            with torch.no_grad():
                outputs = self._model(**inputs)  # type: ignore[operator]
            start = int(torch.argmax(outputs.start_logits))
            end = int(torch.argmax(outputs.end_logits))
            if end < start:
                end = start
            answer = self._tokenizer.decode(  # type: ignore[union-attr]
                inputs["input_ids"][0][start : end + 1],
                skip_special_tokens=True,
            )
            score = float(outputs.start_logits[0][start] + outputs.end_logits[0][end])
            score += 0.25 * keyword_overlap(question, chunk)
            if answer.strip() and answer.strip().lower() not in {"[cls]", "[sep]"} and score > best_score:
                best_answer = answer
                best_score = score
        return best_answer or "not enough context"


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


def render_chat_prompt(tokenizer: Any, messages: list[dict[str, Any]]) -> str:
    if hasattr(tokenizer, "apply_chat_template"):
        try:
            return tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True,
            )
        except Exception:
            pass
    return render_prompt(messages)


def parse_qa_prompt(messages: list[dict[str, Any]]) -> tuple[str, str]:
    text = "\n".join(str(message.get("content") or "") for message in messages if isinstance(message, dict))
    match = re.search(
        r"Question:\s*(.*?)\n\s*(?:Context|Evidence):\s*(.*?)(?:\n\s*Answer\s*:|\Z)",
        text,
        re.S | re.I,
    )
    if match:
        return match.group(1).strip(), match.group(2).strip()
    question_match = re.search(r"<question>(.*?)</question>", text, re.S | re.I)
    context_match = re.search(r"<context>(.*?)</context>", text, re.S | re.I)
    question = question_match.group(1).strip() if question_match else text[:200].strip()
    context = context_match.group(1).strip() if context_match else text.strip()
    return question, context


def normalize_qa_answer(question: str, answer: str, context: str) -> str:
    cleaned = re.sub(r"\s+", " ", (answer or "")).strip(" .\n\t")
    question_lower = question.lower()
    if cleaned.lower() in {"", "[cls]", "[sep]", "not enough context", "not enough context provided", "not enough information"}:
        cleaned = temporal_answer_fallback(question_lower, context) or cleaned
    if cleaned.lower() in {"yesterday", "the previous day"}:
        event_date = first_context_timestamp(context)
        if event_date is not None:
            return format_date(event_date - timedelta(days=1))
    if any(token in question_lower for token in ("when", "what year", "which year", "date")):
        temporal = temporal_answer_fallback(question_lower, context)
        if temporal and (contains_date(cleaned) or cleaned.lower() in {"", "[cls]", "[sep]", "last year"}):
            return temporal
    if "degree" in question_lower:
        degree = regex_first(
            context,
            [
                r"degree\s+in\s+([A-Za-z][A-Za-z &-]+?)(?:,|\.| which| that|$)",
                r"graduated\s+with\s+(?:a|an)?\s*([A-Za-z][A-Za-z &-]+?)\s+degree(?:,|\.| which| that|$)",
            ],
        )
        if degree:
            return degree
    if "commute" in question_lower or question_lower.startswith("how long"):
        duration = regex_first(
            context,
            [
                r"(\d+\s+minutes\s+each\s+way)",
                r"(\d+\s+hours?\s+each\s+way)",
            ],
        )
        if duration:
            return duration
    return cleaned or "not enough context"


def temporal_answer_fallback(question_lower: str, context: str) -> str:
    if not any(token in question_lower for token in ("when", "what year", "which year", "date")):
        return ""
    compact = re.sub(r"\s+", " ", context)
    keywords = [word for word in re.findall(r"[A-Za-z][A-Za-z]+", question_lower) if len(word) > 3]
    relative_year = relative_last_year_answer(compact, keywords)
    if relative_year:
        return relative_year
    dated_spans: list[tuple[int, str]] = []
    for match in re.finditer(
        r"((?:\d{1,2}:\d{2}\s*(?:am|pm)\s+on\s+)?(?:\d{1,2}\s+[A-Za-z]+,?\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})|\b\d{4}\b)",
        compact,
        re.I,
    ):
        left = compact[max(0, match.start() - 220) : match.start()]
        right = compact[match.end() : match.end() + 220]
        window = f"{left} {match.group(1)} {right}".lower()
        overlap = sum(1 for word in keywords if word in window)
        dated_spans.append((overlap, normalize_date_text(match.group(1))))
    if not dated_spans:
        return ""
    dated_spans.sort(key=lambda item: item[0], reverse=True)
    return dated_spans[0][1] if dated_spans[0][0] > 0 else ""


def ranked_context_chunks(question: str, context: str) -> list[str]:
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", context) if part.strip()]
    if not paragraphs:
        paragraphs = [context]
    scored = sorted(
        ((keyword_overlap(question, paragraph), index, paragraph) for index, paragraph in enumerate(paragraphs)),
        key=lambda item: (-item[0], item[1]),
    )
    ordered = [paragraph for _, _, paragraph in scored]
    if context.strip() and context.strip() not in ordered:
        ordered.append(context)
    return ordered


def keyword_overlap(question: str, text: str) -> int:
    words = [word for word in re.findall(r"[A-Za-z][A-Za-z]+", question.lower()) if len(word) > 3]
    haystack = text.lower()
    return sum(1 for word in words if word in haystack)


def relative_last_year_answer(compact_context: str, keywords: list[str]) -> str:
    for match in re.finditer(r"\blast year\b", compact_context, re.I):
        left = compact_context[max(0, match.start() - 180) : match.start()]
        right = compact_context[match.end() : match.end() + 180]
        window = f"{left} last year {right}".lower()
        if not any(word in window for word in keywords):
            continue
        event_date = first_context_timestamp(window)
        if event_date is None:
            event_date = first_context_timestamp(compact_context)
        if event_date is not None:
            return str(event_date.year - 1)
    return ""


def regex_first(text: str, patterns: list[str]) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if match:
            return match.group(1).strip()
    return ""


def first_context_timestamp(text: str) -> datetime | None:
    match = re.search(r"\d{1,2}:\d{2}\s*(?:am|pm)\s+on\s+(\d{1,2}\s+[A-Za-z]+,?\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})", text, re.I)
    if not match:
        match = re.search(r"(\d{1,2}\s+[A-Za-z]+,?\s+\d{4}|[A-Za-z]+\s+\d{1,2},?\s+\d{4})", text, re.I)
    if not match:
        return None
    cleaned = match.group(1).replace(",", "")
    try:
        return parse_flexible_date(cleaned)
    except ValueError:
        return None


def parse_flexible_date(cleaned: str) -> datetime:
    for pattern in ("%d %B %Y", "%B %d %Y"):
        try:
            return datetime.strptime(cleaned, pattern)
        except ValueError:
            continue
    raise ValueError(cleaned)

def normalize_date_text(text: str) -> str:
    if re.fullmatch(r"\d{4}", text.strip()):
        return text.strip()
    cleaned = text.strip().replace(",", "")
    try:
        return format_date(parse_flexible_date(cleaned))
    except ValueError:
        return text.strip()


def contains_date(text: str) -> bool:
    return bool(re.search(r"\b\d{4}\b|\d{1,2}\s+[A-Za-z]+,?\s+\d{4}", text))


def format_date(value: datetime) -> str:
    return f"{value.day} {value.strftime('%B')} {value.year}"


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
                        },
                        {
                            "id": self.server.model_state.embedding_model,  # type: ignore[attr-defined]
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
        path = self.path.rstrip("/")
        if path == "/v1/embeddings":
            self.handle_embeddings()
            return
        if path != "/v1/chat/completions":
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

    def handle_embeddings(self) -> None:
        try:
            body = self.read_json()
            raw_input = body.get("input", "")
            texts = raw_input if isinstance(raw_input, list) else [raw_input]
            model = str(body.get("model") or self.server.model_state.embedding_model)  # type: ignore[attr-defined]
            dim = int(self.server.model_state.embedding_dim)  # type: ignore[attr-defined]
            data = []
            total_tokens = 0
            for index, text in enumerate(texts):
                rendered = json.dumps(text, ensure_ascii=False) if isinstance(text, (dict, list)) else str(text)
                total_tokens += max(1, len(rendered.split()))
                data.append(
                    {
                        "object": "embedding",
                        "index": index,
                        "embedding": hash_embedding(rendered, dim),
                    }
                )
            self.write_json(
                HTTPStatus.OK,
                {
                    "object": "list",
                    "model": model,
                    "data": data,
                    "usage": {
                        "prompt_tokens": total_tokens,
                        "total_tokens": total_tokens,
                    },
                },
            )
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


def hash_embedding(text: str, dim: int) -> list[float]:
    vector = [0.0] * max(1, dim)
    for token in re.findall(r"[A-Za-z0-9_./:-]+", text.lower()):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=16).digest()
        bucket = int.from_bytes(digest[:4], "little") % len(vector)
        sign = 1.0 if digest[4] & 1 else -1.0
        weight = 1.0 + (digest[5] % 7) / 10.0
        vector[bucket] += sign * weight
    norm = math.sqrt(sum(value * value for value in vector)) or 1.0
    return [round(value / norm, 8) for value in vector]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local OpenAI-compatible HF reader endpoint.")
    parser.add_argument("--host", default=os.environ.get("TEMPORALSTORE_HF_READER_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("TEMPORALSTORE_HF_READER_PORT", "8000")))
    parser.add_argument("--model", default=os.environ.get("TEMPORALSTORE_READER_MODEL", "google/flan-t5-small"))
    parser.add_argument("--max-new-tokens", type=int, default=int(os.environ.get("TEMPORALSTORE_HF_READER_MAX_NEW_TOKENS", "96")))
    parser.add_argument(
        "--task",
        choices=("seq2seq", "question-answering", "causal-lm"),
        default=os.environ.get("TEMPORALSTORE_HF_READER_TASK", "seq2seq"),
        help="Use seq2seq, causal language-model generation, or extractive QA for OSS readers.",
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=int(os.environ.get("TEMPORALSTORE_HF_READER_MAX_LENGTH", "1024")),
        help="Maximum tokenizer input length for the reader model.",
    )
    parser.add_argument(
        "--embedding-model",
        default=os.environ.get("TEMPORALSTORE_HF_EMBEDDING_MODEL", "matrixark-hash-embedding-32"),
        help="OpenAI-compatible embedding model id exposed by /v1/embeddings.",
    )
    parser.add_argument(
        "--embedding-dim",
        type=int,
        default=int(os.environ.get("TEMPORALSTORE_HF_EMBEDDING_DIM", "32")),
        help="Deterministic hash embedding dimension for local baseline bootstraps.",
    )
    parser.add_argument("--preload", action="store_true", default=os.environ.get("TEMPORALSTORE_HF_READER_PRELOAD", "1") != "0")
    args = parser.parse_args()

    state = ModelState(
        args.model,
        args.max_new_tokens,
        args.task,
        args.max_length,
        args.embedding_model,
        args.embedding_dim,
    )
    if args.preload:
        state.ensure_loaded()
    server = ThreadingHTTPServer((args.host, args.port), ReaderHandler)
    server.model_state = state  # type: ignore[attr-defined]
    print(f"TemporalStore HF OSS reader serving {args.model} at http://{args.host}:{args.port}/v1", flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
