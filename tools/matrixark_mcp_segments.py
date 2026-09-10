#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""MatrixArk memory segmentation helpers."""

from __future__ import annotations

import os
import re
from typing import Any

Json = dict[str, Any]

try:
    from tools.matrixark_mcp_embeddings import embedding_model_name
    from tools.matrixark_mcp_errors import MatrixArkError
    from tools.matrixark_mcp_extraction_provider import parse_first_json_object
    from tools.matrixark_mcp_oss_understanding import oss_encoder_memory_segments, require_oss_understanding
    from tools.matrixark_mcp_scoring import tokens
    from tools.matrixark_mcp_summaries import summarize_text
except ModuleNotFoundError:  # Direct script execution from tools/.
    from matrixark_mcp_embeddings import embedding_model_name
    from matrixark_mcp_errors import MatrixArkError
    from matrixark_mcp_extraction_provider import parse_first_json_object
    from matrixark_mcp_oss_understanding import oss_encoder_memory_segments, require_oss_understanding
    from matrixark_mcp_scoring import tokens
    from matrixark_mcp_summaries import summarize_text


_OSS_SEGMENT_MODEL_CACHE: dict[str, Any] = {}


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from tools.matrixark_mcp_core import detect_memory_segments
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import detect_memory_segments


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import build_segment_prompt
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import build_segment_prompt


def oss_model_memory_segments(messages: list[Json], *, model: str, model_path: str = "", max_new_tokens: int = 512, local_only: bool = False) -> Json:
    try:
        import torch  # type: ignore
        from transformers import AutoModelForCausalLM, AutoTokenizer  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on optional OSS stack.
        raise MatrixArkError("torch and transformers are required for segment_provider=oss") from exc

    target = model_path or model
    cache_key = f"{target}:{max_new_tokens}"
    cached = _OSS_SEGMENT_MODEL_CACHE.get(cache_key)
    if cached is None:
        local_only = bool(local_only) or bool(model_path) or os.getenv("MATRIXARK_SEGMENT_MODEL_LOCAL_ONLY", "").strip().lower() in {"1", "true", "yes", "on"}
        tokenizer = AutoTokenizer.from_pretrained(target, local_files_only=local_only)
        model_obj = AutoModelForCausalLM.from_pretrained(target, local_files_only=local_only)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        model_obj.to(device)
        model_obj.eval()
        cached = {"tokenizer": tokenizer, "model": model_obj, "device": device}
        _OSS_SEGMENT_MODEL_CACHE[cache_key] = cached
    tokenizer = cached["tokenizer"]
    model_obj = cached["model"]
    device = cached["device"]
    prompt = build_segment_prompt(messages)
    if getattr(tokenizer, "chat_template", None):
        chat = [
            {"role": "system", "content": "Return only JSON. No markdown."},
            {"role": "user", "content": prompt},
        ]
        input_ids = tokenizer.apply_chat_template(chat, add_generation_prompt=True, return_tensors="pt").to(device)
        outputs = model_obj.generate(input_ids, max_new_tokens=max_new_tokens, do_sample=False)
        generated = outputs[0][input_ids.shape[-1]:]
        response = tokenizer.decode(generated, skip_special_tokens=True)
    else:
        inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=4096)
        inputs = {key: value.to(device) for key, value in inputs.items()}
        outputs = model_obj.generate(**inputs, max_new_tokens=max_new_tokens, do_sample=False)
        generated = outputs[0][inputs["input_ids"].shape[-1]:]
        response = tokenizer.decode(generated, skip_special_tokens=True)
    return parse_first_json_object(response)


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from tools.matrixark_mcp_core import normalize_model_segments
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import normalize_model_segments


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import normalize_coordinate_tuples
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import normalize_coordinate_tuples


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import normalize_message_indexes
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import normalize_message_indexes

def intelligent_memory_segments(messages: list[Json]) -> list[Json]:
    """Segment a batch into salient, event-centric memories.

    The production provider can emit the same coordinate tuples from one LLM
    call. The local implementation does deterministic semantic saliency and
    topic grouping, including non-contiguous segment consolidation.
    """

    salient: list[tuple[int, Json, str, str, float]] = []
    for index, message in enumerate(messages):
        text = str(message.get("content", ""))
        saliency = semantic_saliency_score(text)
        if saliency < 0.5:
            continue
        topic = infer_segment_topic(text)
        salient.append((index, message, text, topic, saliency))
    grouped: dict[str, list[tuple[int, Json, str, float]]] = {}
    for index, message, text, topic, saliency in salient:
        grouped.setdefault(topic, []).append((index, message, text, saliency))

    segments = []
    for topic, items in grouped.items():
        if not items:
            continue
        coordinate_tuples = contiguous_ranges([item[0] for item in items])
        segment_text = "\n".join(f"{index}: {text}" for index, _message, text, _score in items)
        avg_saliency = sum(item[3] for item in items) / len(items)
        segments.append(
            {
                "topic": topic,
                "coordinate_tuples": coordinate_tuples,
                "message_indexes": [item[0] for item in items],
                "saliency_score": round(avg_saliency, 6),
                "summary_text": summarize_text(segment_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(coordinate_tuples) > 1,
                "segment_origin": "semantic_derived_from_events",
                "derived_from_context_events": True,
            }
        )
    segments.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return segments[:12]


def semantic_saliency_score(text: str) -> float:
    lower = text.lower().strip()
    if not lower:
        return 0.0
    filler = {
        "hi",
        "hello",
        "hey",
        "thanks",
        "thank you",
        "ok",
        "okay",
        "cool",
        "great",
        "sounds good",
    }
    compact = re.sub(r"[^a-z0-9 ]+", "", lower).strip()
    if compact in filler or len(compact) < 8:
        return 0.0
    score = 0.2
    if re.search(r"\b(recursion|base case|merge sort|algorithm|complexity|efficiency|dynamic programming|graph|game)\b", lower):
        score += 0.55
    if re.search(r"\b(prefer|favorite|approved|budget|plan|correction|instead|current|remember|important|moved|moving|located|location|live|lives|staying|deadline|owner|owns|reviewer|checklist|decision|decided|require|requires|required|incident|runbook|alert|outage|rollback|metric|latency|p95|p99|sla|policy|control_state|blocked|blocker)\b", lower):
        score += 0.45
    if re.search(r"\b(is|means|because|therefore|warning|avoid|must|should|cannot|can|require|requires|required|blocked|blocker)\b", lower):
        score += 0.2
    if re.search(r"\b(\d{2,}|monday|tuesday|wednesday|thursday|friday|saturday|sunday|january|february|march|april|may|june|july|august|september|october|november|december)\b", lower):
        score += 0.1
    if len(tokens(text)) >= 8:
        score += 0.15
    return min(score, 1.0)


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import infer_segment_topic
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import infer_segment_topic


try:  # the implementation lives in matrixark_mcp_core; this module re-exports it
    from .matrixark_mcp_core import contiguous_ranges
except ImportError:  # Direct script execution from tools/.
    from matrixark_mcp_core import contiguous_ranges
