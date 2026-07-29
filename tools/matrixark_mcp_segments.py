#!/usr/bin/env python3
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


def detect_memory_segments(messages: list[Json], envelope: Json | None = None) -> tuple[list[Json], Json]:
    envelope = envelope or {}
    provider = str(envelope.get("segment_provider") or os.getenv("MATRIXARK_SEGMENT_PROVIDER", "deterministic")).strip().lower()
    if provider in {"oss_encoder", "oss-encoder", "embedding"}:
        segments = oss_encoder_memory_segments(messages)
        return segments, {
            "provider": "oss_encoder",
            "execution_mode": "oss_embedding_model",
            "model": embedding_model_name(),
            "fallback_used": False,
            "segment_count": len(segments),
        }
    if provider in {"", "deterministic", "rules", "local"}:
        if require_oss_understanding():
            raise MatrixArkError("deterministic segmentation is disabled because MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1")
        return intelligent_memory_segments(messages), {
            "provider": "deterministic",
            "execution_mode": "rules",
            "model": "matrixark-local-segmentation-v1",
            "fallback_used": False,
        }

    fallback_enabled = bool(envelope.get("segment_provider_fallback", False)) or provider in {"oss-fallback", "oss_with_fallback"} or os.getenv("MATRIXARK_SEGMENT_PROVIDER_FALLBACK", "").lower() in {"1", "true", "yes"}
    if provider in {"oss", "oss-fallback", "oss_with_fallback"}:
        model = str(envelope.get("segment_model") or os.getenv("MATRIXARK_SEGMENT_MODEL", "Qwen/Qwen2.5-0.5B-Instruct"))
        model_path = str(envelope.get("segment_model_path") or os.getenv("MATRIXARK_SEGMENT_MODEL_PATH", ""))
        max_new_tokens = int(envelope.get("segment_max_new_tokens") or os.getenv("MATRIXARK_SEGMENT_MAX_NEW_TOKENS", "512"))
        try:
            raw = oss_model_memory_segments(
                messages,
                model=model,
                model_path=model_path,
                max_new_tokens=max_new_tokens,
                local_only=fallback_enabled,
            )
            segments = normalize_model_segments(raw, messages)
            return segments, {
                "provider": "oss",
                "execution_mode": "oss_model",
                "model": model_path or model,
                "fallback_used": False,
                "segment_count": len(segments),
            }
        except Exception as exc:  # pragma: no cover - optional local model stack.
            if not fallback_enabled:
                raise MatrixArkError(f"OSS segment provider failed: {exc}") from exc
            segments = intelligent_memory_segments(messages)
            return segments, {
                "provider": "oss",
                "execution_mode": "rules_fallback",
                "model": model_path or model,
                "fallback_used": True,
                "fallback_reason": str(exc),
                "segment_count": len(segments),
            }
    raise MatrixArkError("segment_provider must be deterministic, oss, or oss-fallback")


def build_segment_prompt(messages: list[Json]) -> str:
    indexed = "\n".join(f"{index}. {message.get('role', 'user')}: {message.get('content', '')}" for index, message in enumerate(messages))
    return (
        "You are MatrixArk's memory segmentation extractor. Identify high-saliency memory segments from the indexed conversation. "
        "Prune greetings, acknowledgements, and filler. Merge semantically related non-contiguous messages into the same segment. "
        "Return only valid JSON with this shape: "
        '{"segments":[{"topic":"short_snake_case","coordinate_tuples":[[start,end]],"message_indexes":[0],"saliency_score":0.0,"summary_text":"short summary"}]} '
        "Indexes are zero-based and coordinate end is inclusive. Do not include messages that are only filler.\n\n"
        f"Conversation:\n{indexed}\n\nJSON:"
    )


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
        local_only = bool(local_only) or bool(model_path) or os.getenv("MATRIXARK_SEGMENT_MODEL_LOCAL_ONLY", "").lower() in {"1", "true", "yes"}
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


def normalize_model_segments(raw: Any, messages: list[Json]) -> list[Json]:
    if isinstance(raw, list):
        raw_segments = raw
    elif isinstance(raw, dict) and isinstance(raw.get("segments"), list):
        raw_segments = raw["segments"]
    else:
        raise MatrixArkError("OSS segment provider must return {segments:[...]}")
    max_index = len(messages) - 1
    normalized: list[Json] = []
    for raw_segment in raw_segments[:12]:
        if not isinstance(raw_segment, dict):
            continue
        topic = re.sub(r"[^a-z0-9_]+", "_", str(raw_segment.get("topic") or "model_segment").lower()).strip("_") or "model_segment"
        coordinate_tuples = normalize_coordinate_tuples(raw_segment.get("coordinate_tuples"), max_index)
        message_indexes = normalize_message_indexes(raw_segment.get("message_indexes"), coordinate_tuples, max_index)
        if not message_indexes:
            continue
        if not coordinate_tuples:
            coordinate_tuples = contiguous_ranges(message_indexes)
        segment_text = "\n".join(f"{index}: {messages[index].get('content', '')}" for index in message_indexes)
        saliency = raw_segment.get("saliency_score", 0.85)
        try:
            saliency_score = max(0.0, min(1.0, float(saliency)))
        except (TypeError, ValueError):
            saliency_score = 0.85
        summary_text = str(raw_segment.get("summary_text") or summarize_text(segment_text, limit=420))
        normalized.append(
            {
                "topic": topic,
                "coordinate_tuples": coordinate_tuples,
                "message_indexes": message_indexes,
                "saliency_score": round(saliency_score, 6),
                "summary_text": summarize_text(summary_text, limit=420),
                "text": segment_text,
                "non_contiguous": len(coordinate_tuples) > 1,
                "detected_by": "oss_model",
            }
        )
    normalized.sort(key=lambda item: (-item["saliency_score"], item["topic"]))
    return normalized


def normalize_coordinate_tuples(value: Any, max_index: int) -> list[list[int]]:
    ranges: list[list[int]] = []
    if not isinstance(value, list):
        return ranges
    for item in value:
        if not isinstance(item, list) or len(item) != 2:
            continue
        try:
            start = int(item[0])
            end = int(item[1])
        except (TypeError, ValueError):
            continue
        start = max(0, min(max_index, start))
        end = max(0, min(max_index, end))
        if end < start:
            start, end = end, start
        ranges.append([start, end])
    return ranges


def normalize_message_indexes(value: Any, coordinate_tuples: list[list[int]], max_index: int) -> list[int]:
    indexes: set[int] = set()
    if isinstance(value, list):
        for item in value:
            try:
                index = int(item)
            except (TypeError, ValueError):
                continue
            if 0 <= index <= max_index:
                indexes.add(index)
    for start, end in coordinate_tuples:
        indexes.update(range(start, end + 1))
    return sorted(indexes)

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


def infer_segment_topic(text: str) -> str:
    lower = text.lower()
    topic_keywords = [
        ("recursion", ["recursion", "recursive", "base case", "merge sort", "call stack"]),
        ("game_algorithm", ["game", "minimax", "alpha beta", "pathfinding", "npc"]),
        ("preference", ["prefer", "favorite", "likes", "loves"]),
        ("location", ["moved", "moving", "located", "location", "live", "lives", "staying"]),
        ("approval_budget", ["approved", "approval", "budget", "cost", "purchase"]),
        ("incident_runbook", ["incident", "runbook", "alert", "outage", "rollback", "postmortem"]),
        ("task_decision", ["decision", "decided", "owner", "owns", "deadline", "checklist", "reviewer", "require", "requires", "required"]),
        ("metric_sla", ["metric", "latency", "p95", "p99", "qps", "sla", "error rate"]),
        ("plan_status", ["plan", "current", "status", "going to", "will"]),
        ("correction", ["correction", "instead", "wrong", "changed", "updated"]),
    ]
    for topic, keywords in topic_keywords:
        if any(keyword in lower for keyword in keywords):
            return topic
    token_list = [token for token in tokens(text) if len(token) > 4]
    return token_list[0] if token_list else "general"


def contiguous_ranges(indexes: list[int]) -> list[list[int]]:
    if not indexes:
        return []
    ordered = sorted(set(indexes))
    ranges: list[list[int]] = []
    start = previous = ordered[0]
    for value in ordered[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append([start, previous])
        start = previous = value
    ranges.append([start, previous])
    return ranges
