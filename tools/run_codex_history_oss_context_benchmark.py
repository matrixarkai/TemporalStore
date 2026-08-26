#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
from __future__ import annotations

import argparse
import collections
import hashlib
import html
import json
import re
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_QUERIES = [
    "What did the user ask about Rust TemporalStore hooks and real-time ingestion?",
    "What did the user ask about and Rust TemporalStore parity?",
    "What did the user ask about Windows Docker installation for TemporalStore?",
    "What did the user ask about context management data fields and compaction?",
    "What did the user ask about Qwen, Ollama, vLLM, LoCoMo, LongMemEval, and ExternalBaseline benchmarking?",
    "What did the user ask about MatrixObject, shared storage, S3, and object storage?",
]


STOPWORDS = {
    "about",
    "above",
    "after",
    "again",
    "also",
    "because",
    "before",
    "between",
    "both",
    "code",
    "context",
    "could",
    "done",
    "from",
    "have",
    "hook",
    "into",
    "make",
    "more",
    "need",
    "other",
    "query",
    "should",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "with",
    "would",
    "your",
}


@dataclass
class ContextEvent:
    event_id: str
    timestamp: str
    timestamp_ms: int
    session_id: str
    source_file: str
    line_no: int
    role: str
    text: str


@dataclass
class ContextSegment:
    segment_id: str
    session_id: str
    start_ms: int
    end_ms: int
    event_ids: list[str]
    text: str


@dataclass
class ContextEntity:
    entity_id: str
    entity_type: str
    name: str
    session_ids: list[str]
    support_event_ids: list[str]
    state: str


@dataclass
class ContextSummary:
    summary_id: str
    summary_type: str
    scope: str
    source_ids: list[str]
    text: str


def stable_id(*parts: Any) -> str:
    payload = "\x1f".join(str(part) for part in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]


def parse_timestamp_ms(value: str) -> int:
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except Exception:
        return 0


def compact(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def token_estimate(text: str) -> int:
    return max(1, (len(text) + 3) // 4) if text else 0


def extract_message_text(payload: dict[str, Any]) -> tuple[str, str]:
    role = str(payload.get("role") or "")
    parts: list[str] = []
    content = payload.get("content")
    if isinstance(content, str):
        parts.append(content)
    elif isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
            elif isinstance(item, dict) and isinstance(item.get("content"), str):
                parts.append(item["content"])
    return role, compact(" ".join(parts))


def is_visible_real_user_prompt(text: str) -> bool:
    if not text:
        return False
    stripped = text.strip()
    if stripped.startswith("<environment_context"):
        return False
    if stripped.startswith("<permissions instructions"):
        return False
    if stripped.startswith("<in-app-browser-context"):
        return False
    if stripped.startswith("<codex_internal_context"):
        return False
    if stripped.startswith("<codex_delegation"):
        return False
    if stripped.startswith("<recommended_plugins"):
        return False
    if stripped.startswith("# Files mentioned by the user:"):
        return False
    return len(stripped) >= 8


def iter_recent_codex_events(log_root: Path, max_sessions: int, max_messages_per_session: int) -> list[ContextEvent]:
    files = sorted(log_root.rglob("*.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)[:max_sessions]
    events: list[ContextEvent] = []
    for path in files:
        match = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$", path.stem)
        session_id = match.group(1) if match else path.stem
        count_for_session = 0
        try:
            handle = path.open("r", encoding="utf-8", errors="replace")
        except OSError:
            continue
        with handle:
            for line_no, line in enumerate(handle, 1):
                if count_for_session >= max_messages_per_session:
                    break
                if '"role":"user"' not in line and '"role": "user"' not in line:
                    continue
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if item.get("type") != "response_item":
                    continue
                payload = item.get("payload")
                if not isinstance(payload, dict) or payload.get("type") != "message":
                    continue
                role, text = extract_message_text(payload)
                if role != "user" or not is_visible_real_user_prompt(text):
                    continue
                ts = str(item.get("timestamp") or "")
                ts_ms = parse_timestamp_ms(ts)
                events.append(
                    ContextEvent(
                        event_id=stable_id(path.name, line_no, ts, text),
                        timestamp=ts,
                        timestamp_ms=ts_ms,
                        session_id=session_id,
                        source_file=str(path),
                        line_no=line_no,
                        role=role,
                        text=text,
                    )
                )
                count_for_session += 1
    return sorted(events, key=lambda event: event.timestamp_ms)


def build_segments(events: list[ContextEvent], chunk_size: int) -> list[ContextSegment]:
    by_session: dict[str, list[ContextEvent]] = collections.defaultdict(list)
    for event in events:
        by_session[event.session_id].append(event)
    segments: list[ContextSegment] = []
    for session_id, session_events in by_session.items():
        for offset in range(0, len(session_events), chunk_size):
            chunk = session_events[offset : offset + chunk_size]
            if not chunk:
                continue
            text = "\n".join(f"user: {event.text}" for event in chunk)
            segments.append(
                ContextSegment(
                    segment_id=stable_id("segment", session_id, offset, chunk[0].event_id, chunk[-1].event_id),
                    session_id=session_id,
                    start_ms=chunk[0].timestamp_ms,
                    end_ms=chunk[-1].timestamp_ms,
                    event_ids=[event.event_id for event in chunk],
                    text=text,
                )
            )
    return segments


def keywords(text: str) -> list[str]:
    words = re.findall(r"[A-Za-z][A-Za-z0-9_+.-]{2,}", text.lower())
    return [word for word in words if word not in STOPWORDS][:80]


def build_entities(events: list[ContextEvent], max_entities: int) -> list[ContextEntity]:
    topic_terms = {
        "rust": "rust_temporalstore",
        "native": "native_temporalstore",
        "native": "native_temporalstore",
        "temporalstore": "temporalstore",
        "hook": "codex_hook",
        "codex": "codex_hook",
        "qwen": "oss_reader_benchmark",
        "ollama": "oss_reader_benchmark",
        "vllm": "oss_reader_benchmark",
        "locomo": "memory_benchmark",
        "longmemeval": "memory_benchmark",
        "external_baseline": "memory_benchmark",
        "windows": "windows_docker_install",
        "docker": "windows_docker_install",
        "matrixobject": "matrixobject_shared_storage",
        "s3": "matrixobject_shared_storage",
        "storage": "storage_engine",
        "context": "context_management",
    }
    buckets: dict[str, list[ContextEvent]] = collections.defaultdict(list)
    for event in events:
        low = event.text.lower()
        for term, entity in topic_terms.items():
            if term in low:
                buckets[entity].append(event)
    entities: list[ContextEntity] = []
    for name, support in sorted(buckets.items(), key=lambda item: len(item[1]), reverse=True)[:max_entities]:
        sessions = sorted({event.session_id for event in support})
        latest = support[-1].text if support else ""
        entities.append(
            ContextEntity(
                entity_id=stable_id("entity", name),
                entity_type="topic",
                name=name,
                session_ids=sessions,
                support_event_ids=[event.event_id for event in support[-16:]],
                state=f"{name} appears in {len(support)} historical Codex user prompts; latest: {latest[:220]}",
            )
        )
    return entities


def build_summaries(events: list[ContextEvent], segments: list[ContextSegment], entities: list[ContextEntity]) -> list[ContextSummary]:
    summaries: list[ContextSummary] = []
    by_session: dict[str, list[ContextEvent]] = collections.defaultdict(list)
    for event in events:
        by_session[event.session_id].append(event)
    for session_id, session_events in by_session.items():
        sample = " ".join(event.text for event in session_events[-5:])
        summaries.append(
            ContextSummary(
                summary_id=stable_id("summary", "session_l0", session_id),
                summary_type="session_l0",
                scope=f"session:{session_id}",
                source_ids=[event.event_id for event in session_events[-12:]],
                text=f"Session {session_id} has {len(session_events)} user prompts. Recent focus: {sample[:500]}",
            )
        )
    user_entity_text = "; ".join(f"{entity.name}: {entity.state}" for entity in entities[:8])
    summaries.append(
        ContextSummary(
            summary_id=stable_id("summary", "user_l1", len(events), len(segments)),
            summary_type="user_l1",
            scope="tenant:tenant_codex/user:local_user",
            source_ids=[entity.entity_id for entity in entities[:8]],
            text=f"Cross-session user memory synthesized from child session summaries and selected topic state: {user_entity_text[:900]}",
        )
    )
    return summaries


def score_text(query_terms: set[str], text: str, recency_rank: int) -> float:
    terms = set(keywords(text))
    overlap = len(query_terms & terms)
    return overlap * 10.0 + max(0.0, 5.0 - recency_rank * 0.01)


def retrieve_pack(query: str, events: list[ContextEvent], segments: list[ContextSegment], entities: list[ContextEntity], summaries: list[ContextSummary], token_budget: int) -> dict[str, Any]:
    q_terms = set(keywords(query))
    candidates: list[tuple[float, str, str, str]] = []
    recent_events = list(reversed(events))
    for rank, event in enumerate(recent_events):
        candidates.append((score_text(q_terms, event.text, rank), "event", event.event_id, event.text))
    for rank, segment in enumerate(reversed(segments)):
        candidates.append((score_text(q_terms, segment.text, rank), "segment", segment.segment_id, segment.text))
    for rank, entity in enumerate(entities):
        candidates.append((score_text(q_terms, entity.name + " " + entity.state, rank), "entity", entity.entity_id, entity.state))
    for rank, summary in enumerate(summaries):
        candidates.append((score_text(q_terms, summary.text, rank), "summary", summary.summary_id, summary.text))
    selected = []
    used_tokens = 0
    for score, ref_type, ref_id, text in sorted(candidates, reverse=True):
        if score <= 0:
            continue
        cost = token_estimate(text)
        if used_tokens + cost > token_budget:
            remaining = token_budget - used_tokens
            if remaining <= 0:
                continue
            text = text[: remaining * 4]
            cost = token_estimate(text)
        if used_tokens + cost > token_budget:
            continue
        selected.append({"score": round(score, 3), "ref_type": ref_type, "ref_id": ref_id, "tokens": cost, "text": text[:1200]})
        used_tokens += cost
        if used_tokens >= token_budget:
            break
    return {
        "query": query,
        "token_budget": token_budget,
        "used_tokens": used_tokens,
        "selected_refs": selected,
        "source_tokens": sum(token_estimate(event.text) for event in events),
    }


def provider_candidates(args: argparse.Namespace) -> list[dict[str, str]]:
    if args.reader_base_url:
        return [{"name": args.reader_provider, "base_url": args.reader_base_url.rstrip("/"), "model": args.reader_model}]
    return [
        {"name": "ollama", "base_url": "http://127.0.0.1:11434/v1", "model": args.reader_model or "qwen2.5:7b"},
        {"name": "vllm", "base_url": "http://127.0.0.1:8000/v1", "model": args.reader_model or "Qwen/Qwen2.5-7B-Instruct"},
    ]


def openai_get_models(provider: dict[str, str], timeout: float) -> dict[str, Any]:
    url = provider["base_url"] + "/models"
    req = urllib.request.Request(url, headers={"authorization": "Bearer local"})
    try:
        data = urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8")
        payload = json.loads(data)
        models = payload.get("data") if isinstance(payload, dict) else None
        ready = isinstance(models, list) and len(models) > 0
        error = None if ready else "models endpoint returned no available models"
        return {"ready": ready, "models": models, "error": error}
    except Exception as exc:
        return {"ready": False, "models": None, "error": f"{type(exc).__name__}: {exc}"}


def call_openai_chat(provider: dict[str, str], query: str, context: str, timeout: float, max_tokens: int) -> dict[str, Any]:
    prompt = (
        "Answer the question using only the visible context. "
        "If the answer is not in context, say what is missing.\n\n"
        f"Question: {query}\n\nContext:\n{context}\n\nAnswer:"
    )
    body = {
        "model": provider["model"],
        "messages": [
            {"role": "system", "content": "You are a concise memory benchmark reader."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    start = time.perf_counter()
    req = urllib.request.Request(
        provider["base_url"] + "/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={"content-type": "application/json", "authorization": "Bearer local"},
    )
    try:
        payload = json.loads(urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8"))
        content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
        return {"ok": True, "latency_ms": round((time.perf_counter() - start) * 1000, 3), "answer": compact(str(content)), "error": None}
    except Exception as exc:
        return {"ok": False, "latency_ms": round((time.perf_counter() - start) * 1000, 3), "answer": "", "error": f"{type(exc).__name__}: {exc}"}


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Codex History OSS Context Benchmark",
        "",
        f"Generated at `{report['generated_at']}`.",
        "",
        "## Summary",
        "",
        f"- Historical Codex sessions scanned: `{report['ingestion']['sessions_seen']}`.",
        f"- Real user prompts ingested into ContextEvent: `{report['ingestion']['context_event_count']}`.",
        f"- ContextSegments generated: `{report['ingestion']['context_segment_count']}`.",
        f"- ContextEntities generated: `{report['ingestion']['context_entity_count']}`.",
        f"- ContextSummaries generated: `{report['ingestion']['context_summary_count']}`.",
        f"- Query count: `{len(report['queries'])}`.",
        f"- Per-query token budget: `{report['token_budget']}`.",
        "",
        "## Reader Runtime",
        "",
    ]
    for provider in report["providers"]:
        status = "ready" if provider["ready"] else "blocked"
        lines.append(f"- `{provider['name']}` at `{provider['base_url']}` model `{provider['model']}`: **{status}**; error: `{provider.get('error')}`")
    lines.extend([
        "",
        "No deterministic answer templates are used. If all OSS endpoints are blocked, answer rows are marked blocked instead of falling back.",
        "",
        "## Token Budget And Savings",
        "",
        "|Query|Source tokens|ContextPack tokens|Token reduction|Selected refs|",
        "|---|---:|---:|---:|---:|",
    ])
    for item in report["queries"]:
        source = item["context_pack"]["source_tokens"]
        used = item["context_pack"]["used_tokens"]
        reduction = 0.0 if source == 0 else 100.0 * (1.0 - used / source)
        lines.append(f"|{item['query']}|{source}|{used}|{reduction:.2f}%|{len(item['context_pack']['selected_refs'])}|")
    lines.extend(["", "## OSS Reader Answers", "", "|Provider|Query|With Context|Without Context|Errors|", "|---|---|---|---|---|"])
    for item in report["queries"]:
        for answer in item["answers"]:
            lines.append(
                "|"
                + "|".join(
                    [
                        answer["provider"],
                        item["query"],
                        (answer["with_context"]["answer"] or "BLOCKED")[:260].replace("|", "\\|"),
                        (answer["without_context"]["answer"] or "BLOCKED")[:260].replace("|", "\\|"),
                        compact(str(answer["with_context"].get("error") or answer["without_context"].get("error") or ""))[:180].replace("|", "\\|"),
                    ]
                )
                + "|"
            )
    lines.extend(["", "## Data Model Samples", "", "### ContextEvents", ""])
    for event in report["samples"]["context_events"]:
        lines.append(f"- `{event['event_id']}` `{event['session_id']}` `{event['timestamp']}`: {event['text'][:260]}")
    lines.extend(["", "### ContextSegments", ""])
    for segment in report["samples"]["context_segments"]:
        lines.append(f"- `{segment['segment_id']}` `{segment['session_id']}` events={len(segment['event_ids'])}: {segment['text'][:260]}")
    lines.extend(["", "### ContextEntities", ""])
    for entity in report["samples"]["context_entities"]:
        lines.append(f"- `{entity['name']}` sessions={len(entity['session_ids'])}: {entity['state'][:260]}")
    lines.extend(["", "### ContextSummaries", ""])
    for summary in report["samples"]["context_summaries"]:
        lines.append(f"- `{summary['summary_type']}` `{summary['scope']}`: {summary['text'][:260]}")
    return "\n".join(line.rstrip() for line in lines) + "\n"


def render_html(markdown: str) -> str:
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Codex History OSS Context Benchmark</title>"
        "<style>body{font-family:Segoe UI,Arial,sans-serif;margin:32px;color:#172033}"
        "pre{white-space:pre-wrap;background:#f8fafc;border:1px solid #d8dee8;padding:16px}</style>"
        "</head><body><pre>"
        + html.escape(markdown)
        + "</pre></body></html>"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill Codex history and compare OSS-reader answers with/without MatrixArk context.")
    parser.add_argument("--log-root", default=str(Path.home() / ".codex" / "sessions"))
    parser.add_argument("--out-dir", default="docs/debug/codex_history_oss_context_benchmark_20260724")
    parser.add_argument("--max-sessions", type=int, default=10)
    parser.add_argument("--max-messages-per-session", type=int, default=80)
    parser.add_argument("--segment-size", type=int, default=12)
    parser.add_argument("--max-entities", type=int, default=16)
    parser.add_argument("--token-budget", type=int, default=1536)
    parser.add_argument("--reader-provider", default="oss-openai-compatible")
    parser.add_argument("--reader-base-url", default="")
    parser.add_argument("--reader-model", default="")
    parser.add_argument("--reader-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--reader-max-tokens", type=int, default=128)
    args = parser.parse_args()

    events = iter_recent_codex_events(Path(args.log_root), args.max_sessions, args.max_messages_per_session)
    segments = build_segments(events, args.segment_size)
    entities = build_entities(events, args.max_entities)
    summaries = build_summaries(events, segments, entities)

    provider_reports = []
    ready_providers = []
    for provider in provider_candidates(args):
        probe = openai_get_models(provider, args.reader_timeout_seconds)
        provider_report = {**provider, **probe}
        provider_reports.append(provider_report)
        if probe["ready"]:
            ready_providers.append(provider)

    query_reports = []
    for query in DEFAULT_QUERIES:
        pack = retrieve_pack(query, events, segments, entities, summaries, args.token_budget)
        context = "\n\n".join(f"[{ref['ref_type']}:{ref['ref_id']}] {ref['text']}" for ref in pack["selected_refs"])
        answers = []
        for provider in ready_providers:
            answers.append(
                {
                    "provider": provider["name"],
                    "with_context": call_openai_chat(provider, query, context, args.reader_timeout_seconds, args.reader_max_tokens),
                    "without_context": call_openai_chat(provider, query, "", args.reader_timeout_seconds, args.reader_max_tokens),
                }
            )
        if not answers:
            answers.append(
                {
                    "provider": "none_ready",
                    "with_context": {"ok": False, "answer": "", "error": "No Qwen/Ollama/vLLM OpenAI-compatible model endpoint was ready."},
                    "without_context": {"ok": False, "answer": "", "error": "No Qwen/Ollama/vLLM OpenAI-compatible model endpoint was ready."},
                }
            )
        query_reports.append({"query": query, "context_pack": pack, "answers": answers})

    report = {
        "schema": "matrixark_codex_history_oss_context_benchmark_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "token_budget": args.token_budget,
        "ingestion": {
            "log_root": str(Path(args.log_root)),
            "sessions_seen": len({event.session_id for event in events}),
            "context_event_count": len(events),
            "context_segment_count": len(segments),
            "context_entity_count": len(entities),
            "context_summary_count": len(summaries),
            "mode": "historical_codex_jsonl_backfill_to_context_models",
        },
        "providers": provider_reports,
        "queries": query_reports,
        "samples": {
            "context_events": [asdict(event) for event in events[-8:]],
            "context_segments": [asdict(segment) for segment in segments[-4:]],
            "context_entities": [asdict(entity) for entity in entities[:8]],
            "context_summaries": [asdict(summary) for summary in summaries[-6:]],
        },
    }

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "codex_history_oss_context_benchmark.json"
    md_path = out_dir / "codex_history_oss_context_benchmark.md"
    html_path = out_dir / "codex_history_oss_context_benchmark.html"
    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    markdown = render_markdown(report)
    md_path.write_text(markdown, encoding="utf-8")
    html_path.write_text(render_html(markdown), encoding="utf-8")
    print(json.dumps({"json": str(json_path), "markdown": str(md_path), "html": str(html_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
