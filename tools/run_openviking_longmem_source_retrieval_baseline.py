#!/usr/bin/env python3
"""Run a LongMemEval direct-source retrieval diagnostic baseline.

This is a fallback baseline for OpenViking/VikingMem comparisons when local
OpenViking memory extraction returns no recallable memories. It ranks raw
LongMemEval haystack sessions, calls the same OpenAI-compatible OSS reader used
by MatrixArk diagnostics, and writes an explicit non-paper-comparable report.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import time
import urllib.request
from pathlib import Path
from typing import Any

WORD_RE = re.compile(r"[A-Za-z0-9]+")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/root/matrixark_benchmarks/data/longmemeval_s_tiny_2.json")
    parser.add_argument("--reader-base-url", default="http://127.0.0.1:18087/v1")
    parser.add_argument("--reader-model", default="Qwen/Qwen2.5-0.5B-Instruct")
    parser.add_argument("--reader-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--reader-max-tokens", type=int, default=96)
    parser.add_argument("--top-k", type=int, default=16)
    parser.add_argument("--max-context-chars", type=int, default=12000)
    parser.add_argument("--report", default="/tmp/openviking_direct_source_longmem_tiny_20260726.json")
    parser.add_argument("--matrixark-report", default="/tmp/matrixark_qwen_longmem_tiny_pythononly_20260726.json")
    args = parser.parse_args()

    started = time.time()
    report: dict[str, Any] = {
        "benchmark_family": "longmemeval_s",
        "baseline": "openviking_style_direct_source_retrieval",
        "claim_status": "diagnostic_not_paper_comparable",
        "reader_base_url": args.reader_base_url,
        "reader_model": args.reader_model,
        "input": args.input,
        "top_k": args.top_k,
        "max_context_chars": args.max_context_chars,
        "blockers": [],
        "warnings": [
            "OpenViking LongMemEval import completed only after setting a user API key, but official eval retrieved zero memories locally.",
            "This diagnostic ranks raw LongMemEval source sessions directly; it is a fallback retrieval baseline, not OpenViking memory recall evidence.",
            "Token counts are regex token estimates because local OSS endpoints do not return reliable usage counters.",
        ],
    }

    input_path = Path(args.input)
    if not input_path.exists():
        report["blockers"].append(f"missing_input:{args.input}")
        return finish(report, args.report, started, 2)
    if not probe_reader(args.reader_base_url):
        report["blockers"].append("reader_endpoint_unreachable")
        return finish(report, args.report, started, 2)

    data = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        report["blockers"].append("invalid_longmemeval_input")
        return finish(report, args.report, started, 2)

    per_query: list[dict[str, Any]] = []
    retrieval_hits = 0
    reader_hits = 0
    total_retrieved_tokens = 0
    total_source_tokens = 0
    retrieval_latencies: list[float] = []
    reader_latencies: list[float] = []

    for query_index, item in enumerate(data):
        question = str(item.get("question") or "")
        expected_answer = str(item.get("answer") or "")
        expected_session_ids = {str(x) for x in item.get("answer_session_ids") or []}
        sessions = build_sessions(item)
        source_tokens = sum(token_count(session["text"]) for session in sessions)
        total_source_tokens += source_tokens

        retrieval_started = time.perf_counter()
        ranked = rank_sessions(question, sessions)
        selected = trim_context(ranked[: max(1, args.top_k)], args.max_context_chars)
        retrieval_ms = (time.perf_counter() - retrieval_started) * 1000.0
        retrieval_latencies.append(retrieval_ms)
        selected_ids = {session["session_id"] for session in selected}
        hit = bool(expected_session_ids & selected_ids)
        retrieval_hits += int(hit)
        retrieved_tokens = sum(token_count(session["text"]) for session in selected)
        total_retrieved_tokens += retrieved_tokens

        context = "\n\n".join(
            f"[{session['session_id']}] {session['date']}\n{session['text']}" for session in selected
        )
        reader_started = time.perf_counter()
        answer = call_reader(
            args.reader_base_url,
            args.reader_model,
            question,
            context,
            timeout=args.reader_timeout_seconds,
            max_tokens=args.reader_max_tokens,
        )
        reader_ms = (time.perf_counter() - reader_started) * 1000.0
        reader_latencies.append(reader_ms)
        answer_ok = answer_matches(expected_answer, answer)
        reader_hits += int(answer_ok)

        per_query.append(
            {
                "query_id": str(item.get("question_id") or f"longmem_q{query_index}"),
                "question": question,
                "question_type": item.get("question_type"),
                "expected_answer": expected_answer,
                "expected_source_refs": sorted(expected_session_ids),
                "retrieved_source_refs": [session["session_id"] for session in selected],
                "retrieval_hit": hit,
                "reader_answer": answer,
                "reader_hit": answer_ok,
                "retrieved_tokens": retrieved_tokens,
                "source_tokens": source_tokens,
                "token_reduction_percent": reduction_percent(retrieved_tokens, source_tokens),
                "retrieval_ms": round(retrieval_ms, 3),
                "reader_ms": round(reader_ms, 3),
                "top_context_preview": context[:700],
            }
        )

    n = max(1, len(per_query))
    report.update(
        {
            "case_count": len(per_query),
            "benchmark_hit_at_k": retrieval_hits / n,
            "benchmark_reader_hit_rate": reader_hits / n,
            "benchmark_avg_retrieved_tokens_per_query": total_retrieved_tokens / n,
            "benchmark_avg_source_tokens_per_query": total_source_tokens / n,
            "benchmark_total_retrieved_tokens": total_retrieved_tokens,
            "benchmark_total_source_tokens": total_source_tokens,
            "benchmark_token_reduction_percent": reduction_percent(total_retrieved_tokens, total_source_tokens),
            "benchmark_retrieval_p50_ms": percentile(retrieval_latencies, 50),
            "benchmark_retrieval_p95_ms": percentile(retrieval_latencies, 95),
            "benchmark_reader_p50_ms": percentile(reader_latencies, 50),
            "benchmark_reader_p95_ms": percentile(reader_latencies, 95),
            "benchmark_per_query": per_query,
            "matrixark_reference": load_matrixark_reference(args.matrixark_report),
        }
    )
    return finish(report, args.report, started, 0)


def build_sessions(item: dict[str, Any]) -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    raw_sessions = item.get("haystack_sessions") or []
    ids = item.get("haystack_session_ids") or []
    dates = item.get("haystack_dates") or []
    for index, raw_session in enumerate(raw_sessions):
        session_id = str(ids[index]) if index < len(ids) else f"session_{index + 1}"
        date = str(dates[index]) if index < len(dates) else ""
        turns = []
        for turn_index, msg in enumerate(raw_session):
            if not isinstance(msg, dict):
                continue
            role = str(msg.get("role") or "message")
            content = str(msg.get("content") or "").strip()
            if content:
                turns.append(f"turn {turn_index} {role}: {content}")
        text = "\n".join(turns)
        if text:
            sessions.append({"session_id": session_id, "date": date, "text": text})
    return sessions


def rank_sessions(question: str, sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    q_terms = terms(question)
    ranked = []
    for session in sessions:
        text = session["text"].lower()
        text_terms = terms(text)
        overlap = len(q_terms & text_terms)
        phrase_bonus = sum(1 for term in q_terms if len(term) > 4 and term in text) * 0.25
        answerish_bonus = 0.5 if any(token in text for token in ("degree", "graduated", "business administration")) else 0.0
        ranked.append((overlap + phrase_bonus + answerish_bonus, session))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return [session for _, session in ranked]


def trim_context(sessions: list[dict[str, Any]], max_chars: int) -> list[dict[str, Any]]:
    if max_chars <= 0:
        return sessions
    selected = []
    used = 0
    for session in sessions:
        cost = len(session["text"]) + len(session["session_id"]) + len(session["date"]) + 8
        if selected and used + cost > max_chars:
            continue
        selected.append(session)
        used += cost
    return selected


def call_reader(base_url: str, model: str, question: str, context: str, *, timeout: float, max_tokens: int) -> str:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Answer the question from the context. Return only the direct answer."},
            {"role": "user", "content": f"Question: {question}\nContext:\n{context}\nAnswer:"},
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    req = urllib.request.Request(
        base_url.rstrip("/") + "/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        return str(data["choices"][0]["message"]["content"]).strip()
    except Exception as exc:
        return f"reader_error:{type(exc).__name__}:{exc}"


def probe_reader(base_url: str) -> bool:
    try:
        with urllib.request.urlopen(base_url.rstrip("/") + "/models", timeout=10) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False


def answer_matches(expected: str, actual: str) -> bool:
    expected_norm = normalize(expected)
    actual_norm = normalize(actual)
    if not expected_norm:
        return False
    if expected_norm in actual_norm:
        return True
    expected_terms = [term for term in expected_norm.split() if len(term) > 3]
    if expected_terms and all(term in actual_norm for term in expected_terms):
        return True
    return False


def terms(text: str) -> set[str]:
    stop = {"the", "and", "that", "this", "when", "what", "where", "with", "from", "did", "was", "were", "how", "who", "to", "a", "an", "of", "in", "on", "i", "me", "my"}
    return {m.group(0).lower() for m in WORD_RE.finditer(text) if m.group(0).lower() not in stop}


def normalize(text: str) -> str:
    return " ".join(WORD_RE.findall(str(text).lower()))


def token_count(text: str) -> int:
    return len(WORD_RE.findall(text))


def reduction_percent(retrieved: int, source: int) -> float:
    if source <= 0:
        return 0.0
    return round(100.0 * (1.0 - (retrieved / source)), 4)


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    rank = (len(ordered) - 1) * (pct / 100.0)
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return round(ordered[int(rank)], 3)
    value = ordered[low] * (high - rank) + ordered[high] * (rank - low)
    return round(value, 3)


def load_matrixark_reference(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {"available": False, "path": path}
    data = json.loads(p.read_text(encoding="utf-8"))
    return {
        "available": True,
        "path": path,
        "case_count": data.get("case_count"),
        "benchmark_hit_at_k": data.get("benchmark_hit_at_k"),
        "benchmark_reader_hit_rate": data.get("benchmark_reader_hit_rate", data.get("reader_hit_rate")),
        "benchmark_token_reduction_percent": data.get("benchmark_token_reduction_percent"),
        "benchmark_retrieval_p95_ms": data.get("benchmark_retrieval_p95_ms"),
        "benchmark_reader_p95_ms": data.get("benchmark_reader_p95_ms"),
        "python_only_diagnostic": data.get("python_only_diagnostic"),
    }


def finish(report: dict[str, Any], path: str, started: float, code: int) -> int:
    report["duration_seconds"] = round(time.time() - started, 3)
    report["ready"] = code == 0 and not report.get("blockers")
    Path(path).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(path)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
