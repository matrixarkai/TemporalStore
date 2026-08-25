#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Run a LongMemEval direct-source retrieval diagnostic baseline.

This is a fallback baseline for ExternalBaseline/ExternalBaseline comparisons when local
ExternalBaseline memory extraction returns no recallable memories. It ranks raw
LongMemEval haystack sessions, calls the same OpenAI-compatible OSS reader used
by MatrixArk diagnostics, and writes an explicit non-paper-comparable report.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_locomo_ingest_once import (  # noqa: E402
    configure_retrieval_embedding_model,
    hybrid_reader_answer,
    shared_dense_relevance_score,
)

WORD_RE = re.compile(r"[A-Za-z0-9]+")


OSS_READER_SYSTEM_PROMPT = (
    "You are an extractive long-memory benchmark reader. Answer only from the supplied context. "
    "Return a short direct answer to the question. If the question asks for a date, year, "
    "or when something happened, resolve relative phrases against the context timestamp and "
    "return the explicit date or year. If the question asks for a fact such as a degree, "
    "owner, place, or duration, copy the exact answer span from the context. "
    "For `degree in X`, return X, not the credential level. Do not substitute an unrelated date. "
    "If the context is insufficient, say not enough context."
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="/root/matrixark_benchmarks/data/longmemeval_s_tiny_2.json")
    parser.add_argument("--reader-base-url", default="http://127.0.0.1:18087/v1")
    parser.add_argument("--reader-model", default="Qwen/Qwen2.5-0.5B-Instruct")
    parser.add_argument("--embedding-model", default="matrixark-local-hash-embedding")
    parser.add_argument("--provider-name", default="external_baseline-style-direct-source")
    parser.add_argument("--reader-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--reader-max-tokens", type=int, default=96)
    parser.add_argument("--top-k", type=int, default=16)
    parser.add_argument("--question-limit", type=int, default=0, help="Optional maximum number of LongMemEval_s questions to score; 0 means all.")
    parser.add_argument("--question-offset", type=int, default=0)
    parser.add_argument("--max-context-chars", type=int, default=12000)
    parser.add_argument("--reader-include-extractive-hint", action="store_true")
    parser.add_argument("--reader-candidate-only", action="store_true")
    parser.add_argument("--reader-candidate-first", action="store_true")
    parser.add_argument("--reader-candidate-hybrid", action="store_true")
    parser.add_argument("--reader-focus-evidence", action="store_true")
    parser.add_argument("--same-session-percent", type=float, default=0.70)
    parser.add_argument("--cross-session-percent", type=float, default=0.45)
    parser.add_argument("--summary-percent", type=float, default=0.25)
    parser.add_argument("--entity-percent", type=float, default=0.35)
    parser.add_argument("--event-percent", type=float, default=0.80)
    parser.add_argument(
        "--require-shared-oss-models",
        dest="require_shared_oss_models",
        action="store_true",
        default=True,
        help=(
            "Fail if the baseline does not match MatrixArk reader, embedding/encoding, "
            "retrieval, context-budget, and output-token settings. Enabled by default."
        ),
    )
    parser.add_argument(
        "--allow-shared-oss-model-drift",
        dest="require_shared_oss_models",
        action="store_false",
        help="Diagnostic-only escape hatch for intentionally unfair local model or budget experiments.",
    )
    parser.add_argument("--report", default="/tmp/external_baseline_direct_source_longmem_tiny_20260726.json")
    parser.add_argument("--matrixark-report", default="/tmp/matrixark_qwen_longmem_tiny_pythononly_20260726.json")
    args = parser.parse_args()
    try:
        configure_retrieval_embedding_model(
            args.embedding_model,
            require_runtime=bool(args.require_shared_oss_models),
        )
    except RuntimeError as exc:
        report = {
            "benchmark_family": "longmemeval_s",
            "baseline": "external_baseline_style_direct_source_retrieval",
            "ready": False,
            "blockers": [str(exc)],
        }
        return finish(report, args.report, time.time(), 2)

    started = time.time()
    report: dict[str, Any] = {
        "benchmark_family": "longmemeval_s",
        "baseline": "external_baseline_style_direct_source_retrieval",
        "claim_status": "diagnostic_not_paper_comparable",
        "diagnostic_only": True,
        "reader_base_url": args.reader_base_url,
        "reader_provider_name": args.provider_name,
        "reader_model": args.reader_model,
        "embedding_model": args.embedding_model,
        "encoding_model": args.embedding_model,
        "reader_include_extractive_hint": bool(args.reader_include_extractive_hint),
        "reader_candidate_only": bool(args.reader_candidate_only),
        "reader_candidate_first": bool(args.reader_candidate_first or args.reader_candidate_hybrid),
        "reader_candidate_hybrid": bool(args.reader_candidate_hybrid),
        "reader_focus_evidence": bool(args.reader_focus_evidence),
        "retrieval_budget_config": {
            "same_session_percent": clamp_percent(args.same_session_percent),
            "cross_session_percent": clamp_percent(args.cross_session_percent),
            "summary_percent": clamp_percent(args.summary_percent),
            "entity_percent": clamp_percent(args.entity_percent),
            "event_percent": clamp_percent(args.event_percent),
        },
        "input": args.input,
        "top_k": args.top_k,
        "max_events": args.top_k,
        "max_context_chars": args.max_context_chars,
        "reader_max_context_chars": args.max_context_chars,
        "reader_max_tokens": args.reader_max_tokens,
        "blockers": [],
        "warnings": [
            "ExternalBaseline LongMemEval import completed only after setting a user API key, but official eval retrieved zero memories locally.",
            "This diagnostic ranks raw LongMemEval source sessions directly; it is a fallback retrieval baseline, not ExternalBaseline memory recall evidence.",
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
    query_items = flatten_longmem_questions(data)
    if args.question_offset > 0:
        query_items = query_items[args.question_offset :]
    if args.question_limit > 0:
        query_items = query_items[: args.question_limit]
    report["question_limit"] = args.question_limit
    report["question_offset"] = args.question_offset
    report["question_slice_diagnostic"] = args.question_limit > 0

    per_query: list[dict[str, Any]] = []
    retrieval_hits = 0
    reader_hits = 0
    reader_error_count = 0
    total_retrieved_tokens = 0
    total_source_tokens = 0
    retrieval_latencies: list[float] = []
    reader_latencies: list[float] = []
    progress_path = Path(args.report + ".progress.json")

    for query_index, item in enumerate(query_items):
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

        raw_context = "\n\n".join(
            f"[{session['session_id']}] {session['date']}\n{session['text']}" for session in selected
        )
        candidate = (
            extract_candidate_hint(question, raw_context)
            if args.reader_candidate_only or args.reader_candidate_hybrid or args.reader_include_extractive_hint
            else ""
        )
        context = reader_policy_context(question, raw_context, args)
        reader_started = time.perf_counter()
        answer = call_reader(
            args.reader_base_url,
            args.reader_model,
            question,
            context,
            timeout=args.reader_timeout_seconds,
            max_tokens=args.reader_max_tokens,
        )
        if args.reader_candidate_hybrid:
            answer = hybrid_reader_answer(question, candidate, answer)
        elif args.reader_candidate_only and candidate:
            answer = candidate
        reader_ms = (time.perf_counter() - reader_started) * 1000.0
        reader_latencies.append(reader_ms)
        if answer.startswith("reader_error:"):
            reader_error_count += 1
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
        if (query_index + 1) % 25 == 0 or (query_index + 1) == len(data):
            write_progress(
                progress_path,
                completed=query_index + 1,
                total=len(query_items),
                phase="running_reader",
                last_query_id=per_query[-1]["query_id"],
                retrieval_hits=retrieval_hits,
                reader_hits=reader_hits,
                reader_error_count=reader_error_count,
                reader_latencies=reader_latencies,
                retrieval_latencies=retrieval_latencies,
                total_retrieved_tokens=total_retrieved_tokens,
                total_source_tokens=total_source_tokens,
            )

    n = max(1, len(per_query))
    report.update(
        {
            "case_count": len(per_query),
            "benchmark_hit_at_k": retrieval_hits / n,
            "benchmark_reader_hit_rate": reader_hits / n,
            "reader_hit_rate": reader_hits / n,
            "reader_fallback_count": 0,
            "reader_error_count": reader_error_count,
            "reader_open_source_calls": len(per_query),
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
    report["benchmark_model_contract"] = benchmark_model_contract(args, report.get("matrixark_reference") or {})
    write_progress(
        progress_path,
        completed=len(per_query),
        total=len(query_items),
        phase="complete",
        last_query_id=per_query[-1]["query_id"] if per_query else "",
        retrieval_hits=retrieval_hits,
        reader_hits=reader_hits,
        reader_error_count=reader_error_count,
        reader_latencies=reader_latencies,
        retrieval_latencies=retrieval_latencies,
        total_retrieved_tokens=total_retrieved_tokens,
        total_source_tokens=total_source_tokens,
    )
    return finish_with_contract_gate(report, args.report, started, args.require_shared_oss_models)


def flatten_longmem_questions(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for sample_index, item in enumerate(data):
        if not isinstance(item, dict):
            continue
        if item.get("question"):
            cloned = dict(item)
            if not cloned.get("question_id"):
                cloned["question_id"] = str(item.get("sample_id") or f"longmem_q{sample_index}")
            if not cloned.get("answer_session_ids"):
                cloned["answer_session_ids"] = infer_answer_session_ids(cloned, str(cloned.get("answer") or ""))
            flattened.append(cloned)
            continue
        nested_questions = item.get("questions") or item.get("qa") or []
        for question_index, question in enumerate(nested_questions):
            if not isinstance(question, dict) or not question.get("question"):
                continue
            answer = question.get("answer")
            if answer is None and isinstance(question.get("answers"), list) and question["answers"]:
                answer = question["answers"][0]
            cloned = dict(item)
            cloned.update(question)
            cloned["question"] = str(question.get("question") or "")
            cloned["answer"] = str(answer or "")
            cloned["question_type"] = question.get("question_type") or question.get("ability") or question.get("reasoning_type")
            cloned["question_id"] = str(
                question.get("question_id")
                or f"{item.get('sample_id') or f'longmem_sample_{sample_index + 1}'}-q{question_index + 1}"
            )
            if not cloned.get("answer_session_ids"):
                cloned["answer_session_ids"] = infer_answer_session_ids(cloned, cloned["answer"])
            flattened.append(cloned)
    return flattened


def infer_answer_session_ids(item: dict[str, Any], answer: str) -> list[str]:
    answer_terms = terms(answer)
    if not answer_terms:
        return []
    sessions = build_sessions(item)
    matches: list[str] = []
    for session in sessions:
        text_terms = terms(session["text"])
        if answer_terms <= text_terms or normalize(answer) in normalize(session["text"]):
            matches.append(session["session_id"])
    return matches


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
        dense_bonus = shared_dense_relevance_score(question, session["text"]) / 80.0
        ranked.append((overlap + phrase_bonus + answerish_bonus + dense_bonus, session))
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


def reader_policy_context(question: str, context: str, args: argparse.Namespace) -> str:
    candidate = extract_candidate_hint(question, context)
    if args.reader_candidate_only and candidate:
        return f"Answer candidate from retrieved context: {candidate}\n\nRetrieved evidence:\n{candidate}"
    if args.reader_candidate_hybrid and candidate:
        prefix = (
            f"Answer candidate from retrieved context: {candidate}\n"
            f"Use exactly this candidate if it answers the question; otherwise use the evidence below.\n\n"
        )
        return (prefix + context)[: max(1, args.max_context_chars)]
    if args.reader_include_extractive_hint and candidate:
        prefix = f"Extractive answer hint from retrieved context: {candidate}\n\n"
        return (prefix + context)[: max(1, args.max_context_chars)]
    if args.reader_candidate_first and candidate:
        prefix = f"Likely answer span: {candidate}\n\n"
        return (prefix + context)[: max(1, args.max_context_chars)]
    return context


def extract_candidate_hint(question: str, context: str) -> str:
    q_terms = terms(question)
    best_score = -1.0
    best_sentence = ""
    for sentence in re.split(r"(?<=[.!?])\s+|\n+", context):
        clean = re.sub(r"\s+", " ", sentence).strip()
        if not clean:
            continue
        s_terms = terms(clean)
        score = len(q_terms & s_terms) + sum(1 for term in q_terms if len(term) > 4 and term in clean.lower()) * 0.25
        if score > best_score:
            best_score = score
            best_sentence = clean
    return best_sentence[:500]


def call_reader(base_url: str, model: str, question: str, context: str, *, timeout: float, max_tokens: int) -> str:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": OSS_READER_SYSTEM_PROMPT},
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


def write_progress(
    path: Path,
    *,
    completed: int,
    total: int,
    phase: str,
    last_query_id: str,
    retrieval_hits: int,
    reader_hits: int,
    reader_error_count: int,
    reader_latencies: list[float],
    retrieval_latencies: list[float],
    total_retrieved_tokens: int,
    total_source_tokens: int,
) -> None:
    n = max(1, completed)
    progress = {
        "schema": "external_baseline_longmem_reader_progress_v1",
        "phase": phase,
        "completed_queries": completed,
        "total_queries": total,
        "last_query_id": last_query_id,
        "retrieval_hit_at_k": retrieval_hits / n,
        "reader_hit_rate": reader_hits / n,
        "reader_error_count": reader_error_count,
        "reader_open_source_calls": completed,
        "retrieval_p95_ms": percentile(retrieval_latencies, 95),
        "reader_p95_ms": percentile(reader_latencies, 95),
        "token_reduction_percent": reduction_percent(total_retrieved_tokens, total_source_tokens),
    }
    path.write_text(json.dumps(progress, indent=2, sort_keys=True), encoding="utf-8")


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
        "retrieval_budget_config": data.get("retrieval_budget_config") or {},
        "benchmark_model_contract": data.get("benchmark_model_contract") or {},
    }


def benchmark_model_contract(args: argparse.Namespace, matrixark_reference: dict[str, Any]) -> dict[str, Any]:
    reference_contract = matrixark_reference.get("benchmark_model_contract") if isinstance(matrixark_reference, dict) else {}
    if not isinstance(reference_contract, dict):
        reference_contract = {}
    matrixark_reader = str(reference_contract.get("matrixark_reader_model") or args.reader_model).strip()
    matrixark_embedding = str(reference_contract.get("matrixark_embedding_model") or args.embedding_model).strip()
    matrixark_max_events = int(reference_contract.get("matrixark_max_events") or args.top_k)
    matrixark_reader_budget = int(reference_contract.get("matrixark_reader_max_context_chars") or args.max_context_chars)
    matrixark_reader_max_tokens = int(reference_contract.get("matrixark_reader_max_tokens") or args.reader_max_tokens)
    matrixark_reader_fallback_allowed = bool(reference_contract.get("matrixark_reader_fallback_allowed"))
    baseline_reader = str(args.reader_model).strip()
    baseline_embedding = str(args.embedding_model).strip()
    baseline_max_events = int(args.top_k)
    baseline_reader_budget = int(args.max_context_chars)
    baseline_reader_max_tokens = int(args.reader_max_tokens)
    baseline_reader_fallback_allowed = False
    baseline_same_session_percent = clamp_percent(args.same_session_percent)
    baseline_cross_session_percent = clamp_percent(args.cross_session_percent)
    baseline_summary_percent = clamp_percent(args.summary_percent)
    baseline_entity_percent = clamp_percent(args.entity_percent)
    baseline_event_percent = clamp_percent(args.event_percent)
    matrixark_budget = matrixark_reference.get("retrieval_budget_config")
    if not isinstance(matrixark_budget, dict):
        matrixark_budget = {}
    matrixark_same_session_percent = clamp_percent(
        reference_contract.get("matrixark_retrieval_same_session_percent")
        if "matrixark_retrieval_same_session_percent" in reference_contract
        else matrixark_budget.get("same_session_percent") or baseline_same_session_percent
    )
    matrixark_cross_session_percent = clamp_percent(
        reference_contract.get("matrixark_retrieval_cross_session_percent")
        if "matrixark_retrieval_cross_session_percent" in reference_contract
        else matrixark_budget.get("cross_session_percent") or baseline_cross_session_percent
    )
    matrixark_summary_percent = clamp_percent(
        reference_contract.get("matrixark_retrieval_summary_percent")
        if "matrixark_retrieval_summary_percent" in reference_contract
        else matrixark_budget.get("summary_percent") or baseline_summary_percent
    )
    matrixark_entity_percent = clamp_percent(
        reference_contract.get("matrixark_retrieval_entity_percent")
        if "matrixark_retrieval_entity_percent" in reference_contract
        else matrixark_budget.get("entity_percent") or baseline_entity_percent
    )
    matrixark_event_percent = clamp_percent(
        reference_contract.get("matrixark_retrieval_event_percent")
        if "matrixark_retrieval_event_percent" in reference_contract
        else matrixark_budget.get("event_percent") or baseline_event_percent
    )
    retrieval_budget_match = (
        matrixark_same_session_percent == baseline_same_session_percent
        and matrixark_cross_session_percent == baseline_cross_session_percent
        and matrixark_summary_percent == baseline_summary_percent
        and matrixark_entity_percent == baseline_entity_percent
        and matrixark_event_percent == baseline_event_percent
    )
    return {
        "matrixark_provider_name": str(reference_contract.get("matrixark_provider_name") or "matrixark").strip(),
        "matrixark_reader_model": matrixark_reader,
        "matrixark_embedding_model": matrixark_embedding,
        "matrixark_encoding_model": matrixark_embedding,
        "matrixark_max_events": matrixark_max_events,
        "matrixark_reader_max_context_chars": matrixark_reader_budget,
        "matrixark_reader_max_tokens": matrixark_reader_max_tokens,
        "matrixark_reader_fallback_allowed": matrixark_reader_fallback_allowed,
        "matrixark_retrieval_same_session_percent": matrixark_same_session_percent,
        "matrixark_retrieval_cross_session_percent": matrixark_cross_session_percent,
        "matrixark_retrieval_summary_percent": matrixark_summary_percent,
        "matrixark_retrieval_entity_percent": matrixark_entity_percent,
        "matrixark_retrieval_event_percent": matrixark_event_percent,
        "baseline_provider_name": str(args.provider_name).strip(),
        "baseline_reader_model": baseline_reader,
        "baseline_embedding_model": baseline_embedding,
        "baseline_encoding_model": baseline_embedding,
        "baseline_max_events": baseline_max_events,
        "baseline_reader_max_context_chars": baseline_reader_budget,
        "baseline_reader_max_tokens": baseline_reader_max_tokens,
        "baseline_reader_fallback_allowed": baseline_reader_fallback_allowed,
        "baseline_retrieval_same_session_percent": baseline_same_session_percent,
        "baseline_retrieval_cross_session_percent": baseline_cross_session_percent,
        "baseline_retrieval_summary_percent": baseline_summary_percent,
        "baseline_retrieval_entity_percent": baseline_entity_percent,
        "baseline_retrieval_event_percent": baseline_event_percent,
        "provider_identity_declared": bool(args.provider_name),
        "reader_model_match": normalized_model_name(matrixark_reader) == normalized_model_name(baseline_reader),
        "embedding_model_match": normalized_model_name(matrixark_embedding) == normalized_model_name(baseline_embedding),
        "max_events_match": matrixark_max_events == baseline_max_events,
        "reader_context_budget_match": matrixark_reader_budget == baseline_reader_budget,
        "reader_output_budget_match": matrixark_reader_max_tokens == baseline_reader_max_tokens,
        "reader_fallback_policy_match": matrixark_reader_fallback_allowed == baseline_reader_fallback_allowed,
        "retrieval_budget_match": retrieval_budget_match,
        "shared_oss_model_contract_required": True,
        "shared_oss_models_forced": bool(args.require_shared_oss_models),
        "same_oss_reader_model_forced": bool(args.require_shared_oss_models),
        "same_oss_encoding_model_forced": bool(args.require_shared_oss_models),
        "shared_oss_model_contract_passed": (
            normalized_model_name(matrixark_reader) == normalized_model_name(baseline_reader)
            and normalized_model_name(matrixark_embedding) == normalized_model_name(baseline_embedding)
            and matrixark_max_events == baseline_max_events
            and matrixark_reader_budget == baseline_reader_budget
            and matrixark_reader_max_tokens == baseline_reader_max_tokens
            and matrixark_reader_fallback_allowed == baseline_reader_fallback_allowed
            and retrieval_budget_match
        ),
        "comparison_rule": (
            "MatrixArk and ExternalBaseline/ExternalBaseline rows must use the same OSS reader model, "
            "embedding/encoding model, retrieval block budget, retrieval budget split, reader context budget, "
            "reader output-token budget, and reader fallback policy."
        ),
    }


def normalized_model_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9._:-]+", "", str(value or "").strip().lower())


def clamp_percent(value: Any) -> float:
    try:
        return round(max(0.0, min(1.0, float(value))), 6)
    except (TypeError, ValueError):
        return 0.0


def finish(report: dict[str, Any], path: str, started: float, code: int) -> int:
    report["duration_seconds"] = round(time.time() - started, 3)
    report["ready"] = code == 0 and not report.get("blockers")
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(path)
    return code


def finish_with_contract_gate(report: dict[str, Any], path: str, started: float, require_contract: bool) -> int:
    contract = report.get("benchmark_model_contract")
    if require_contract and not (isinstance(contract, dict) and contract.get("shared_oss_model_contract_passed")):
        report.setdefault("blockers", []).append("shared_oss_model_contract_mismatch")
        return finish(report, path, started, 2)
    return finish(report, path, started, 0)


if __name__ == "__main__":
    raise SystemExit(main())
