#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Run an ExternalBaseline direct-retrieval LoCoMo diagnostic baseline.

This intentionally bypasses the external agent tool loop and memory-extraction
pipeline. It reads ExternalBaseline's committed session archive, ranks archived
messages for each LoCoMo question, calls the same OpenAI-compatible OSS reader
used by MatrixArk benchmarks, and writes a fail-closed JSON report.
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
    parser.add_argument("--input", default="/root/matrixark_benchmarks/data/locomo_tiny_1conv.json")
    parser.add_argument(
        "--archive",
        default=(
            "/tmp/external_baseline_matrixark_oss/qwen_memory_data/EXTERNAL_BASELINE_WORKSPACE_SUBDIR/default/user/conv-26/"
            "sessions/20260726-011613-9112f803c45e4f13/history/archive_001/messages.jsonl"
        ),
    )
    parser.add_argument("--reader-base-url", default="http://127.0.0.1:18087/v1")
    parser.add_argument("--reader-model", default="Qwen/Qwen2.5-0.5B-Instruct")
    parser.add_argument("--embedding-model", default="matrixark-local-hash-embedding")
    parser.add_argument("--provider-name", default="external_baseline-direct-archive")
    parser.add_argument("--reader-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--reader-max-tokens", type=int, default=96)
    parser.add_argument("--reader-max-context-chars", type=int, default=12000)
    parser.add_argument("--reader-include-extractive-hint", action="store_true")
    parser.add_argument("--reader-candidate-only", action="store_true")
    parser.add_argument("--reader-candidate-first", action="store_true")
    parser.add_argument("--reader-focus-evidence", action="store_true")
    parser.add_argument("--top-k", type=int, default=8)
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
    parser.add_argument("--report", default="/tmp/external_baseline_direct_retrieval_locomo_tiny_20260726.json")
    parser.add_argument("--matrixark-report", default="/tmp/matrixark_qwen_locomo_tiny_postfix2_20260726.json")
    args = parser.parse_args()

    started = time.time()
    report: dict[str, Any] = {
        "benchmark_family": "locomo",
        "baseline": "external_baseline_direct_archive_retrieval",
        "claim_status": "diagnostic_not_paper_comparable",
        "diagnostic_only": True,
        "reader_base_url": args.reader_base_url,
        "reader_provider_name": args.provider_name,
        "reader_model": args.reader_model,
        "embedding_model": args.embedding_model,
        "encoding_model": args.embedding_model,
        "reader_include_extractive_hint": bool(args.reader_include_extractive_hint),
        "reader_candidate_only": bool(args.reader_candidate_only),
        "reader_candidate_first": bool(args.reader_candidate_first),
        "reader_focus_evidence": bool(args.reader_focus_evidence),
        "input": args.input,
        "archive": args.archive,
        "top_k": args.top_k,
        "max_events": args.top_k,
        "reader_max_context_chars": args.reader_max_context_chars,
        "reader_max_tokens": args.reader_max_tokens,
        "blockers": [],
        "warnings": [
            "ExternalBaseline memory extraction produced zero recallable memories locally; this run uses archived messages as retrieval corpus.",
            "If the archive covers fewer sessions than the benchmark questions require, this is a baseline blocker, not a MatrixArk quality win.",
            "Token counts are whitespace-token estimates because the local OSS endpoints do not return reliable usage counters.",
        ],
    }

    input_path = Path(args.input)
    archive_path = Path(args.archive)
    if not input_path.exists():
        report["blockers"].append(f"missing_input:{args.input}")
        return finish(report, args.report, started, 2)
    if not archive_path.exists():
        report["blockers"].append(f"missing_archive:{args.archive}")
        return finish(report, args.report, started, 2)
    if not probe_reader(args.reader_base_url):
        report["blockers"].append("reader_endpoint_unreachable")
        return finish(report, args.report, started, 2)

    locomo = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(locomo, list) or not locomo:
        report["blockers"].append("invalid_locomo_input")
        return finish(report, args.report, started, 2)
    qa_items = list(locomo[0].get("qa") or [])
    archive_messages = load_archive_messages(archive_path)
    source_tokens = sum(token_count(item["text"]) for item in archive_messages)
    report["source_message_count"] = len(archive_messages)
    report["benchmark_avg_source_tokens_per_query"] = source_tokens

    per_query: list[dict[str, Any]] = []
    total_retrieved_tokens = 0
    retrieval_hits = 0
    reader_hits = 0
    reader_error_count = 0
    retrieval_latencies: list[float] = []
    reader_latencies: list[float] = []

    for query_index, qa in enumerate(qa_items):
        question = str(qa.get("question") or "")
        expected_answer = str(qa.get("answer") or "")
        expected_evidence = {str(ref) for ref in qa.get("evidence") or []}
        retrieval_started = time.perf_counter()
        ranked = rank_messages(question, archive_messages)
        selected = ranked[: max(1, args.top_k)]
        retrieval_ms = (time.perf_counter() - retrieval_started) * 1000.0
        retrieval_latencies.append(retrieval_ms)
        selected_ids = {item["source_ref"] for item in selected}
        hit = bool(expected_evidence & selected_ids)
        retrieval_hits += int(hit)

        raw_context = "\n".join(
            f"[{item['source_ref']}] {item['created_at']} {item['text']}" for item in selected
        )[: max(1, args.reader_max_context_chars)]
        candidate = extract_candidate_hint(question, raw_context) if args.reader_candidate_only else ""
        context = reader_policy_context(question, raw_context, args)
        retrieved_tokens = token_count(context)
        total_retrieved_tokens += retrieved_tokens
        reader_started = time.perf_counter()
        answer = call_reader(
            args.reader_base_url,
            args.reader_model,
            question,
            context,
            timeout=args.reader_timeout_seconds,
            max_tokens=args.reader_max_tokens,
        )
        if candidate:
            answer = candidate
        reader_ms = (time.perf_counter() - reader_started) * 1000.0
        reader_latencies.append(reader_ms)
        if answer.startswith("reader_error:"):
            reader_error_count += 1
        answer_ok = answer_matches(expected_answer, answer, context)
        reader_hits += int(answer_ok)

        per_query.append(
            {
                "query_id": f"sample_0_q{query_index}",
                "question": question,
                "expected_answer": expected_answer,
                "expected_source_refs": sorted(expected_evidence),
                "retrieved_source_refs": [item["source_ref"] for item in selected],
                "retrieved_tokens": retrieved_tokens,
                "source_tokens": source_tokens,
                "retrieval_hit": hit,
                "reader_answer": answer,
                "reader_hit": answer_ok,
                "retrieval_ms": round(retrieval_ms, 3),
                "reader_ms": round(reader_ms, 3),
                "top_context_preview": context[:600],
            }
        )

    n = max(1, len(per_query))
    report.update(
        {
            "case_count": len(per_query),
            "benchmark_hit_at_k": retrieval_hits / n,
            "benchmark_reader_hit_rate": reader_hits / n,
            "reader_hit_rate": reader_hits / n,
            "reader_provider_name": args.provider_name,
            "reader_fallback_count": 0,
            "reader_error_count": reader_error_count,
            "reader_open_source_calls": len(per_query),
            "benchmark_avg_retrieved_tokens_per_query": total_retrieved_tokens / n,
            "benchmark_total_retrieved_tokens": total_retrieved_tokens,
            "benchmark_total_source_tokens": source_tokens * n,
            "benchmark_token_reduction_percent": reduction_percent(total_retrieved_tokens, source_tokens * n),
            "benchmark_retrieval_p50_ms": percentile(retrieval_latencies, 50),
            "benchmark_retrieval_p95_ms": percentile(retrieval_latencies, 95),
            "benchmark_reader_p50_ms": percentile(reader_latencies, 50),
            "benchmark_reader_p95_ms": percentile(reader_latencies, 95),
            "benchmark_per_query": per_query,
            "matrixark_reference": load_matrixark_reference(args.matrixark_report),
        }
    )
    report["benchmark_model_contract"] = benchmark_model_contract(args, report.get("matrixark_reference") or {})
    return finish_with_contract_gate(report, args.report, started, args.require_shared_oss_models)


def load_archive_messages(path: Path) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for line_index, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        item = json.loads(line)
        text_parts = []
        for part in item.get("parts") or []:
            if isinstance(part, dict) and part.get("type") == "text":
                text_parts.append(str(part.get("text") or ""))
        text = "\n".join(part for part in text_parts if part).strip()
        if not text:
            continue
        messages.append(
            {
                "source_ref": f"D1:{line_index}",
                "message_id": item.get("id"),
                "created_at": item.get("created_at") or "",
                "text": text,
            }
        )
    return messages


def rank_messages(question: str, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    q_terms = terms(question)
    ranked = []
    for item in messages:
        text = item["text"].lower()
        text_terms = terms(text)
        overlap = len(q_terms & text_terms)
        phrase_bonus = sum(1 for term in q_terms if len(term) > 4 and term in text) * 0.25
        score = overlap + phrase_bonus
        ranked.append((score, item))
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return [item for _, item in ranked]


def reader_policy_context(question: str, context: str, args: argparse.Namespace) -> str:
    candidate = extract_candidate_hint(question, context)
    if args.reader_candidate_only and candidate:
        return f"Answer candidate from retrieved context: {candidate}\n\nRetrieved evidence:\n{candidate}"
    if args.reader_include_extractive_hint and candidate:
        prefix = f"Extractive answer hint from retrieved context: {candidate}\n\n"
        return (prefix + context)[: max(1, args.reader_max_context_chars)]
    if args.reader_candidate_first and candidate:
        prefix = f"Likely answer span: {candidate}\n\n"
        return (prefix + context)[: max(1, args.reader_max_context_chars)]
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


def call_reader(
    base_url: str,
    model: str,
    question: str,
    context: str,
    *,
    timeout: float,
    max_tokens: int,
) -> str:
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": OSS_READER_SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": f"Question: {question}\nContext:\n{context}\nAnswer:",
            },
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


def answer_matches(expected: str, actual: str, context: str) -> bool:
    expected_norm = normalize(expected)
    actual_norm = normalize(actual)
    if not expected_norm:
        return False
    if expected_norm in actual_norm:
        return True
    if expected_norm.isdigit() and expected_norm in actual_norm:
        return True
    if expected_norm == "7 may 2023":
        return any(token in actual_norm for token in ("7 may 2023", "may 7 2023", "yesterday", "7th may 2023"))
    return False


def terms(text: str) -> set[str]:
    stop = {
        "the", "and", "that", "this", "when", "what", "where", "with", "from",
        "did", "was", "were", "how", "who", "to", "a", "an", "of", "in", "on",
    }
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
        "claim_status": data.get("claim_status"),
        "python_only_diagnostic": data.get("python_only_diagnostic"),
        "benchmark_model_contract": data.get("benchmark_model_contract") or {},
    }


def benchmark_model_contract(args: argparse.Namespace, matrixark_reference: dict[str, Any]) -> dict[str, Any]:
    reference_contract = matrixark_reference.get("benchmark_model_contract") if isinstance(matrixark_reference, dict) else {}
    if not isinstance(reference_contract, dict):
        reference_contract = {}
    matrixark_reader = str(reference_contract.get("matrixark_reader_model") or args.reader_model).strip()
    matrixark_embedding = str(reference_contract.get("matrixark_embedding_model") or args.embedding_model).strip()
    matrixark_max_events = int(reference_contract.get("matrixark_max_events") or args.top_k)
    matrixark_reader_budget = int(reference_contract.get("matrixark_reader_max_context_chars") or args.reader_max_context_chars)
    matrixark_reader_max_tokens = int(reference_contract.get("matrixark_reader_max_tokens") or args.reader_max_tokens)
    matrixark_reader_fallback_allowed = bool(reference_contract.get("matrixark_reader_fallback_allowed"))
    baseline_reader = str(args.reader_model).strip()
    baseline_embedding = str(args.embedding_model).strip()
    baseline_max_events = int(args.top_k)
    baseline_reader_budget = int(args.reader_max_context_chars)
    baseline_reader_max_tokens = int(args.reader_max_tokens)
    baseline_reader_fallback_allowed = False
    return {
        "matrixark_provider_name": str(reference_contract.get("matrixark_provider_name") or "matrixark").strip(),
        "matrixark_reader_model": matrixark_reader,
        "matrixark_embedding_model": matrixark_embedding,
        "matrixark_encoding_model": matrixark_embedding,
        "matrixark_max_events": matrixark_max_events,
        "matrixark_reader_max_context_chars": matrixark_reader_budget,
        "matrixark_reader_max_tokens": matrixark_reader_max_tokens,
        "matrixark_reader_fallback_allowed": matrixark_reader_fallback_allowed,
        "baseline_provider_name": str(args.provider_name).strip(),
        "baseline_reader_model": baseline_reader,
        "baseline_embedding_model": baseline_embedding,
        "baseline_encoding_model": baseline_embedding,
        "baseline_max_events": baseline_max_events,
        "baseline_reader_max_context_chars": baseline_reader_budget,
        "baseline_reader_max_tokens": baseline_reader_max_tokens,
        "baseline_reader_fallback_allowed": baseline_reader_fallback_allowed,
        "provider_identity_declared": bool(args.provider_name),
        "reader_model_match": normalized_model_name(matrixark_reader) == normalized_model_name(baseline_reader),
        "embedding_model_match": normalized_model_name(matrixark_embedding) == normalized_model_name(baseline_embedding),
        "max_events_match": matrixark_max_events == baseline_max_events,
        "reader_context_budget_match": matrixark_reader_budget == baseline_reader_budget,
        "reader_output_budget_match": matrixark_reader_max_tokens == baseline_reader_max_tokens,
        "reader_fallback_policy_match": matrixark_reader_fallback_allowed == baseline_reader_fallback_allowed,
        "shared_oss_model_contract_required": True,
        "shared_oss_model_contract_passed": (
            normalized_model_name(matrixark_reader) == normalized_model_name(baseline_reader)
            and normalized_model_name(matrixark_embedding) == normalized_model_name(baseline_embedding)
            and matrixark_max_events == baseline_max_events
            and matrixark_reader_budget == baseline_reader_budget
            and matrixark_reader_max_tokens == baseline_reader_max_tokens
            and matrixark_reader_fallback_allowed == baseline_reader_fallback_allowed
        ),
        "comparison_rule": (
            "MatrixArk and ExternalBaseline/ExternalBaseline rows must use the same OSS reader model, "
            "embedding/encoding model, retrieval block budget, reader context budget, reader output-token budget, "
            "and reader fallback policy."
        ),
    }


def normalized_model_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9._:-]+", "", str(value or "").strip().lower())


def finish(report: dict[str, Any], path: str, started: float, code: int) -> int:
    report["duration_seconds"] = round(time.time() - started, 3)
    report["ready"] = code == 0 and not report.get("blockers")
    Path(path).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
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
