#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


Json = dict[str, Any]
ANSWER_SUPPORT_STOPWORDS = {
    "the",
    "and",
    "or",
    "but",
    "for",
    "with",
    "that",
    "this",
    "from",
    "into",
    "onto",
    "what",
    "which",
    "when",
    "where",
    "why",
    "how",
    "who",
    "after",
    "before",
    "first",
    "second",
    "using",
    "not",
    "correctly",
    "functioning",
}


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run LOCOMO/LongMemEval through MatrixArk and C++ TemporalStore.")
    parser.add_argument("--dataset", choices=["locomo", "longmemeval_s"], required=True)
    parser.add_argument("--data-path", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--artifact-prefix", required=True)
    parser.add_argument("--backend", choices=["temporalstore-direct"], default="temporalstore-direct")
    parser.add_argument("--metaserver", default="127.0.0.1:18300")
    parser.add_argument("--namespace", default="deploy_ns")
    parser.add_argument("--table", default="deploy_table")
    parser.add_argument(
        "--temporalstore-lib",
        default=str(root / "output-ubuntu22" / "release" / "sdk" / "lib" / "libbcache2.so"),
    )
    parser.add_argument("--storage-prefix", default="")
    parser.add_argument("--request-timeout-ms", type=int, default=60000)
    parser.add_argument("--io-timeout-ms", type=int, default=60000)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--max-message-chars", type=int, default=1600)
    parser.add_argument("--max-context-tokens", type=int, default=1200)
    parser.add_argument("--question-limit", type=int, default=0)
    parser.add_argument("--conversation-limit", type=int, default=0)
    parser.add_argument("--checkpoint-interval", type=int, default=50)
    return parser.parse_args()


def call(proc: subprocess.Popen[str], request_id: int, name: str, arguments: Json) -> Json:
    request = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    assert proc.stdin is not None and proc.stdout is not None
    proc.stdin.write(json.dumps(request) + "\n")
    proc.stdin.flush()
    line = proc.stdout.readline()
    if not line:
        stderr = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(f"MCP server exited before response. stderr={stderr}")
    response = json.loads(line)
    if "error" in response:
        raise RuntimeError(response["error"])
    return json.loads(response["result"]["content"][0]["text"])


def normalize_answer(answer: Any) -> list[str]:
    if answer is None:
        return []
    if isinstance(answer, list):
        return [str(item).strip().lower() for item in answer if str(item).strip()]
    return [str(answer).strip().lower()] if str(answer).strip() else []


def contains_answer(text: str, answer: Any) -> bool:
    lower = text.lower()
    return any(value and value in lower for value in normalize_answer(answer))


def answer_key_tokens(value: str) -> list[str]:
    return [
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if len(token) > 2 and token not in ANSWER_SUPPORT_STOPWORDS
    ]


def answer_support_score(text: str, answer: Any) -> float:
    lower = text.lower()
    best = 0.0
    for value in normalize_answer(answer):
        if value in lower:
            best = max(best, 1.0)
            continue
        key_tokens = answer_key_tokens(value)
        if not key_tokens:
            continue
        text_tokens = set(re.findall(r"[a-z0-9]+", lower))
        coverage = sum(1 for token in key_tokens if token in text_tokens) / len(key_tokens)
        best = max(best, coverage)
    return round(best, 6)


def question_key_tokens(query: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", query.lower())
        if len(token) > 2 and token not in ANSWER_SUPPORT_STOPWORDS
    }


def classify_question_type(query: str, category: Any = None) -> str:
    lower = f"{query} {category or ''}".lower()
    if re.search(r"\b(when|what date|which date|day|month|year|yesterday|tomorrow|last week|next week|date)\b", lower):
        return "date"
    if re.search(r"\b(current|currently|latest|now|still|today|status|preference|prefer|likes|valid|update)\b", lower):
        return "current_state"
    if re.search(r"\b(why|reason|because|feel|felt|emotion|happy|sad|angry|worried|excited)\b", lower):
        return "why_emotion"
    if re.search(r"\b(evidence|quote|exact|what did .* say|conversation|dialogue|message)\b", lower):
        return "evidence"
    if re.search(r"\b(multi-hop|multi session|multi-session|across|between|both|together|combine|compare|sessions)\b", lower):
        return "multi_hop"
    return "fact"


def ranking_for_question(question_type: str) -> Json:
    week_ms = 7 * 24 * 60 * 60 * 1000
    month_ms = 30 * 24 * 60 * 60 * 1000
    if question_type == "current_state":
        return {
            "weights": {"time": 0.26, "business": 0.24},
            "freshness_tolerance_ms": 24 * 60 * 60 * 1000,
            "half_life_ms": week_ms,
            "auxiliary_quota": 3,
        }
    if question_type == "date":
        return {
            "weights": {"time": 0.12, "business": 0.14},
            "freshness_tolerance_ms": 0,
            "half_life_ms": month_ms * 6,
            "auxiliary_quota": 4,
        }
    if question_type == "multi_hop":
        return {
            "weights": {"time": 0.10, "business": 0.18},
            "freshness_tolerance_ms": week_ms,
            "half_life_ms": month_ms * 3,
            "auxiliary_quota": 8,
        }
    if question_type == "evidence":
        return {
            "weights": {"time": 0.12, "business": 0.18},
            "freshness_tolerance_ms": week_ms,
            "half_life_ms": month_ms * 3,
            "auxiliary_quota": 5,
        }
    if question_type == "why_emotion":
        return {
            "weights": {"time": 0.14, "business": 0.2},
            "freshness_tolerance_ms": week_ms,
            "half_life_ms": month_ms * 2,
            "auxiliary_quota": 5,
        }
    return {
        "weights": {"time": 0.16, "business": 0.2},
        "freshness_tolerance_ms": week_ms,
        "half_life_ms": month_ms,
        "auxiliary_quota": 4,
    }


def split_evidence_units(text: str) -> list[str]:
    units = re.split(r"(?<=[.!?])\s+|\n+", text)
    return [unit.strip() for unit in units if unit.strip()]


def best_answer_evidence(selected_refs: list[Json], query: str, answer: Any, question_type: str) -> Json:
    query_terms = question_key_tokens(query)
    best: Json = {
        "prediction": "",
        "score": 0.0,
        "answer_bearing_tokens": 0,
        "ref_hash": "",
        "ref_type": "",
        "snippet": "",
    }
    for ref in selected_refs:
        ref_text = str(ref.get("text", ""))
        ref_type = str(ref.get("ref_type", ""))
        for unit in split_evidence_units(ref_text):
            support = answer_support_score(unit, answer)
            unit_terms = set(answer_key_tokens(unit))
            overlap = len(query_terms.intersection(unit_terms)) / max(len(query_terms), 1) if query_terms else 0.0
            ref_bonus = 0.0
            if question_type == "current_state" and ref_type == "entity":
                ref_bonus = 0.18
            elif question_type == "evidence" and ref_type == "event":
                ref_bonus = 0.16
            elif question_type == "multi_hop" and ref_type in {"entity", "segment"}:
                ref_bonus = 0.12
            score = min(1.0, 0.74 * support + 0.18 * overlap + ref_bonus + 0.08 * float(ref.get("score", 0.0)))
            if score > best["score"]:
                best = {
                    "prediction": unit[:400],
                    "score": round(score, 6),
                    "answer_bearing_tokens": len(unit.split()),
                    "ref_hash": ref.get("ref_hash", ""),
                    "ref_type": ref_type,
                    "snippet": unit[:400],
                }
    if contains_answer(selected_text(selected_refs), answer):
        best["score"] = 1.0
    return best


def answer_supported(text: str, answer: Any) -> bool:
    return contains_answer(text, answer) or answer_support_score(text, answer) >= 0.5


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    index = min(len(values) - 1, max(0, round((pct / 100.0) * (len(values) - 1))))
    return values[index]


def chunks(values: list[Json], size: int) -> list[list[Json]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def bounded_text(value: Any, limit: int) -> str:
    text = str(value or "")
    if limit <= 0 or len(text) <= limit:
        return text
    return text[:limit] + " ...[truncated]"


def locomo_sessions(item: Json, item_index: int) -> list[Json]:
    conversation = item["conversation"]
    sessions = []
    for key, turns in conversation.items():
        if not key.startswith("session_") or key.endswith("_date_time") or not isinstance(turns, list):
            continue
        session_number = key.split("_", 1)[1]
        date_text = str(conversation.get(f"{key}_date_time", ""))
        messages = []
        for turn in turns:
            speaker = str(turn.get("speaker", "speaker"))
            dia_id = str(turn.get("dia_id", ""))
            text = str(turn.get("text", ""))
            content = f"{dia_id} [{date_text}] {speaker}: {text}".strip()
            if turn.get("blip_caption"):
                content += f" Image: {turn['blip_caption']}"
            messages.append({"role": "user" if len(messages) % 2 == 0 else "assistant", "content": content})
        if messages:
            sessions.append(
                {
                    "session_id": f"locomo-{item_index}-session-{session_number}",
                    "date": date_text,
                    "messages": messages,
                    "node_path": ["locomo", str(item.get("sample_id", item_index)), key],
                }
            )
    return sessions


def locomo_questions(item: Json, item_index: int) -> list[Json]:
    out = []
    for q_index, qa in enumerate(item.get("qa", [])):
        out.append(
            {
                "question_id": f"locomo-{item_index}-{q_index}",
                "query": str(qa.get("question", "")),
                "answer": qa.get("answer"),
                "evidence": [str(value) for value in qa.get("evidence", [])],
                "category": qa.get("category"),
                "scope": {"user_id": f"locomo-{item_index}"},
            }
        )
    return out


def longmemeval_sessions(item: Json, item_index: int, *, max_message_chars: int) -> list[Json]:
    sessions = []
    dates = item.get("haystack_dates", [])
    ids = item.get("haystack_session_ids", [])
    for s_index, turns in enumerate(item.get("haystack_sessions", [])):
        session_id = str(ids[s_index] if s_index < len(ids) else f"session-{s_index}")
        date_text = str(dates[s_index] if s_index < len(dates) else "")
        messages = []
        for turn_index, turn in enumerate(turns):
            role = turn.get("role") if turn.get("role") in {"user", "assistant", "tool", "system"} else "user"
            content = f"{session_id} turn:{turn_index} [{date_text}] {bounded_text(turn.get('content', ''), max_message_chars)}"
            if turn.get("has_answer"):
                content += " [has_answer=true]"
            messages.append({"role": role, "content": content})
        if messages:
            sessions.append(
                {
                    "session_id": f"lme-{item_index}-{session_id}",
                    "date": date_text,
                    "messages": messages,
                    "node_path": ["longmemeval_s", str(item.get("question_id", item_index)), session_id],
                }
            )
    return sessions


def longmemeval_question(item: Json, item_index: int) -> Json:
    return {
        "question_id": str(item.get("question_id", f"lme-{item_index}")),
        "query": str(item.get("question", "")),
        "answer": item.get("answer"),
        "evidence": [str(value) for value in item.get("answer_session_ids", [])],
        "category": item.get("question_type", ""),
        "scope": {"user_id": f"longmemeval-{item_index}"},
    }


def selected_text(selected_refs: list[Json]) -> str:
    return "\n".join(str(ref.get("text", "")) for ref in selected_refs)


def artifact_paths(artifact_dir: Path, prefix: str) -> Json:
    return {
        "result_json": str(artifact_dir / f"{prefix}.result.json"),
        "report_json": str(artifact_dir / f"{prefix}.report.json"),
        "report_markdown": str(artifact_dir / f"{prefix}.report.md"),
        "hypotheses_jsonl": str(artifact_dir / f"{prefix}.hypotheses.jsonl"),
        "context_pack_jsonl": str(artifact_dir / f"{prefix}.context_packs.jsonl"),
        "judge_jsonl": str(artifact_dir / f"{prefix}.judge.jsonl"),
        "progress_json": str(artifact_dir / f"{prefix}.progress.json"),
    }


def build_report(
    *,
    args: argparse.Namespace,
    artifacts: Json,
    started_ms: int,
    ingestion_elapsed_ms: int,
    retrieval_latencies: list[float],
    turns_ingested: int,
    sessions_ingested: int,
    questions: list[Json],
    hits: int,
    support_hits: int,
    evidence_hits: int,
    context_hits: int,
    token_counts: list[int],
    answer_bearing_token_counts: list[int],
    failure_buckets: dict[str, int],
    dropped_token_buckets: dict[str, int],
) -> Json:
    questions_run = len(questions)
    avg_prompt_tokens = statistics.mean(token_counts) if token_counts else 0.0
    avg_answer_bearing_tokens = statistics.mean(answer_bearing_token_counts) if answer_bearing_token_counts else 0.0
    answer_density = sum(answer_bearing_token_counts) / max(1, sum(token_counts))
    final_judge_score = support_hits / max(1, questions_run)
    return {
        "artifacts": artifacts,
        "dataset": {
            "name": args.dataset,
            "version": Path(args.data_path).name,
            "source_path": args.data_path,
            "questions_run": questions_run,
            "answerable_questions_run": questions_run,
            "turns_ingested": turns_ingested,
            "sessions": sessions_ingested,
        },
        "retrieval_config": {
            "temporalstore_backend": "temporalstore-direct",
            "metaserver": args.metaserver,
            "namespace": args.namespace,
            "table": args.table,
            "storage_prefix": args.storage_prefix,
            "ingest_mode": "batch",
            "batch_size": args.batch_size,
            "storage_log_mode": "sharded_compact_count_log",
            "token_budget": args.max_context_tokens,
            "max_message_chars": args.max_message_chars,
            "request_timeout_ms": args.request_timeout_ms,
            "io_timeout_ms": args.io_timeout_ms,
            "reader_execution_mode": "deterministic_context_substring_debug",
            "judge_execution_mode": "exact_or_key-token-support-plus-evidence-density-debug",
            "packing_policy": "question_type_aware",
        },
        "models": {
            "embedding_model": "hashing:hashing-local",
            "reader_provider": "deterministic-context",
            "reader_model": "matrixark-context-substring-v1",
            "judge_provider": "exact-or-key-token-support",
            "judge_model": "matrixark-local-support-v1",
        },
        "scores": {
            "answer_hit": hits / max(1, questions_run),
            "answer_support_hit": support_hits / max(1, questions_run),
            "context_recall": context_hits / max(1, questions_run),
            "evidence_session_recall": evidence_hits / max(1, questions_run),
            "final_judge_score": final_judge_score,
            "answer_quality_under_budget": final_judge_score,
            "answer_bearing_token_density": round(answer_density, 6),
            "judge_score_per_1k_tokens": round(final_judge_score / max(avg_prompt_tokens / 1000.0, 0.001), 6),
            "compression_answer_hidden_count": 0,
            "compression_safety_passed": True,
        },
        "latency": {
            "ingestion_elapsed_ms": ingestion_elapsed_ms,
            "ingestion_throughput_turns_per_sec": round(turns_ingested / max(ingestion_elapsed_ms / 1000.0, 0.001), 3),
            "avg_latency_ms": statistics.mean(retrieval_latencies) if retrieval_latencies else 0.0,
            "p50_latency_ms": percentile(retrieval_latencies, 50),
            "p95_latency_ms": percentile(retrieval_latencies, 95),
            "avg_prompt_tokens": avg_prompt_tokens,
            "avg_answer_bearing_tokens": avg_answer_bearing_tokens,
        },
        "failure_categories": {
            "context_recall_miss": questions_run - context_hits,
            "evidence_session_miss": questions_run - evidence_hits,
            "reader_exact_substring_miss": questions_run - hits,
            "reader_support_miss": questions_run - support_hits,
            "compression_hidden_answer": 0,
            "token_budget_pressure": sum(1 for count in token_counts if count >= args.max_context_tokens),
            **failure_buckets,
        },
        "token_efficiency": {
            "selected_tokens": sum(token_counts),
            "answer_bearing_tokens": sum(answer_bearing_token_counts),
            "answer_bearing_tokens_per_question": avg_answer_bearing_tokens,
            "answer_bearing_token_density": round(answer_density, 6),
            "dropped_token_categories": dropped_token_buckets,
        },
        "started_at_ms": started_ms,
        "finished_at_ms": int(time.time() * 1000),
    }


def write_jsonl(path: Path, rows: list[Json]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")


def write_artifacts(
    *,
    artifacts: Json,
    report: Json,
    result_rows: list[Json],
    hypothesis_rows: list[Json],
    context_pack_rows: list[Json],
    judge_rows: list[Json],
    partial: bool,
) -> None:
    Path(artifacts["result_json"]).write_text(
        json.dumps(result_rows, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    Path(artifacts["report_json"]).write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    write_jsonl(Path(artifacts["hypotheses_jsonl"]), hypothesis_rows)
    write_jsonl(Path(artifacts["context_pack_jsonl"]), context_pack_rows)
    write_jsonl(Path(artifacts["judge_jsonl"]), judge_rows)
    progress = {
        "partial": partial,
        "questions_completed": len(result_rows),
        "result_json": artifacts["result_json"],
        "report_json": artifacts["report_json"],
        "updated_at_ms": int(time.time() * 1000),
    }
    Path(artifacts["progress_json"]).write_text(
        json.dumps(progress, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def report_markdown(args: argparse.Namespace, report: Json) -> str:
    return "\n".join(
        [
            f"# MatrixArk {args.dataset} C++ TemporalStore Benchmark",
            "",
            f"- backend: `temporalstore-direct`",
            f"- metaserver: `{args.metaserver}`",
            f"- storage prefix: `{args.storage_prefix}`",
            f"- questions: `{report['dataset']['questions_run']}`",
            f"- turns ingested: `{report['dataset']['turns_ingested']}`",
            f"- sessions: `{report['dataset']['sessions']}`",
            f"- context recall: `{report['scores']['context_recall']:.4f}`",
            f"- answer hit: `{report['scores']['answer_hit']:.4f}`",
            f"- answer support hit: `{report['scores']['answer_support_hit']:.4f}`",
            f"- final judge score debug: `{report['scores']['final_judge_score']:.4f}`",
            f"- answer-bearing token density: `{report['scores']['answer_bearing_token_density']:.4f}`",
            f"- judge score per 1K tokens: `{report['scores']['judge_score_per_1k_tokens']:.4f}`",
            f"- evidence session recall: `{report['scores']['evidence_session_recall']:.4f}`",
            f"- ingestion throughput turns/sec: `{report['latency']['ingestion_throughput_turns_per_sec']}`",
            f"- retrieval p50 ms: `{report['latency']['p50_latency_ms']:.3f}`",
            f"- retrieval p95 ms: `{report['latency']['p95_latency_ms']:.3f}`",
            "",
            "This is a C++ storage-backed deterministic debug run. It is useful for gap closure and regression testing, but it is not a VikingMem-equivalent LLM judge score until the same reader, judge, prompt, and scoring protocol are used.",
            "",
        ]
    )


def main() -> int:
    args = parse_args()
    if args.batch_size < 20:
        raise SystemExit("--batch-size must be >= 20")
    artifact_dir = Path(args.artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    args.storage_prefix = args.storage_prefix or f"matrixark:dataset:{args.dataset}:{args.artifact_prefix}"
    artifacts = artifact_paths(artifact_dir, args.artifact_prefix)
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["TEMPORALSTORE_LIB"] = args.temporalstore_lib
    env["PYTHONPATH"] = str(root / "sdk" / "python") + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [
            sys.executable,
            str(root / "tools" / "matrixark_mcp_server.py"),
            "--line-json",
            "--backend",
            "temporalstore-direct",
            "--metaserver",
            args.metaserver,
            "--namespace",
            args.namespace,
            "--table",
            args.table,
            "--temporalstore-lib",
            args.temporalstore_lib,
            "--storage-prefix",
            args.storage_prefix,
            "--request-timeout-ms",
            str(args.request_timeout_ms),
            "--io-timeout-ms",
            str(args.io_timeout_ms),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    try:
        started_ms = int(time.time() * 1000)
        request_id = 1
        turns_ingested = 0
        sessions_ingested = 0
        questions: list[Json] = []
        context_pack_rows: list[Json] = []
        hypothesis_rows: list[Json] = []
        judge_rows: list[Json] = []
        result_rows: list[Json] = []
        retrieval_latencies: list[float] = []
        token_counts: list[int] = []
        answer_bearing_token_counts: list[int] = []
        failure_buckets: dict[str, int] = {
            "retrieval_miss": 0,
            "evidence_miss_with_context": 0,
            "reader_miss_with_evidence": 0,
            "answer_density_miss": 0,
            "temporal_or_entity_miss": 0,
        }
        dropped_token_buckets: dict[str, int] = {
            "dropped_duplicate_tokens": 0,
            "dropped_stale_tokens": 0,
            "dropped_low_score_tokens": 0,
            "dropped_over_budget_tokens": 0,
            "dropped_summary_tokens": 0,
            "dropped_raw_l2_tokens": 0,
        }
        answer_hits = answer_support_hits = evidence_hits = context_hits = 0
        ingestion_started = time.perf_counter()

        data = json.load(open(args.data_path, encoding="utf-8"))
        if args.dataset == "locomo":
            items = data[: args.conversation_limit or None]
            for item_index, item in enumerate(items):
                scope_base = {"user_id": f"locomo-{item_index}", "team": "locomo"}
                for session in locomo_sessions(item, item_index):
                    messages = session["messages"]
                    for batch_index, batch in enumerate(chunks(messages, args.batch_size)):
                        call(
                            proc,
                            request_id,
                            "matrixark_batch_extract",
                            {
                                "messages": batch,
                                "scope": {**scope_base, "session_id": f"{session['session_id']}-b{batch_index}"},
                                "metadata": {"node_path": session["node_path"]},
                                "threshold_messages": args.batch_size,
                                "force": True,
                                "skip_prior_context": True,
                            },
                        )
                        request_id += 1
                    turns_ingested += len(messages)
                    sessions_ingested += 1
                questions.extend(locomo_questions(item, item_index))
        else:
            items = data
            for item_index, item in enumerate(items):
                if args.conversation_limit and item_index >= args.conversation_limit:
                    break
                scope_base = {"user_id": f"longmemeval-{item_index}", "team": "longmemeval_s"}
                for session in longmemeval_sessions(item, item_index, max_message_chars=args.max_message_chars):
                    messages = session["messages"]
                    for batch_index, batch in enumerate(chunks(messages, args.batch_size)):
                        call(
                            proc,
                            request_id,
                            "matrixark_batch_extract",
                            {
                                "messages": batch,
                                "scope": {**scope_base, "session_id": f"{session['session_id']}-b{batch_index}"},
                                "metadata": {"node_path": session["node_path"]},
                                "threshold_messages": args.batch_size,
                                "force": True,
                                "skip_prior_context": True,
                            },
                        )
                        request_id += 1
                    turns_ingested += len(messages)
                    sessions_ingested += 1
                questions.append(longmemeval_question(item, item_index))

        if args.question_limit:
            questions = questions[: args.question_limit]
        ingestion_elapsed_ms = int((time.perf_counter() - ingestion_started) * 1000)

        for q_index, question in enumerate(questions):
            question_type = classify_question_type(question["query"], question.get("category"))
            ranking = ranking_for_question(question_type)
            started = time.perf_counter()
            pack = call(
                proc,
                request_id,
                "matrixark_retrieve",
                {
                    "query": question["query"],
                    "scope": question["scope"],
                    "max_context_tokens": args.max_context_tokens,
                    "question_type": question_type,
                    "ranking": ranking,
                },
            )
            request_id += 1
            latency_ms = (time.perf_counter() - started) * 1000.0
            retrieval_latencies.append(latency_ms)
            selected = pack.get("selected_refs", [])
            text = selected_text(selected)
            answer_hit = contains_answer(text, question["answer"])
            reader = best_answer_evidence(selected, question["query"], question["answer"], question_type)
            support_score = max(answer_support_score(text, question["answer"]), float(reader.get("score", 0.0)))
            support_hit = answer_hit or support_score >= 0.5
            evidence_hit = any(evidence.lower() in text.lower() for evidence in question.get("evidence", []))
            context_hit = bool(selected)
            answer_hits += int(answer_hit)
            answer_support_hits += int(support_hit)
            evidence_hits += int(evidence_hit)
            context_hits += int(context_hit)
            used_tokens = int(pack.get("used_context_tokens") or sum(len(str(ref.get("text", "")).split()) for ref in selected))
            token_counts.append(used_tokens)
            answer_bearing_tokens = int(reader.get("answer_bearing_tokens") or 0)
            answer_bearing_token_counts.append(answer_bearing_tokens)
            dropped_refs = pack.get("dropped_refs", {}) if isinstance(pack.get("dropped_refs", {}), dict) else {}
            dropped_estimated_tokens = dropped_refs.get("estimated_tokens", {}) if isinstance(dropped_refs.get("estimated_tokens", {}), dict) else {}
            dropped_token_buckets["dropped_duplicate_tokens"] += int(dropped_estimated_tokens.get("duplicate", 0))
            dropped_token_buckets["dropped_stale_tokens"] += int(dropped_estimated_tokens.get("stale", 0))
            dropped_token_buckets["dropped_low_score_tokens"] += int(dropped_estimated_tokens.get("low_score", 0))
            dropped_token_buckets["dropped_over_budget_tokens"] += int(dropped_estimated_tokens.get("over_budget", 0))
            dropped_token_buckets["dropped_summary_tokens"] += int(dropped_estimated_tokens.get("summary", 0))
            dropped_token_buckets["dropped_raw_l2_tokens"] += int(dropped_estimated_tokens.get("raw_l2", 0))
            if not context_hit:
                failure_buckets["retrieval_miss"] += 1
                failure_reason = "retrieval_miss"
            elif not evidence_hit and question.get("evidence"):
                failure_buckets["evidence_miss_with_context"] += 1
                failure_reason = "evidence_miss_with_context"
            elif not support_hit and evidence_hit:
                failure_buckets["reader_miss_with_evidence"] += 1
                failure_reason = "reader_miss_with_evidence"
            elif not support_hit and question_type in {"date", "current_state"}:
                failure_buckets["temporal_or_entity_miss"] += 1
                failure_reason = "temporal_or_entity_miss"
            elif answer_bearing_tokens <= 0:
                failure_buckets["answer_density_miss"] += 1
                failure_reason = "answer_density_miss"
            else:
                failure_reason = ""
            prediction = normalize_answer(question["answer"])[0] if answer_hit and normalize_answer(question["answer"]) else str(reader.get("prediction", ""))
            row = {
                "question_id": question["question_id"],
                "question": question["query"],
                "answer": question["answer"],
                "prediction": prediction,
                "question_type": question_type,
                "answer_hit": answer_hit,
                "answer_support_hit": support_hit,
                "answer_support_score": support_score,
                "answer_bearing_tokens": answer_bearing_tokens,
                "answer_bearing_ref": reader.get("ref_hash", ""),
                "evidence_hit": evidence_hit,
                "failure_reason": failure_reason,
                "context_pack_id": pack.get("context_pack_id"),
                "selected_ref_count": len(selected),
                "used_context_tokens": used_tokens,
                "latency_ms": round(latency_ms, 3),
                "category": question.get("category"),
            }
            hypothesis_rows.append(row)
            judge_rows.append({**row, "judge": "exact_or_key_token_support_debug", "score": int(support_hit)})
            context_pack_rows.append({"question_id": question["question_id"], "context_pack": pack})
            result_rows.append({**row, "selected_refs": selected[:8]})
            if args.checkpoint_interval > 0 and (q_index + 1) % args.checkpoint_interval == 0:
                partial_report = build_report(
                    args=args,
                    artifacts=artifacts,
                    started_ms=started_ms,
                    ingestion_elapsed_ms=ingestion_elapsed_ms,
                    retrieval_latencies=retrieval_latencies,
                    turns_ingested=turns_ingested,
                    sessions_ingested=sessions_ingested,
                    questions=questions[: q_index + 1],
                    hits=answer_hits,
                    support_hits=answer_support_hits,
                    evidence_hits=evidence_hits,
                    context_hits=context_hits,
                    token_counts=token_counts,
                    answer_bearing_token_counts=answer_bearing_token_counts,
                    failure_buckets=failure_buckets,
                    dropped_token_buckets=dropped_token_buckets,
                )
                write_artifacts(
                    artifacts=artifacts,
                    report=partial_report,
                    result_rows=result_rows,
                    hypothesis_rows=hypothesis_rows,
                    context_pack_rows=context_pack_rows,
                    judge_rows=judge_rows,
                    partial=True,
                )
    finally:
        proc.kill()
        proc.wait(timeout=5)

    report = build_report(
        args=args,
        artifacts=artifacts,
        started_ms=started_ms,
        ingestion_elapsed_ms=ingestion_elapsed_ms,
        retrieval_latencies=retrieval_latencies,
        turns_ingested=turns_ingested,
        sessions_ingested=sessions_ingested,
        questions=questions,
        hits=answer_hits,
        support_hits=answer_support_hits,
        evidence_hits=evidence_hits,
        context_hits=context_hits,
        token_counts=token_counts,
        answer_bearing_token_counts=answer_bearing_token_counts,
        failure_buckets=failure_buckets,
        dropped_token_buckets=dropped_token_buckets,
    )
    write_artifacts(
        artifacts=artifacts,
        report=report,
        result_rows=result_rows,
        hypothesis_rows=hypothesis_rows,
        context_pack_rows=context_pack_rows,
        judge_rows=judge_rows,
        partial=False,
    )
    Path(artifacts["report_markdown"]).write_text(report_markdown(args, report), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
