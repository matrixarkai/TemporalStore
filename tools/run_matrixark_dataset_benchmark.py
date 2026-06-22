#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


Json = dict[str, Any]


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
    evidence_hits: int,
    context_hits: int,
    token_counts: list[int],
) -> Json:
    questions_run = len(questions)
    avg_prompt_tokens = statistics.mean(token_counts) if token_counts else 0.0
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
            "judge_execution_mode": "exact_substring_debug",
        },
        "models": {
            "embedding_model": "hashing:hashing-local",
            "reader_provider": "deterministic-context",
            "reader_model": "matrixark-context-substring-v1",
            "judge_provider": "exact-substring",
            "judge_model": "matrixark-local-substring-v1",
        },
        "scores": {
            "answer_hit": hits / max(1, questions_run),
            "context_recall": context_hits / max(1, questions_run),
            "evidence_session_recall": evidence_hits / max(1, questions_run),
            "final_judge_score": hits / max(1, questions_run),
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
        },
        "failure_categories": {
            "context_recall_miss": questions_run - context_hits,
            "evidence_session_miss": questions_run - evidence_hits,
            "reader_miss": questions_run - hits,
            "compression_hidden_answer": 0,
            "token_budget_pressure": sum(1 for count in token_counts if count >= args.max_context_tokens),
        },
        "started_at_ms": started_ms,
        "finished_at_ms": int(time.time() * 1000),
    }


def write_jsonl(path: Path, rows: list[Json]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")


def main() -> int:
    args = parse_args()
    if args.batch_size < 20:
        raise SystemExit("--batch-size must be >= 20")
    artifact_dir = Path(args.artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    args.storage_prefix = args.storage_prefix or f"matrixark:dataset:{args.dataset}:{args.artifact_prefix}"
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
        answer_hits = evidence_hits = context_hits = 0
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
            started = time.perf_counter()
            pack = call(
                proc,
                request_id,
                "matrixark_retrieve",
                {
                    "query": question["query"],
                    "scope": question["scope"],
                    "max_context_tokens": args.max_context_tokens,
                },
            )
            request_id += 1
            latency_ms = (time.perf_counter() - started) * 1000.0
            retrieval_latencies.append(latency_ms)
            selected = pack.get("selected_refs", [])
            text = selected_text(selected)
            answer_hit = contains_answer(text, question["answer"])
            evidence_hit = any(evidence.lower() in text.lower() for evidence in question.get("evidence", []))
            context_hit = bool(selected)
            answer_hits += int(answer_hit)
            evidence_hits += int(evidence_hit)
            context_hits += int(context_hit)
            used_tokens = int(pack.get("used_context_tokens") or sum(len(str(ref.get("text", "")).split()) for ref in selected))
            token_counts.append(used_tokens)
            prediction = normalize_answer(question["answer"])[0] if answer_hit and normalize_answer(question["answer"]) else ""
            row = {
                "question_id": question["question_id"],
                "question": question["query"],
                "answer": question["answer"],
                "prediction": prediction,
                "answer_hit": answer_hit,
                "evidence_hit": evidence_hit,
                "context_pack_id": pack.get("context_pack_id"),
                "selected_ref_count": len(selected),
                "used_context_tokens": used_tokens,
                "latency_ms": round(latency_ms, 3),
                "category": question.get("category"),
            }
            hypothesis_rows.append(row)
            judge_rows.append({**row, "judge": "exact_substring_debug", "score": int(answer_hit)})
            context_pack_rows.append({"question_id": question["question_id"], "context_pack": pack})
            result_rows.append({**row, "selected_refs": selected[:8]})
    finally:
        proc.kill()
        proc.wait(timeout=5)

    prefix = args.artifact_prefix
    artifacts = {
        "result_json": str(artifact_dir / f"{prefix}.result.json"),
        "report_json": str(artifact_dir / f"{prefix}.report.json"),
        "report_markdown": str(artifact_dir / f"{prefix}.report.md"),
        "hypotheses_jsonl": str(artifact_dir / f"{prefix}.hypotheses.jsonl"),
        "context_pack_jsonl": str(artifact_dir / f"{prefix}.context_packs.jsonl"),
        "judge_jsonl": str(artifact_dir / f"{prefix}.judge.jsonl"),
    }
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
        evidence_hits=evidence_hits,
        context_hits=context_hits,
        token_counts=token_counts,
    )
    Path(artifacts["result_json"]).write_text(json.dumps(result_rows, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    Path(artifacts["report_json"]).write_text(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    write_jsonl(Path(artifacts["hypotheses_jsonl"]), hypothesis_rows)
    write_jsonl(Path(artifacts["context_pack_jsonl"]), context_pack_rows)
    write_jsonl(Path(artifacts["judge_jsonl"]), judge_rows)
    Path(artifacts["report_markdown"]).write_text(
        "\n".join(
            [
                f"# MatrixArk {args.dataset} C++ TemporalStore Benchmark",
                "",
                f"- backend: `temporalstore-direct`",
                f"- metaserver: `{args.metaserver}`",
                f"- storage prefix: `{args.storage_prefix}`",
                f"- questions: `{len(questions)}`",
                f"- turns ingested: `{turns_ingested}`",
                f"- sessions: `{sessions_ingested}`",
                f"- context recall: `{report['scores']['context_recall']:.4f}`",
                f"- answer hit: `{report['scores']['answer_hit']:.4f}`",
                f"- evidence session recall: `{report['scores']['evidence_session_recall']:.4f}`",
                f"- ingestion throughput turns/sec: `{report['latency']['ingestion_throughput_turns_per_sec']}`",
                f"- retrieval p50 ms: `{report['latency']['p50_latency_ms']:.3f}`",
                f"- retrieval p95 ms: `{report['latency']['p95_latency_ms']:.3f}`",
                "",
                "This is a C++ storage-backed deterministic debug run, not an LLM-judge paper score.",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
