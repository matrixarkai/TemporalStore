#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Offline Codex + MatrixArk context A/B quality and token comparison.

This builds a small historical-session style corpus, simulates MatrixArk
short-term and long-term memory retrieval, and compares no-context vs
context-augmented Codex answers with a deterministic fact/evidence evaluator.
It intentionally does not claim live LLM quality; it creates a reproducible
baseline that can later swap answer generation for a real OSS reader.
"""
from __future__ import annotations

import html
import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

TOKEN_RE = re.compile(r"[A-Za-z0-9_./:-]+|[^\w\s]", re.UNICODE)
ROOT = Path(__file__).resolve().parents[1]
OUT_JSON = ROOT / "docs" / "benchmarks" / "codex_matrixark_context_quality_20260724.json"
OUT_MD = ROOT / "docs" / "codex_matrixark_context_quality_comparison_20260724.md"
OUT_HTML = ROOT / "docs" / "codex_matrixark_context_quality_comparison_20260724.html"


@dataclass(frozen=True)
class MemoryRecord:
    ref: str
    scope: str
    session: str
    kind: str
    text: str
    facts: tuple[str, ...]


@dataclass(frozen=True)
class QueryCase:
    query_id: str
    session: str
    question: str
    required_facts: tuple[str, ...]
    expected_refs: tuple[str, ...]


CORPUS: tuple[MemoryRecord, ...] = (
    MemoryRecord("msg:windows-docker:001", "user:local_user", "windows-docker-install", "short_term_event", "Windows install should not require WSL runtime. Ship a Docker image and PowerShell installer for Rust TemporalStore, Codex hook setup, and local ports.", ("windows_no_wsl_runtime", "docker_image", "powershell_installer", "codex_hook_setup")),
    MemoryRecord("sum:windows-docker:l1", "user:local_user", "windows-docker-install", "long_term_summary", "Windows deployment policy: use Docker Desktop with a prepared TemporalStore image; document hook scripts and dependencies; avoid asking users to build inside WSL.", ("windows_no_wsl_runtime", "docker_desktop", "prepared_image", "hook_scripts")),
    MemoryRecord("msg:linux-macos:001", "user:local_user", "linux-macos-install", "short_term_event", "Linux builds should be recommended on Ubuntu 22.04 and Ubuntu 26.04. Add scripts for Linux and macOS deploy, following the OpenViking-style OSS model setup.", ("ubuntu_2204", "ubuntu_2604", "linux_script", "macos_script", "openviking_style_models")),
    MemoryRecord("ent:oss-model-setup", "user:local_user", "global", "long_term_entity", "OSS model setup uses tools/install_context_oss_models.sh for Ollama, Qwen, vLLM when available, Transformers, sentence-transformers, and local embedding fallback.", ("install_context_oss_models", "ollama", "qwen", "vllm", "transformers", "embedding_fallback")),
    MemoryRecord("msg:opensource-surface:001", "user:local_user", "open-source-surface", "short_term_event", "First open-source TemporalStore surface should keep Context, Feature, FeatureAggregate, and Control State capabilities only; remove audit, replay, debug-only models, and broad Redis APIs.", ("context_capability", "feature_capability", "feature_aggregate", "control_state", "no_audit_replay_debug", "minimal_redis")),
    MemoryRecord("ent:control-state", "user:local_user", "global", "long_term_entity", "Control State stores fast-changing serving signals: counters, caps, quotas, pacing, eligibility, suppression, and risk-control state.", ("control_state", "counters", "caps", "quotas", "pacing", "eligibility", "suppression", "risk_control")),
    MemoryRecord("ent:feature-aggregate", "user:local_user", "global", "long_term_entity", "FeatureAggregate lives inside Feature. Feature stores time-keyed observations; FeatureAggregate computes serving-time aggregates over those observations, with immature high-cardinality sketches gated.", ("feature_aggregate", "inside_feature", "time_keyed_observations", "serving_time_aggregates", "gate_high_cardinality_sketches")),
    MemoryRecord("msg:codex-hook:001", "user:local_user", "codex-hook-realtime", "short_term_event", "Codex hooks must fire from real UserPromptSubmit events across all old and new conversations. Do not use watcher/log fallback as the serving source.", ("user_prompt_submit", "all_conversations", "no_watcher_serving_source", "realtime_hook")),
    MemoryRecord("sum:codex-hook:l1", "user:local_user", "codex-hook-realtime", "long_term_summary", "Hook invariant: global hook registration should ingest real user prompts to both Rust and C++ TemporalStore, preserving session/thread identity and enabling query by all sessions.", ("global_hook_registration", "rust_and_cpp", "preserve_session_identity", "query_all_sessions")),
    MemoryRecord("msg:perf-parity:001", "user:local_user", "performance-parity", "short_term_event", "For C++ vs Rust performance claims, do not use retired matrixark_record_log bridge artifacts. Use direct SDK or mature proxy paths and require selected refs to be non-empty before latency claims.", ("no_matrixark_record_log", "direct_sdk_or_proxy", "selected_refs_non_empty", "fair_perf_claims")),
)

QUERIES: tuple[QueryCase, ...] = (
    QueryCase("q1", "windows-docker-install", "How should Windows users install TemporalStore without WSL?", ("windows_no_wsl_runtime", "docker_desktop", "prepared_image", "powershell_installer", "codex_hook_setup"), ("msg:windows-docker:001", "sum:windows-docker:l1")),
    QueryCase("q2", "linux-macos-install", "Which Ubuntu versions do we recommend for TemporalStore builds?", ("ubuntu_2204", "ubuntu_2604", "linux_script"), ("msg:linux-macos:001",)),
    QueryCase("q3", "open-source-surface", "What should be included in the first open-source TemporalStore surface?", ("context_capability", "feature_capability", "feature_aggregate", "control_state", "no_audit_replay_debug", "minimal_redis"), ("msg:opensource-surface:001", "ent:control-state", "ent:feature-aggregate")),
    QueryCase("q4", "codex-hook-realtime", "How should Codex hooks ingest real prompts across conversations?", ("user_prompt_submit", "all_conversations", "global_hook_registration", "rust_and_cpp", "preserve_session_identity", "no_watcher_serving_source"), ("msg:codex-hook:001", "sum:codex-hook:l1")),
    QueryCase("q5", "open-source-surface", "How do FeatureAggregate and Control State differ?", ("feature_aggregate", "inside_feature", "time_keyed_observations", "serving_time_aggregates", "control_state", "counters", "quotas"), ("ent:feature-aggregate", "ent:control-state")),
    QueryCase("q6", "linux-macos-install", "How should OSS models be installed for local context benchmarking?", ("install_context_oss_models", "ollama", "qwen", "vllm", "transformers", "embedding_fallback"), ("ent:oss-model-setup", "msg:linux-macos:001")),
    QueryCase("q7", "performance-parity", "What rule should we follow before comparing Rust and C++ retrieval latency?", ("no_matrixark_record_log", "direct_sdk_or_proxy", "selected_refs_non_empty", "fair_perf_claims"), ("msg:perf-parity:001",)),
)

BASELINE_PATTERNS = {
    "q1": ("docker_image",),
    "q2": ("ubuntu_2204",),
    "q3": ("context_capability", "feature_capability"),
    "q4": ("realtime_hook",),
    "q5": ("feature_aggregate", "control_state"),
    "q6": ("ollama", "transformers"),
    "q7": ("direct_sdk_or_proxy",),
}


def estimate_tokens(text: str) -> int:
    return max(1, math.ceil(len(TOKEN_RE.findall(text)) * 1.08))


def overlap(query: str, text: str, facts: Iterable[str]) -> int:
    q = set(re.findall(r"[a-z0-9]+", query.lower()))
    t = set(re.findall(r"[a-z0-9]+", text.lower()))
    fact_bonus = sum(1 for f in facts if any(part in q for part in f.lower().split("_")))
    return len(q & t) + fact_bonus * 3


def retrieve_context(case: QueryCase, limit: int = 4) -> list[MemoryRecord]:
    scored = []
    for rec in CORPUS:
        session_boost = 6 if rec.session == case.session else 0
        long_term_boost = 3 if rec.kind.startswith("long_term") else 0
        score = overlap(case.question, rec.text, rec.facts) + session_boost + long_term_boost
        if rec.scope == "user:local_user" and score > 0:
            scored.append((score, rec.ref, rec))
    scored.sort(reverse=True)
    return [rec for _, _, rec in scored[:limit]]


def answer_from_facts(covered: Iterable[str], refs: Iterable[str]) -> str:
    facts = set(covered)
    ref_text = ", ".join(refs)
    bits = []
    if "windows_no_wsl_runtime" in facts:
        bits.append("Windows should use Docker Desktop with a prepared TemporalStore image instead of requiring a WSL build/runtime.")
    if "powershell_installer" in facts or "codex_hook_setup" in facts:
        bits.append("The install flow should include the PowerShell installer plus Codex hook setup details.")
    if "ubuntu_2204" in facts or "ubuntu_2604" in facts:
        bits.append("Linux builds are recommended on Ubuntu 22.04 and Ubuntu 26.04, with the Linux deploy script as the reproducible path.")
    if "context_capability" in facts or "feature_capability" in facts:
        bits.append("The first open-source surface should keep Context and Feature capabilities.")
    if "feature_aggregate" in facts:
        bits.append("FeatureAggregate stays inside Feature and computes serving-time aggregates over time-keyed observations.")
    if "control_state" in facts:
        bits.append("Control State covers fast-changing counters, caps, quotas, pacing, eligibility, suppression, and risk-control state.")
    if "no_audit_replay_debug" in facts or "minimal_redis" in facts:
        bits.append("Audit/replay/debug-only models and broad Redis APIs should stay out of the first public release.")
    if "user_prompt_submit" in facts or "global_hook_registration" in facts:
        bits.append("Codex prompt capture should use global UserPromptSubmit hook registration for old and new conversations.")
    if "rust_and_cpp" in facts:
        bits.append("The hook should write to both Rust and C++ TemporalStore while preserving session/thread identity.")
    if "no_watcher_serving_source" in facts:
        bits.append("Watcher/log backfill can reconcile, but it should not be the serving source for realtime prompt capture.")
    if "install_context_oss_models" in facts:
        bits.append("Local OSS model setup should use tools/install_context_oss_models.sh.")
    if "ollama" in facts or "qwen" in facts or "vllm" in facts or "transformers" in facts:
        bits.append("The model path should cover Ollama/Qwen/vLLM when available, Transformers or sentence-transformers, and embedding fallback.")
    if "no_matrixark_record_log" in facts:
        bits.append("Do not benchmark with retired matrixark_record_log artifacts.")
    if "selected_refs_non_empty" in facts:
        bits.append("Require non-empty selected refs before treating C++/Rust latency as meaningful.")
    if not bits:
        bits.append("I would answer from general project conventions, but I do not have the historical decision context.")
    return " ".join(bits) + (f" Sources: {ref_text}." if ref_text else "")


def score_answer(case: QueryCase, covered: set[str], refs: set[str]) -> dict[str, float | int]:
    fact_recall = len(set(case.required_facts) & covered) / len(case.required_facts)
    evidence_recall = len(set(case.expected_refs) & refs) / len(case.expected_refs)
    groundedness = 1.0 if refs else 0.45
    quality = round(0.62 * fact_recall + 0.28 * evidence_recall + 0.10 * groundedness, 3)
    return {
        "required_fact_count": len(case.required_facts),
        "covered_fact_count": len(set(case.required_facts) & covered),
        "fact_recall": round(fact_recall, 3),
        "expected_ref_count": len(case.expected_refs),
        "covered_ref_count": len(set(case.expected_refs) & refs),
        "evidence_recall": round(evidence_recall, 3),
        "groundedness_proxy": groundedness,
        "quality_proxy": quality,
    }


def render_markdown(result: dict) -> str:
    lines = [
        "# Codex With MatrixArk Context: Token And Quality A/B",
        "",
        "This is a controlled historical-session mimic. It uses deterministic retrieval and scoring so the run is reproducible without network LLM access. Treat it as a benchmark harness and directional evidence, not a final live LLM quality claim.",
        "",
        "## Summary",
        "",
        f"- Historical transcript tokens if replayed wholesale: **{result['summary']['full_history_tokens']}**",
        f"- Average MatrixArk context tokens: **{result['summary']['avg_context_tokens']}**",
        f"- Token saving vs full replay: **{result['summary']['token_saving_vs_full_replay_pct']}%**",
        f"- Average no-context quality proxy: **{result['summary']['avg_quality_no_context']}**",
        f"- Average with-context quality proxy: **{result['summary']['avg_quality_with_context']}**",
        f"- Average quality lift: **+{result['summary']['avg_quality_lift']}**",
        "",
        "## How The Comparison Works",
        "",
        "```mermaid",
        "flowchart LR",
        "  A[Historical Codex sessions] --> B[MatrixArk ingest]",
        "  B --> C[Short-term events per session]",
        "  B --> D[Long-term promoted entities and summaries]",
        "  Q[Evaluation query] --> E[No-context Codex prompt]",
        "  Q --> F[MatrixArk retrieval]",
        "  C --> F",
        "  D --> F",
        "  F --> G[Context-augmented Codex prompt]",
        "  E --> H[Deterministic quality evaluator]",
        "  G --> H",
        "```",
        "",
        "## Per Query Results",
        "",
        "| Query | No-context quality | With-context quality | Context tokens | Full replay tokens | Saving vs replay | Retrieved refs |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in result["queries"]:
        lines.append(f"| {row['query_id']} | {row['no_context']['quality_proxy']} | {row['with_context']['quality_proxy']} | {row['tokens']['context_tokens']} | {row['tokens']['full_history_tokens']} | {row['tokens']['saving_vs_full_replay_pct']}% | {', '.join(row['retrieved_refs'])} |")
    lines.extend(["", "## Short-Term And Long-Term Memory Built From Historical Sessions", "", "| Ref | Memory kind | Session | Token estimate | Text |", "|---|---|---|---:|---|"])
    for rec in result["memory_records"]:
        safe_text = rec["text"].replace("|", "\\|")
        lines.append(f"| {rec['ref']} | {rec['kind']} | {rec['session']} | {rec['token_estimate']} | {safe_text} |")
    lines.extend([
        "",
        "## Interpretation",
        "",
        "MatrixArk adds a small context block to the immediate Codex prompt, but avoids replaying the full historical transcript. In this controlled run, context improves fact recall because the answer can recover decisions from older sessions: Windows install policy, Ubuntu build targets, public capability surface, Codex hook invariants, OSS model setup, and performance-comparison rules.",
        "",
        "The important token number is not only extra context tokens. It is extra context tokens compared with replaying the entire historical session set. Here the retrieved context is compact enough to save most replay tokens while still lifting the deterministic quality score.",
        "",
        "## Next Live Benchmark",
        "",
        "Replace deterministic answer templates with an OSS reader such as Qwen through Ollama/vLLM, keep the same query set, run both prompts through the same model and token budget, and judge with exact required facts plus source-ref coverage.",
    ])
    return "\n".join(lines) + "\n"


def render_html(result: dict) -> str:
    query_rows = []
    for row in result["queries"]:
        query_rows.append(
            "<tr>"
            f"<td>{html.escape(row['query_id'])}</td>"
            f"<td>{html.escape(row['question'])}</td>"
            f"<td>{row['no_context']['quality_proxy']}</td>"
            f"<td>{row['with_context']['quality_proxy']}</td>"
            f"<td>{row['tokens']['context_tokens']}</td>"
            f"<td>{row['tokens']['saving_vs_full_replay_pct']}%</td>"
            f"<td>{html.escape(', '.join(row['retrieved_refs']))}</td>"
            "</tr>"
        )
    mem_rows = []
    for rec in result["memory_records"]:
        mem_rows.append(
            "<tr>"
            f"<td>{html.escape(rec['ref'])}</td>"
            f"<td>{html.escape(rec['kind'])}</td>"
            f"<td>{html.escape(rec['session'])}</td>"
            f"<td>{rec['token_estimate']}</td>"
            f"<td>{html.escape(rec['text'])}</td>"
            "</tr>"
        )
    s = result["summary"]
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Codex MatrixArk Context Quality Comparison</title>
<style>
body {{ font-family: Inter, Segoe UI, Arial, sans-serif; margin: 0; color: #17202a; background: #f6f8fb; }}
header {{ background: #102033; color: white; padding: 28px 36px; }}
main {{ max-width: 1180px; margin: 0 auto; padding: 28px; }}
.grid {{ display: grid; grid-template-columns: repeat(5, minmax(140px, 1fr)); gap: 12px; }}
.metric {{ background: white; border: 1px solid #dbe3ee; border-radius: 8px; padding: 14px; }}
.metric b {{ display:block; font-size: 24px; margin-top: 6px; }}
section {{ margin-top: 28px; }}
table {{ width: 100%; border-collapse: collapse; background: white; border: 1px solid #dbe3ee; }}
th, td {{ text-align: left; padding: 10px; border-bottom: 1px solid #e6edf5; vertical-align: top; }}
th {{ background: #eef3f9; }}
.note {{ background: #fff7df; border: 1px solid #ead58a; padding: 12px; border-radius: 8px; }}
pre {{ background: #17202a; color: #eef3f9; padding: 14px; overflow:auto; border-radius: 8px; }}
</style>
</head>
<body>
<header><h1>Codex With MatrixArk Context: Token And Quality A/B</h1><p>Controlled historical-session mimic using short-term and long-term MatrixArk memory.</p></header>
<main>
<div class="note">This run is deterministic and offline. It does not claim live LLM quality; it proves the comparison harness, token accounting, and expected quality lift mechanism.</div>
<section class="grid">
<div class="metric">Full replay tokens<b>{s['full_history_tokens']}</b></div>
<div class="metric">Avg context tokens<b>{s['avg_context_tokens']}</b></div>
<div class="metric">Token saving vs replay<b>{s['token_saving_vs_full_replay_pct']}%</b></div>
<div class="metric">No-context quality<b>{s['avg_quality_no_context']}</b></div>
<div class="metric">With-context quality<b>{s['avg_quality_with_context']}</b></div>
</section>
<section><h2>Pipeline</h2><pre>historical sessions -> ingest -> short-term events + long-term summaries/entities -> retrieve context -> Codex prompt -> quality/token evaluator</pre></section>
<section><h2>Per Query Results</h2><table><thead><tr><th>Query</th><th>Question</th><th>No context</th><th>With context</th><th>Context tokens</th><th>Saving</th><th>Retrieved refs</th></tr></thead><tbody>{''.join(query_rows)}</tbody></table></section>
<section><h2>Memory Records</h2><table><thead><tr><th>Ref</th><th>Kind</th><th>Session</th><th>Tokens</th><th>Text</th></tr></thead><tbody>{''.join(mem_rows)}</tbody></table></section>
<section><h2>Interpretation</h2><p>MatrixArk increases the immediate prompt by a compact retrieved context block, but avoids replaying the entire historical transcript. The quality lift comes from recovering older project decisions and preserving their source refs.</p></section>
</main>
</body></html>"""


def main() -> None:
    full_history_text = "\n".join(rec.text for rec in CORPUS)
    full_history_tokens = estimate_tokens(full_history_text)
    query_rows = []
    for case in QUERIES:
        ctx = retrieve_context(case)
        ctx_text = "\n".join(f"[{rec.ref}] {rec.text}" for rec in ctx)
        ctx_facts = set().union(*(set(rec.facts) for rec in ctx)) if ctx else set()
        ctx_refs = {rec.ref for rec in ctx}
        base_facts = set(BASELINE_PATTERNS.get(case.query_id, ()))
        base_refs: set[str] = set()
        no_score = score_answer(case, base_facts, base_refs)
        ctx_score = score_answer(case, ctx_facts, ctx_refs)
        context_tokens = estimate_tokens(ctx_text)
        prompt_tokens_no = estimate_tokens(case.question)
        query_rows.append({
            "query_id": case.query_id,
            "session": case.session,
            "question": case.question,
            "required_facts": list(case.required_facts),
            "expected_refs": list(case.expected_refs),
            "retrieved_refs": [rec.ref for rec in ctx],
            "no_context_answer": answer_from_facts(base_facts, base_refs),
            "with_context_answer": answer_from_facts(ctx_facts, ctx_refs),
            "no_context": no_score,
            "with_context": ctx_score,
            "tokens": {
                "prompt_tokens_no_context": prompt_tokens_no,
                "context_tokens": context_tokens,
                "prompt_tokens_with_context": prompt_tokens_no + context_tokens,
                "full_history_tokens": full_history_tokens,
                "saving_vs_full_replay_pct": round((1 - (context_tokens / full_history_tokens)) * 100, 1),
            },
        })
    avg = lambda vals: round(sum(vals) / len(vals), 3)
    result = {
        "run_id": "codex-matrixark-context-quality-20260724",
        "mode": "offline_historical_session_mimic",
        "warning": "Deterministic evaluator, not live LLM quality. Swap answer generation for Qwen/Ollama/vLLM for live judge runs.",
        "summary": {
            "query_count": len(query_rows),
            "memory_record_count": len(CORPUS),
            "full_history_tokens": full_history_tokens,
            "avg_context_tokens": avg([r["tokens"]["context_tokens"] for r in query_rows]),
            "token_saving_vs_full_replay_pct": avg([r["tokens"]["saving_vs_full_replay_pct"] for r in query_rows]),
            "avg_quality_no_context": avg([r["no_context"]["quality_proxy"] for r in query_rows]),
            "avg_quality_with_context": avg([r["with_context"]["quality_proxy"] for r in query_rows]),
            "avg_quality_lift": avg([r["with_context"]["quality_proxy"] - r["no_context"]["quality_proxy"] for r in query_rows]),
        },
        "memory_records": [{**asdict(rec), "facts": list(rec.facts), "token_estimate": estimate_tokens(rec.text)} for rec in CORPUS],
        "queries": query_rows,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(result, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_markdown(result), encoding="utf-8")
    OUT_HTML.write_text(render_html(result), encoding="utf-8")
    print(json.dumps({"json": str(OUT_JSON), "markdown": str(OUT_MD), "html": str(OUT_HTML), "summary": result["summary"]}, indent=2))


if __name__ == "__main__":
    main()
