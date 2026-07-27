#!/usr/bin/env python3
"""Run fair OSS MatrixArk vs OpenViking/VikingMem-style benchmark pairs.

This wrapper exists to prevent accidental apples-to-oranges comparisons. It
pins one OSS reader, one embedding/encoding model, one retrieval budget, one
reader context budget, and one retrieval-budget split across MatrixArk and
OpenViking/VikingMem-style baselines, then validates and summarizes the output.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


DEFAULT_OUTPUT_ROOT = "/root/src/github-services/TemporalStore/benchmark-runs/fair-oss"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--reader-base-url", default="http://127.0.0.1:11434/v1")
    parser.add_argument("--reader-model", default="qwen2.5:1.5b")
    parser.add_argument("--embedding-model", default="matrixark-hash-embedding-32")
    parser.add_argument(
        "--oss-reader-model",
        default="",
        help="Shared OSS reader model forced for MatrixArk and OpenViking/VikingMem. Overrides --reader-model.",
    )
    parser.add_argument(
        "--oss-encoding-model",
        default="",
        help=(
            "Shared OSS encoding/embedding model forced for MatrixArk and OpenViking/VikingMem. "
            "Overrides --embedding-model."
        ),
    )
    parser.add_argument("--reader-timeout-seconds", type=float, default=180.0)
    parser.add_argument("--reader-max-tokens", type=int, default=96)
    parser.add_argument("--locomo-input", default="/root/matrixark_benchmarks/data/locomo10.json")
    parser.add_argument("--longmem-input", default="/root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json")
    parser.add_argument("--locomo-question-limit", type=int, default=300)
    parser.add_argument("--longmem-question-limit", type=int, default=0, help="0 means all LongMemEval_s records.")
    parser.add_argument("--locomo-max-events", type=int, default=192)
    parser.add_argument("--locomo-adaptive-base-max-events", type=int, default=128)
    parser.add_argument("--longmem-max-events", type=int, default=64)
    parser.add_argument("--locomo-reader-context-chars", type=int, default=12000)
    parser.add_argument("--longmem-reader-context-chars", type=int, default=4000)
    parser.add_argument("--same-session-percent", type=float, default=0.70)
    parser.add_argument("--cross-session-percent", type=float, default=0.45)
    parser.add_argument("--summary-percent", type=float, default=0.25)
    parser.add_argument("--entity-percent", type=float, default=0.35)
    parser.add_argument("--event-percent", type=float, default=0.80)
    parser.add_argument(
        "--skip-matrixark",
        action="store_true",
        help="Only regenerate baseline/validation/summary against existing MatrixArk reports.",
    )
    parser.add_argument(
        "--skip-baseline",
        action="store_true",
        help="Only regenerate MatrixArk/validation/summary against existing baseline reports.",
    )
    parser.add_argument(
        "--allow-python-only-diagnostic",
        action="store_true",
        help="Permit diagnostic MatrixArk runs that skip the Rust TemporalStore backend proof.",
    )
    parser.add_argument(
        "--diagnostic-smoke",
        action="store_true",
        help=(
            "Relax benchmark quality thresholds for a tiny orchestration smoke run. "
            "The shared OSS model/budget contract is still enforced."
        ),
    )
    args = parser.parse_args()
    apply_shared_oss_stack_aliases(args)

    repo = Path(__file__).resolve().parents[1]
    out = Path(args.output_root)
    out.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["MATRIXARK_READER_MAX_TOKENS"] = str(args.reader_max_tokens)
    write_shared_oss_stack_contract(out, args)

    locomo_matrixark = out / "matrixark_locomo_oss_report.json"
    locomo_matrixark_misses = out / "matrixark_locomo_oss_misses.jsonl"
    locomo_baseline = out / "openviking_locomo_oss_report.json"
    locomo_validation = out / "locomo_shared_oss_contract_validation.json"

    longmem_matrixark = out / "matrixark_longmemeval_s_oss_report.json"
    longmem_matrixark_misses = out / "matrixark_longmemeval_s_oss_misses.jsonl"
    longmem_baseline = out / "openviking_longmemeval_s_oss_report.json"
    longmem_validation = out / "longmemeval_s_shared_oss_contract_validation.json"

    if not args.skip_matrixark:
        if not run_or_record_blocker(
            "matrixark_locomo",
            locomo_matrixark_command(repo, args, locomo_matrixark, locomo_matrixark_misses),
            repo,
            env,
            out,
        ):
            return 1
        if not run_or_record_blocker(
            "matrixark_longmemeval_s",
            longmem_matrixark_command(repo, args, longmem_matrixark, longmem_matrixark_misses),
            repo,
            env,
            out,
        ):
            return 1
    require_files(locomo_matrixark, longmem_matrixark)

    if not args.skip_baseline:
        if not run_or_record_blocker(
            "openviking_locomo",
            locomo_baseline_command(repo, args, locomo_matrixark, locomo_baseline),
            repo,
            env,
            out,
        ):
            return 1
        if not run_or_record_blocker(
            "openviking_longmemeval_s",
            longmem_baseline_command(repo, args, longmem_matrixark, longmem_baseline),
            repo,
            env,
            out,
        ):
            return 1
    require_files(locomo_baseline, longmem_baseline)

    run(
        [
            sys.executable,
            str(repo / "tools" / "validate_oss_model_contract.py"),
            "--report",
            str(locomo_matrixark),
            "--label",
            "matrixark_locomo",
            "--report",
            str(locomo_baseline),
            "--label",
            "openviking_locomo",
            "--allow-diagnostic",
            "--output-json",
            str(locomo_validation),
        ],
        repo,
        env,
    )
    run(
        [
            sys.executable,
            str(repo / "tools" / "validate_oss_model_contract.py"),
            "--report",
            str(longmem_matrixark),
            "--label",
            "matrixark_longmem",
            "--report",
            str(longmem_baseline),
            "--label",
            "openviking_longmem",
            "--allow-diagnostic",
            "--output-json",
            str(longmem_validation),
        ],
        repo,
        env,
    )
    run(
        [
            sys.executable,
            str(repo / "tools" / "summarize_oss_benchmark_comparison.py"),
            "--comparison",
            "locomo_qwen_same_budget",
            "--matrixark-report",
            str(locomo_matrixark),
            "--baseline-report",
            str(locomo_baseline),
            "--contract-validation",
            str(locomo_validation),
            "--comparison",
            "longmemeval_s_qwen_same_budget",
            "--matrixark-report",
            str(longmem_matrixark),
            "--baseline-report",
            str(longmem_baseline),
            "--contract-validation",
            str(longmem_validation),
            "--output-json",
            str(out / "oss_benchmark_summary.json"),
            "--output-md",
            str(out / "oss_benchmark_summary.md"),
        ],
        repo,
        env,
    )
    print(out / "oss_benchmark_summary.md")
    return 0


def locomo_matrixark_command(repo: Path, args: argparse.Namespace, report: Path, misses: Path) -> list[str]:
    command = [
        sys.executable,
        str(repo / "tools" / "run_locomo_ingest_once.py"),
        "--input",
        args.locomo_input,
        "--output",
        str(report),
        "--misses",
        str(misses),
        "--dataset-name",
        "locomo",
        "--reader-mode",
        "open-source",
        "--reader-provider-name",
        f"matrixark-{args.reader_model}",
        "--reader-model",
        args.reader_model,
        "--reader-base-url",
        args.reader_base_url,
        "--reader-timeout-seconds",
        str(args.reader_timeout_seconds),
        "--reader-max-context-chars",
        str(args.locomo_reader_context_chars),
        "--embedding-model",
        args.embedding_model,
        "--baseline-provider-name",
        f"openviking-direct-source-{args.reader_model}",
        "--baseline-reader-model",
        args.reader_model,
        "--baseline-embedding-model",
        args.embedding_model,
        "--baseline-max-events",
        str(args.locomo_max_events),
        "--baseline-reader-max-context-chars",
        str(args.locomo_reader_context_chars),
        "--max-events",
        str(args.locomo_max_events),
        "--min-hit-rate",
        smoke_or(args, "0.0", "0.90"),
        "--min-reader-hit-rate",
        smoke_or(args, "0.0", "0.0"),
        "--min-token-reduction-percent",
        smoke_or(args, "0.0", "0.0"),
        "--max-retrieval-p95-ms",
        smoke_or(args, "999999.0", "1000.0"),
        "--max-reader-p95-ms",
        smoke_or(args, "999999.0", "30000.0"),
        "--adaptive-max-events",
        "--adaptive-base-max-events",
        str(args.locomo_adaptive_base_max_events),
        "--question-limit",
        str(args.locomo_question_limit),
        "--retrieval-same-session-percent",
        str(args.same_session_percent),
        "--retrieval-cross-session-percent",
        str(args.cross_session_percent),
        "--retrieval-summary-percent",
        str(args.summary_percent),
        "--retrieval-entity-percent",
        str(args.entity_percent),
        "--retrieval-event-percent",
        str(args.event_percent),
        "--reader-include-extractive-hint",
        "--reader-candidate-only",
        "--reader-no-fallback",
        "--require-open-source-reader",
        "--require-shared-oss-models",
    ]
    if args.allow_python_only_diagnostic:
        command.extend(["--skip-rust-temporalstore", "--allow-python-only-diagnostic"])
    return command


def longmem_matrixark_command(repo: Path, args: argparse.Namespace, report: Path, misses: Path) -> list[str]:
    command = [
        sys.executable,
        str(repo / "tools" / "run_longmemeval_s_full_path.py"),
        "--input",
        args.longmem_input,
        "--report",
        str(report),
        "--misses",
        str(misses),
        "--threshold-profile",
        smoke_or(args, "custom", "longmemeval_full"),
        "--min-case-count",
        smoke_or(args, "1", "500"),
        "--min-hit-rate",
        smoke_or(args, "0.0", "0.90"),
        "--min-reader-hit-rate",
        smoke_or(args, "0.0", "0.58"),
        "--min-token-reduction-percent",
        smoke_or(args, "0.0", "80.0"),
        "--max-retrieval-p95-ms",
        smoke_or(args, "999999.0", "2000.0"),
        "--max-reader-p95-ms",
        smoke_or(args, "999999.0", "30000.0"),
        "--reader-mode",
        "open-source",
        "--reader-provider-name",
        f"matrixark-{args.reader_model}",
        "--reader-model",
        args.reader_model,
        "--reader-base-url",
        args.reader_base_url,
        "--reader-timeout-seconds",
        str(args.reader_timeout_seconds),
        "--reader-max-context-chars",
        str(args.longmem_reader_context_chars),
        "--embedding-model",
        args.embedding_model,
        "--baseline-provider-name",
        f"openviking-direct-source-{args.reader_model}",
        "--baseline-reader-model",
        args.reader_model,
        "--baseline-embedding-model",
        args.embedding_model,
        "--baseline-max-events",
        str(args.longmem_max_events),
        "--baseline-reader-max-context-chars",
        str(args.longmem_reader_context_chars),
        "--max-events",
        str(args.longmem_max_events),
        "--retrieval-same-session-percent",
        str(args.same_session_percent),
        "--retrieval-cross-session-percent",
        str(args.cross_session_percent),
        "--retrieval-summary-percent",
        str(args.summary_percent),
        "--retrieval-entity-percent",
        str(args.entity_percent),
        "--retrieval-event-percent",
        str(args.event_percent),
        "--reader-include-extractive-hint",
        "--reader-candidate-only",
        "--reader-no-fallback",
        "--require-open-source-reader",
        "--require-shared-oss-models",
    ]
    if args.longmem_question_limit > 0:
        command.extend(["--question-limit", str(args.longmem_question_limit)])
    if args.allow_python_only_diagnostic:
        command.extend(["--skip-rust-temporalstore", "--allow-python-only-diagnostic"])
    return command


def locomo_baseline_command(repo: Path, args: argparse.Namespace, matrixark_report: Path, report: Path) -> list[str]:
    return [
        sys.executable,
        str(repo / "tools" / "run_openviking_locomo_source_retrieval_baseline.py"),
        "--input",
        args.locomo_input,
        "--report",
        str(report),
        "--matrixark-report",
        str(matrixark_report),
        "--reader-base-url",
        args.reader_base_url,
        "--reader-model",
        args.reader_model,
        "--embedding-model",
        args.embedding_model,
        "--provider-name",
        f"openviking-direct-source-{args.reader_model}",
        "--reader-timeout-seconds",
        str(args.reader_timeout_seconds),
        "--reader-max-context-chars",
        str(args.locomo_reader_context_chars),
        "--max-events",
        str(args.locomo_max_events),
        "--adaptive-max-events",
        "--adaptive-base-max-events",
        str(args.locomo_adaptive_base_max_events),
        "--question-limit",
        str(args.locomo_question_limit),
        "--same-session-percent",
        str(args.same_session_percent),
        "--cross-session-percent",
        str(args.cross_session_percent),
        "--summary-percent",
        str(args.summary_percent),
        "--entity-percent",
        str(args.entity_percent),
        "--event-percent",
        str(args.event_percent),
        "--reader-include-extractive-hint",
        "--reader-candidate-only",
        "--require-shared-oss-models",
    ]


def longmem_baseline_command(repo: Path, args: argparse.Namespace, matrixark_report: Path, report: Path) -> list[str]:
    return [
        sys.executable,
        str(repo / "tools" / "run_openviking_longmem_source_retrieval_baseline.py"),
        "--input",
        args.longmem_input,
        "--report",
        str(report),
        "--matrixark-report",
        str(matrixark_report),
        "--reader-base-url",
        args.reader_base_url,
        "--reader-model",
        args.reader_model,
        "--embedding-model",
        args.embedding_model,
        "--provider-name",
        f"openviking-direct-source-{args.reader_model}",
        "--reader-timeout-seconds",
        str(args.reader_timeout_seconds),
        "--top-k",
        str(args.longmem_max_events),
        "--max-context-chars",
        str(args.longmem_reader_context_chars),
        "--same-session-percent",
        str(args.same_session_percent),
        "--cross-session-percent",
        str(args.cross_session_percent),
        "--summary-percent",
        str(args.summary_percent),
        "--entity-percent",
        str(args.entity_percent),
        "--event-percent",
        str(args.event_percent),
        "--reader-include-extractive-hint",
        "--reader-candidate-only",
        "--require-shared-oss-models",
    ]


def require_files(*paths: Path) -> None:
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        raise SystemExit(f"missing required benchmark report(s): {missing}")


def smoke_or(args: argparse.Namespace, smoke_value: str, full_value: str) -> str:
    return smoke_value if args.diagnostic_smoke else full_value


def apply_shared_oss_stack_aliases(args: argparse.Namespace) -> None:
    if args.oss_reader_model:
        args.reader_model = args.oss_reader_model
    if args.oss_encoding_model:
        args.embedding_model = args.oss_encoding_model


def write_shared_oss_stack_contract(output_root: Path, args: argparse.Namespace) -> None:
    contract = {
        "schema": "matrixark_fair_oss_shared_stack_contract_v1",
        "rule": (
            "MatrixArk, OpenViking, VikingMem, and other baselines in this suite are forced to use "
            "one shared OSS reader model and one shared encoding/embedding model. Child runners and "
            "the validator fail closed if a report drifts from this stack."
        ),
        "shared_reader_model": args.reader_model,
        "shared_encoding_model": args.embedding_model,
        "shared_embedding_model": args.embedding_model,
        "reader_base_url": args.reader_base_url,
        "reader_max_tokens": args.reader_max_tokens,
        "locomo_max_events": args.locomo_max_events,
        "longmem_max_events": args.longmem_max_events,
        "locomo_reader_context_chars": args.locomo_reader_context_chars,
        "longmem_reader_context_chars": args.longmem_reader_context_chars,
        "retrieval_budget_split": {
            "same_session_percent": args.same_session_percent,
            "cross_session_percent": args.cross_session_percent,
            "summary_percent": args.summary_percent,
            "entity_percent": args.entity_percent,
            "event_percent": args.event_percent,
        },
    }
    (output_root / "fair_oss_shared_stack_contract.json").write_text(
        json.dumps(contract, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run(command: list[str], cwd: Path, env: dict[str, str]) -> None:
    print("+ " + " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, env=env, check=True)


def run_or_record_blocker(stage: str, command: list[str], cwd: Path, env: dict[str, str], output_root: Path) -> bool:
    print("+ " + " ".join(command), flush=True)
    completed = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True)
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode == 0:
        return True
    blocker = {
        "schema": "matrixark_fair_oss_benchmark_blocker_v1",
        "stage": stage,
        "returncode": completed.returncode,
        "command": command,
        "cwd": str(cwd),
        "message": (
            "Fair OSS benchmark suite stopped before comparison because this stage failed. "
            "Do not use partial rows as MatrixArk vs OpenViking/VikingMem quality evidence."
        ),
        "stdout_tail": tail_text(completed.stdout),
        "stderr_tail": tail_text(completed.stderr),
        "known_report_paths": sorted(str(path) for path in output_root.glob("*.json")),
    }
    blocker_path = output_root / "oss_benchmark_blocker.json"
    blocker_path.write_text(json.dumps(blocker, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(blocker_path, file=sys.stderr)
    return False


def tail_text(value: str, max_chars: int = 12000) -> str:
    return value[-max_chars:] if len(value) > max_chars else value


if __name__ == "__main__":
    raise SystemExit(main())
