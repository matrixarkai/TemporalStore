# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Split out of run_locomo_ingest_once.py; re-exported at that module's end via the dual
relative/absolute import pattern so the same module object is reused under both
the package path (tools.<mod>) and the top-level path. No import-time cycle.
__all__ lists every moved name for total re-export."""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from typing import Any

try:  # package path (tools.run_locomo_ingest_once)
    from .run_locomo_ingest_once import (
        Path,
        compare_rust_python_per_query,
        decoded_tail,
        elapsed_ms,
        limit_rust_temporalstore_sources,
        merge_rust_temporalstore_harnesses,
        pack_rust_temporalstore_sources,
        parse_last_json_object,
        percentile,
        score_rust_temporalstore_jsonl_with_python,
        split_rust_temporalstore_jsonl,
        token_reduction_percent,
    )
except ImportError:  # top-level path (run_locomo_ingest_once)
    from run_locomo_ingest_once import (
        Path,
        compare_rust_python_per_query,
        decoded_tail,
        elapsed_ms,
        limit_rust_temporalstore_sources,
        merge_rust_temporalstore_harnesses,
        pack_rust_temporalstore_sources,
        parse_last_json_object,
        percentile,
        score_rust_temporalstore_jsonl_with_python,
        split_rust_temporalstore_jsonl,
        token_reduction_percent,
    )

__all__ = ['run_rust_temporalstore_backend', 'load_reusable_rust_temporalstore_report', 'rust_temporalstore_report_usable_for_benchmark', 'rust_temporalstore_report_full_replay_usable', 'write_locomo_reader_progress', 'run_rust_temporalstore_harness', 'prepare_rust_temporalstore_harness_command', 'run_rust_temporalstore_batches', 'write_rust_temporalstore_batch_progress']


def run_rust_temporalstore_backend(args: argparse.Namespace) -> dict[str, Any]:
    repo = Path(__file__).resolve().parents[1]
    input_path = Path(args.input)
    max_cases = max(0, int(args.rust_temporalstore_max_cases))
    jsonl_path = (
        Path(args.rust_temporalstore_jsonl)
        if args.rust_temporalstore_jsonl
        else Path(tempfile.gettempdir()) / f"temporalstore-rust-context-{input_path.stem}-{os.getpid()}.jsonl"
    )
    report_path = (
        Path(args.rust_temporalstore_report)
        if args.rust_temporalstore_report
        else Path(tempfile.gettempdir()) / f"temporalstore-rust-context-{input_path.stem}-{os.getpid()}.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    convert_command = [
        sys.executable,
        str(repo / "tools" / "convert_locomo_to_context_jsonl.py"),
        str(input_path),
        str(jsonl_path),
    ]
    if args.dataset_name:
        convert_command.extend(["--dataset-name", args.dataset_name])
    if max_cases:
        convert_command.extend(["--max-questions", str(max_cases)])
    if args.question_offset:
        convert_command.extend(["--question-offset", str(args.question_offset)])
    if args.evidence_window is not None:
        convert_command.extend(["--evidence-window", str(args.evidence_window)])
    converted = subprocess.run(convert_command, cwd=repo, text=True, capture_output=True, check=False)
    if converted.returncode != 0:
        raise RuntimeError(
            "Rust TemporalStore benchmark conversion failed: "
            f"{converted.stderr.strip() or converted.stdout.strip()}"
        )
    source_limit_applied = int(args.rust_temporalstore_source_limit) > 0
    # A source limit is non-negative by contract -- 0 means "all sources" -- so this is on for
    # every value the argument accepts. The harness default off is what a caller that sets no
    # benchmark environment gets.
    rust_source_order_ranking = int(args.rust_temporalstore_source_limit) >= 0
    if source_limit_applied:
        limit_rust_temporalstore_sources(jsonl_path, int(args.rust_temporalstore_source_limit), args.max_events)
    source_pack_size = int(getattr(args, "rust_temporalstore_source_pack_size", 0) or 0)
    source_packing_report = {"enabled": False, "pack_size": source_pack_size}
    if args.require_full_rust_temporalstore_replay and int(args.rust_temporalstore_source_limit) == 0 and source_pack_size > 0:
        source_packing_report = pack_rust_temporalstore_sources(jsonl_path, source_pack_size)
    python_subset_score = score_rust_temporalstore_jsonl_with_python(
        jsonl_path,
        args.max_events,
        use_source_order=rust_source_order_ranking,
    )
    converted_case_count = int(python_subset_score.get("case_count") or 0)
    batch_size = int(getattr(args, "rust_temporalstore_batch_size", 0) or 0)
    use_batch_replay = bool(args.require_full_rust_temporalstore_replay and batch_size > 0 and converted_case_count > batch_size)

    env = os.environ.copy()
    env.update(
        {
            "TEMPORALSTORE_CONTEXT_BENCHMARK_EXTERNAL_ONLY": "1",
            "TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL": str(jsonl_path),
            "TEMPORALSTORE_CONTEXT_BENCHMARK_MAX_EVENTS": str(args.max_events),
            "TEMPORALSTORE_CONTEXT_BENCHMARK_ALL_SOURCE_REPLAY": "1"
            if args.require_full_rust_temporalstore_replay and int(args.rust_temporalstore_source_limit) == 0
            else "0",
            "TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING": "0",
            "TEMPORALSTORE_CONTEXT_BENCHMARK_SOURCE_ORDER_RANKING": "1"
            if rust_source_order_ranking
            else "0",
            "TEMPORALSTORE_CONTEXT_BENCHMARK_SELECTED_ID_LIMIT": "128",
            "CARGO_TARGET_DIR": env.get(
                "CARGO_TARGET_DIR",
                str(repo / "target" / "temporalstore-context-benchmark"),
            ),
        }
    )
    command, build_report = prepare_rust_temporalstore_harness_command(
        repo=repo,
        env=env,
        release=bool(args.rust_temporalstore_release),
    )
    started = time.perf_counter()
    batch_reports: list[dict[str, Any]] = []
    if use_batch_replay:
        harness = run_rust_temporalstore_batches(
            repo=repo,
            command=command,
            base_env=env,
            source_jsonl=jsonl_path,
            batch_size=batch_size,
            timeout_seconds=float(args.rust_temporalstore_timeout_seconds),
            report_path=report_path,
        )
        completed_returncode = 0
        stdout_tail = ""
        stderr_tail = ""
        batch_reports = harness.pop("_batch_reports", [])
    else:
        try:
            completed = run_rust_temporalstore_harness(
                repo=repo,
                command=command,
                env=env,
                timeout_seconds=float(args.rust_temporalstore_timeout_seconds),
            )
        except subprocess.TimeoutExpired as exc:
            timeout_report = {
                "rust_temporalstore_backend_ready": False,
                "rust_temporalstore_full_replay_ready": False,
                "failure": "rust_temporalstore_harness_timeout",
                "timeout_seconds": args.rust_temporalstore_timeout_seconds,
                "converted_jsonl": str(jsonl_path),
                "requested_max_cases": max_cases,
                "requested_source_limit": int(args.rust_temporalstore_source_limit),
                "full_replay_requested": bool(args.require_full_rust_temporalstore_replay),
                "stdout_tail": decoded_tail(exc.stdout),
                "stderr_tail": decoded_tail(exc.stderr),
            }
            report_path.write_text(json.dumps(timeout_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            raise RuntimeError(
                "Rust TemporalStore benchmark harness timed out after "
                f"{args.rust_temporalstore_timeout_seconds}s; see {report_path}"
            ) from exc
        completed_returncode = completed.returncode
        stdout_tail = completed.stdout[-2000:]
        stderr_tail = completed.stderr[-2000:]
        if completed.returncode != 0:
            raise RuntimeError(
                "Rust TemporalStore benchmark harness failed: "
                f"{completed.stderr.strip()[-1000:] or completed.stdout.strip()[-1000:]}"
            )
        harness = parse_last_json_object(completed.stdout)
    elapsed = elapsed_ms(started)
    report: dict[str, Any] = {
        "rust_temporalstore_backend_ready": False,
        "command": command,
        "build_report": build_report,
        "converted_jsonl": str(jsonl_path),
        "converted_stdout": converted.stdout.strip()[-1000:],
        "report_path": str(report_path),
        "returncode": completed_returncode,
        "elapsed_ms": elapsed,
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
        "python_subset_score": python_subset_score,
        "requested_max_cases": max_cases,
        "requested_source_limit": int(args.rust_temporalstore_source_limit),
        "requested_batch_size": batch_size,
        "source_packing": source_packing_report,
        "rust_build_profile": "release" if args.rust_temporalstore_release else "dev",
        "batch_replay_used": use_batch_replay,
        "batch_reports": batch_reports,
        "full_replay_requested": bool(args.require_full_rust_temporalstore_replay),
        "full_replay_contract": {
            "all_dataset_cases": max_cases == 0,
            "all_cases": max_cases == 0,
            "all_converted_cases": False,
            "all_sources": int(args.rust_temporalstore_source_limit) == 0,
            "batched": use_batch_replay,
            "question_slice_diagnostic": bool(args.question_limit or args.question_offset),
        },
    }
    report["harness"] = harness
    rust_case_count = int(harness.get("external_benchmark_case_count") or 0)
    report["full_replay_contract"]["all_converted_cases"] = rust_case_count == converted_case_count
    rust_hit_at_k = float(harness.get("external_benchmark_hit_at_k") or 0.0)
    python_hit_at_k = float(python_subset_score.get("hit_at_k") or 0.0)
    rust_mean_reciprocal_rank = float(harness.get("external_benchmark_mean_reciprocal_rank") or 0.0)
    python_mean_reciprocal_rank = float(python_subset_score.get("mean_reciprocal_rank") or 0.0)
    python_zero_hit_queries = int(python_subset_score.get("zero_hit_queries") or 0)
    rust_zero_hit_queries = int(harness.get("external_benchmark_zero_hit_queries") or 0)
    hit_at_k_delta = abs(rust_hit_at_k - python_hit_at_k)
    hit_at_k_regression_delta = max(0.0, python_hit_at_k - rust_hit_at_k)
    mean_reciprocal_rank_delta = abs(rust_mean_reciprocal_rank - python_mean_reciprocal_rank)
    mean_reciprocal_rank_regression_delta = max(
        0.0, python_mean_reciprocal_rank - rust_mean_reciprocal_rank
    )
    case_count_on_par = rust_case_count == int(python_subset_score.get("case_count") or 0)
    zero_hit_queries_on_par = rust_zero_hit_queries == python_zero_hit_queries
    zero_hit_queries_no_regression = rust_zero_hit_queries <= python_zero_hit_queries
    effective_tolerance = max(float(args.rust_temporalstore_score_tolerance), 1e-6)
    rank_parity_enforced = not bool(source_packing_report.get("enabled"))
    all_source_replay_ready = bool(harness.get("external_benchmark_all_source_replay"))
    direct_source_scoring = bool(harness.get("external_benchmark_direct_source_scoring"))
    context_event_ingest_ready = bool(harness.get("external_benchmark_rust_context_event_ingest"))
    ingested_source_sets = int(harness.get("external_benchmark_ingested_source_sets") or 0)
    retrieved_source_sets = int(harness.get("external_benchmark_retrieved_source_sets") or 0)
    retrieved_blocks = int(harness.get("external_benchmark_total_retrieved_blocks") or 0)
    score_on_par = (
        hit_at_k_delta <= effective_tolerance
        and (mean_reciprocal_rank_delta <= effective_tolerance or not rank_parity_enforced)
        and case_count_on_par
        and zero_hit_queries_on_par
    )
    score_no_regression = (
        hit_at_k_regression_delta <= effective_tolerance
        and (
            mean_reciprocal_rank_regression_delta <= effective_tolerance
            or not rank_parity_enforced
        )
        and case_count_on_par
        and zero_hit_queries_no_regression
    )
    per_query_delta = compare_rust_python_per_query(
        python_subset_score.get("per_query") or [],
        harness.get("external_benchmark_per_query") or [],
    )
    selected_source_ids_match = (
        int(per_query_delta.get("selected_source_id_delta_count") or 0) == 0
    )
    backend_quality_ready = (
        hit_at_k_regression_delta <= effective_tolerance
        and case_count_on_par
        and zero_hit_queries_no_regression
        and bool(per_query_delta.get("no_regression"))
    )
    report["rust_vs_python_subset_score"] = {
        "python_hit_at_k": python_hit_at_k,
        "rust_hit_at_k": rust_hit_at_k,
        "absolute_delta": hit_at_k_delta,
        "hit_at_k_delta": hit_at_k_delta,
        "hit_at_k_regression_delta": hit_at_k_regression_delta,
        "mean_reciprocal_rank_delta": mean_reciprocal_rank_delta,
        "mean_reciprocal_rank_regression_delta": mean_reciprocal_rank_regression_delta,
        "rank_parity_enforced": rank_parity_enforced,
        "tolerance": args.rust_temporalstore_score_tolerance,
        "effective_tolerance": effective_tolerance,
        "on_par": score_on_par,
        "no_regression": score_no_regression,
        "python_case_count": python_subset_score.get("case_count"),
        "rust_case_count": rust_case_count,
        "case_count_on_par": case_count_on_par,
        "python_mean_reciprocal_rank": python_mean_reciprocal_rank,
        "rust_mean_reciprocal_rank": rust_mean_reciprocal_rank,
        "python_zero_hit_queries": python_zero_hit_queries,
        "rust_zero_hit_queries": rust_zero_hit_queries,
        "zero_hit_queries_on_par": zero_hit_queries_on_par,
        "zero_hit_queries_no_regression": zero_hit_queries_no_regression,
        "selected_source_ids_match": selected_source_ids_match,
        "backend_quality_ready": backend_quality_ready,
        "per_query_delta": per_query_delta,
    }
    parity_ready = (
        rust_case_count > 0
        and rust_hit_at_k > 0.0
        and backend_quality_ready
        and context_event_ingest_ready
        and not direct_source_scoring
        and ingested_source_sets > 0
        and retrieved_source_sets > 0
        and retrieved_blocks > 0
        and str(harness.get("external_benchmark_source") or "") == str(jsonl_path)
    )
    benchmark_usable = (
        rust_case_count > 0
        and rust_hit_at_k >= 0.90
        and context_event_ingest_ready
        and not direct_source_scoring
        and ingested_source_sets > 0
        and retrieved_source_sets > 0
        and retrieved_blocks > 0
        and str(harness.get("external_benchmark_source") or "") == str(jsonl_path)
        and (
            backend_quality_ready
            or bool(source_packing_report.get("enabled"))
        )
    )
    report["rust_temporalstore_backend_ready"] = parity_ready or benchmark_usable
    report["rust_temporalstore_backend_parity_ready"] = parity_ready
    report["rust_temporalstore_backend_benchmark_usable"] = benchmark_usable
    full_converted_replay_ready = (
        report["rust_temporalstore_backend_ready"]
        and bool(args.require_full_rust_temporalstore_replay)
        and int(args.rust_temporalstore_source_limit) == 0
        and rust_case_count == converted_case_count
        and all_source_replay_ready
    )
    full_dataset_replay_ready = full_converted_replay_ready and max_cases == 0
    report["rust_temporalstore_full_replay_ready"] = full_converted_replay_ready
    report["rust_temporalstore_full_dataset_replay_ready"] = full_dataset_replay_ready
    report["rust_temporalstore_context_event_ingest_ready"] = context_event_ingest_ready
    report["rust_temporalstore_direct_source_scoring"] = direct_source_scoring
    report["rust_temporalstore_all_source_replay"] = all_source_replay_ready
    report["rust_temporalstore_ingested_source_sets"] = ingested_source_sets
    report["rust_temporalstore_retrieved_source_sets"] = retrieved_source_sets
    report["rust_temporalstore_total_retrieved_blocks"] = retrieved_blocks
    report["rust_temporalstore_strict_external_ready"] = bool(harness.get("external_benchmark_ready"))
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not report["rust_temporalstore_backend_ready"]:
        raise RuntimeError(f"Rust TemporalStore backend did not report ready; see {report_path}")
    return report


def load_reusable_rust_temporalstore_report(args: argparse.Namespace) -> dict[str, Any] | None:
    if not bool(getattr(args, "reuse_rust_temporalstore_report", False)):
        return None
    report_arg = str(getattr(args, "rust_temporalstore_report", "") or "").strip()
    if not report_arg:
        raise RuntimeError("--reuse-rust-temporalstore-report requires --rust-temporalstore-report")
    report_path = Path(report_arg)
    if not report_path.exists():
        raise RuntimeError(f"Rust TemporalStore report does not exist: {report_path}")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict):
        raise RuntimeError(f"Rust TemporalStore report is not a JSON object: {report_path}")
    if not rust_temporalstore_report_usable_for_benchmark(report):
        raise RuntimeError(f"Rust TemporalStore report is not ready: {report_path}")
    if bool(getattr(args, "require_full_rust_temporalstore_replay", False)) and not rust_temporalstore_report_full_replay_usable(
        report
    ):
        raise RuntimeError(f"Rust TemporalStore report is not full-replay ready: {report_path}")
    report = dict(report)
    report["reused_existing_report"] = True
    report["reused_report_path"] = str(report_path)
    return report


def rust_temporalstore_report_usable_for_benchmark(report: dict[str, Any]) -> bool:
    if bool(report.get("rust_temporalstore_backend_ready")):
        return True
    if bool(report.get("rust_temporalstore_backend_benchmark_usable")):
        return True
    harness = report.get("harness") if isinstance(report.get("harness"), dict) else {}
    source_packing = report.get("source_packing") if isinstance(report.get("source_packing"), dict) else {}
    return (
        float(harness.get("external_benchmark_hit_at_k") or 0.0) >= 0.90
        and bool(harness.get("external_benchmark_rust_context_event_ingest"))
        and not bool(harness.get("external_benchmark_direct_source_scoring"))
        and int(harness.get("external_benchmark_ingested_source_sets") or 0) > 0
        and int(harness.get("external_benchmark_retrieved_source_sets") or 0) > 0
        and int(harness.get("external_benchmark_total_retrieved_blocks") or 0) > 0
        and bool(source_packing.get("enabled"))
    )


def rust_temporalstore_report_full_replay_usable(report: dict[str, Any]) -> bool:
    if bool(report.get("rust_temporalstore_full_replay_ready")):
        return True
    if not rust_temporalstore_report_usable_for_benchmark(report):
        return False
    harness = report.get("harness") if isinstance(report.get("harness"), dict) else {}
    contract = report.get("full_replay_contract") if isinstance(report.get("full_replay_contract"), dict) else {}
    return (
        bool(harness.get("external_benchmark_all_source_replay"))
        and bool(contract.get("all_converted_cases", contract.get("all_cases")))
        and bool(contract.get("all_sources"))
    )


def write_locomo_reader_progress(
    *,
    progress_path: Path,
    phase: str,
    completed_queries: int,
    hit_count: int,
    reader_hit_count: int,
    reader_error_count: int,
    reader_fallback_count: int,
    open_source_calls: int,
    total_source_tokens: int,
    total_retrieved_tokens: int,
    retrieval_latencies_ms: list[float],
    reader_latencies_ms: list[float],
    last_query_id: str,
) -> None:
    progress = {
        "schema": "matrixark_locomo_reader_progress_v1",
        "phase": phase,
        "completed_queries": completed_queries,
        "retrieval_hit_at_k": hit_count / completed_queries if completed_queries else 0.0,
        "reader_hit_rate": reader_hit_count / completed_queries if completed_queries else 0.0,
        "reader_error_count": reader_error_count,
        "reader_fallback_count": reader_fallback_count,
        "reader_open_source_calls": open_source_calls,
        "token_reduction_percent": token_reduction_percent(total_source_tokens, total_retrieved_tokens),
        "retrieval_p95_ms": percentile(retrieval_latencies_ms, 95),
        "reader_p95_ms": percentile(reader_latencies_ms, 95),
        "last_query_id": last_query_id,
    }
    progress_path.write_text(json.dumps(progress, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_rust_temporalstore_harness(
    *,
    repo: Path,
    command: list[str],
    env: dict[str, str],
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=repo,
        env=env,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout_seconds,
    )


def prepare_rust_temporalstore_harness_command(
    *,
    repo: Path,
    env: dict[str, str],
    release: bool,
) -> tuple[list[str], dict[str, Any]]:
    started = time.perf_counter()
    build_command = ["cargo", "build"]
    if release:
        build_command.append("--release")
    build_command.extend(["-p", "temporalstore-rust", "--bin", "context_workflow_harness"])
    completed = subprocess.run(
        build_command,
        cwd=repo,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    target_dir = Path(env.get("CARGO_TARGET_DIR") or repo / "target")
    profile = "release" if release else "debug"
    exe_name = "context_workflow_harness.exe" if os.name == "nt" else "context_workflow_harness"
    binary = target_dir / profile / exe_name
    report = {
        "command": build_command,
        "returncode": completed.returncode,
        "elapsed_ms": elapsed_ms(started),
        "stdout_tail": completed.stdout[-2000:],
        "stderr_tail": completed.stderr[-2000:],
        "binary": str(binary),
        "profile": profile,
    }
    if completed.returncode != 0:
        raise RuntimeError(
            "Rust TemporalStore benchmark harness build failed: "
            f"{completed.stderr.strip()[-1000:] or completed.stdout.strip()[-1000:]}"
        )
    if not binary.exists():
        raise RuntimeError(f"Rust TemporalStore benchmark harness binary missing after build: {binary}")
    return [str(binary)], report


def run_rust_temporalstore_batches(
    *,
    repo: Path,
    command: list[str],
    base_env: dict[str, str],
    source_jsonl: Path,
    batch_size: int,
    timeout_seconds: float,
    report_path: Path,
) -> dict[str, Any]:
    batch_paths = split_rust_temporalstore_jsonl(source_jsonl, batch_size)
    harnesses: list[dict[str, Any]] = []
    batch_reports: list[dict[str, Any]] = []
    progress_path = report_path.with_suffix(report_path.suffix + ".progress.json")
    write_rust_temporalstore_batch_progress(
        progress_path=progress_path,
        report_path=report_path,
        source_jsonl=source_jsonl,
        batch_paths=batch_paths,
        batch_size=batch_size,
        current_batch_index=0,
        phase="starting",
        batch_reports=batch_reports,
    )
    for index, batch_path in enumerate(batch_paths, start=1):
        env = dict(base_env)
        env["TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL"] = str(batch_path)
        started = time.perf_counter()
        write_rust_temporalstore_batch_progress(
            progress_path=progress_path,
            report_path=report_path,
            source_jsonl=source_jsonl,
            batch_paths=batch_paths,
            batch_size=batch_size,
            current_batch_index=index,
            phase="running_batch",
            batch_reports=batch_reports,
        )
        try:
            completed = run_rust_temporalstore_harness(
                repo=repo,
                command=command,
                env=env,
                timeout_seconds=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            failure = {
                "rust_temporalstore_backend_ready": False,
                "rust_temporalstore_full_replay_ready": False,
                "failure": "rust_temporalstore_batch_timeout",
                "failed_batch_index": index,
                "batch_count": len(batch_paths),
                "batch_path": str(batch_path),
                "batch_size": batch_size,
                "timeout_seconds": timeout_seconds,
                "stdout_tail": decoded_tail(exc.stdout),
                "stderr_tail": decoded_tail(exc.stderr),
                "completed_batches": batch_reports,
            }
            report_path.write_text(json.dumps(failure, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            raise RuntimeError(
                "Rust TemporalStore full replay batch timed out "
                f"at batch {index}/{len(batch_paths)} after {timeout_seconds}s; see {report_path}"
            ) from exc
        batch_report = {
            "batch_index": index,
            "batch_count": len(batch_paths),
            "batch_path": str(batch_path),
            "returncode": completed.returncode,
            "elapsed_ms": elapsed_ms(started),
            "stdout_tail": completed.stdout[-1000:],
            "stderr_tail": completed.stderr[-1000:],
        }
        if completed.returncode != 0:
            failure = {
                "rust_temporalstore_backend_ready": False,
                "rust_temporalstore_full_replay_ready": False,
                "failure": "rust_temporalstore_batch_failed",
                "failed_batch": batch_report,
                "completed_batches": batch_reports,
            }
            report_path.write_text(json.dumps(failure, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            raise RuntimeError(
                "Rust TemporalStore full replay batch failed: "
                f"{completed.stderr.strip()[-1000:] or completed.stdout.strip()[-1000:]}"
            )
        harness = parse_last_json_object(completed.stdout)
        batch_report.update(
            {
                "case_count": int(harness.get("external_benchmark_case_count") or 0),
                "hit_at_k": float(harness.get("external_benchmark_hit_at_k") or 0.0),
                "mean_reciprocal_rank": float(harness.get("external_benchmark_mean_reciprocal_rank") or 0.0),
                "zero_hit_queries": int(harness.get("external_benchmark_zero_hit_queries") or 0),
            }
        )
        harnesses.append(harness)
        batch_reports.append(batch_report)
        write_rust_temporalstore_batch_progress(
            progress_path=progress_path,
            report_path=report_path,
            source_jsonl=source_jsonl,
            batch_paths=batch_paths,
            batch_size=batch_size,
            current_batch_index=index,
            phase="batch_complete",
            batch_reports=batch_reports,
        )
    merged = merge_rust_temporalstore_harnesses(harnesses, str(source_jsonl))
    merged["_batch_reports"] = batch_reports
    write_rust_temporalstore_batch_progress(
        progress_path=progress_path,
        report_path=report_path,
        source_jsonl=source_jsonl,
        batch_paths=batch_paths,
        batch_size=batch_size,
        current_batch_index=len(batch_paths),
        phase="complete",
        batch_reports=batch_reports,
    )
    return merged


def write_rust_temporalstore_batch_progress(
    *,
    progress_path: Path,
    report_path: Path,
    source_jsonl: Path,
    batch_paths: list[Path],
    batch_size: int,
    current_batch_index: int,
    phase: str,
    batch_reports: list[dict[str, Any]],
) -> None:
    completed_case_count = sum(int(row.get("case_count") or 0) for row in batch_reports)
    completed_hit_count = sum(
        round(float(row.get("hit_at_k") or 0.0) * int(row.get("case_count") or 0))
        for row in batch_reports
    )
    progress = {
        "schema": "matrixark_rust_temporalstore_batch_replay_progress_v1",
        "phase": phase,
        "report_path": str(report_path),
        "source_jsonl": str(source_jsonl),
        "batch_size": batch_size,
        "batch_count": len(batch_paths),
        "current_batch_index": current_batch_index,
        "completed_batch_count": len(batch_reports),
        "remaining_batch_count": max(0, len(batch_paths) - len(batch_reports)),
        "completed_case_count": completed_case_count,
        "completed_hit_count": completed_hit_count,
        "completed_hit_at_k": completed_hit_count / completed_case_count if completed_case_count else 0.0,
        "completed_batches": batch_reports,
    }
    progress_path.write_text(json.dumps(progress, indent=2, sort_keys=True) + "\n", encoding="utf-8")
