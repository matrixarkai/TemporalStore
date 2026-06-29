#!/usr/bin/env python3
"""Run the Rust LOCOMO plus three-data-node scale gate.

This gate intentionally composes the current Rust evidence paths:

* LOCOMO ingestion/extraction/retrieval through Rust TemporalStore full replay.
* Three-node Rust data-node/Raft scale harness.
* Three-process secondary replication harness.

The repository does not yet have a single LOCOMO runner that routes every query
through three external data-node processes. This script makes that distinction
explicit while still failing closed on the scale/replication conditions that can
hurt the context pipeline.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Rust three-node LOCOMO scale validation.")
    parser.add_argument("--worktree", default=".", help="TemporalStore Rust worktree to run from.")
    parser.add_argument("--input", default="/tmp/locomo10.json", help="LOCOMO JSON input path.")
    parser.add_argument("--out-dir", default="benchmark_reports/three_node_locomo_scale")
    parser.add_argument("--skip-build", action="store_true", help="Reuse existing release binaries.")
    parser.add_argument("--reuse-existing", action="store_true", help="Validate existing reports without rerunning.")
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--string-ops", type=int, default=1000)
    parser.add_argument("--hash-ops", type=int, default=250)
    parser.add_argument("--sequence-keys", type=int, default=4)
    parser.add_argument("--sequence-len", type=int, default=500)
    parser.add_argument("--scale-events", type=int, default=2)
    parser.add_argument("--failover-every", type=int, default=250)
    parser.add_argument("--read-sample-every", type=int, default=100)
    parser.add_argument("--shared-store-ops", type=int, default=1000)
    parser.add_argument("--shared-store-flush-every", type=int, default=25)
    parser.add_argument("--heartbeat-ms", type=int, default=25)
    parser.add_argument("--locomo-timeout-seconds", type=int, default=2400)
    parser.add_argument("--locomo-batch-size", type=int, default=16)
    parser.add_argument("--locomo-source-pack-size", type=int, default=24)
    parser.add_argument("--max-raft-replica-lag", type=int, default=0)
    parser.add_argument(
        "--max-async-shared-store-lag",
        type=int,
        default=None,
        help="Default is --shared-store-flush-every - 1.",
    )
    parser.add_argument("--report", default="", help="Combined JSON report path.")
    return parser.parse_args()


def run(command: list[str], cwd: Path, stdout_path: Path | None = None) -> None:
    stdout = None
    try:
        if stdout_path is not None:
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stdout = stdout_path.open("w", encoding="utf-8")
        subprocess.run(command, cwd=cwd, check=True, stdout=stdout)
    finally:
        if stdout is not None:
            stdout.close()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def require(condition: bool, message: str, blockers: list[str]) -> None:
    if not condition:
        blockers.append(message)


def main() -> int:
    args = parse_args()
    worktree = Path(args.worktree).resolve()
    out_dir = (worktree / args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.report) if args.report else out_dir / "three_node_locomo_scale_gate.json"
    if not report_path.is_absolute():
        report_path = worktree / report_path

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"missing LOCOMO input: {input_path}", file=sys.stderr)
        return 2

    scale_report = out_dir / "scale_harness_3nodes.json"
    secondary_report = out_dir / "raft_secondary_replication_3nodes.json"
    locomo_report = out_dir / "locomo_full_rust_report.json"
    locomo_misses = out_dir / "locomo_full_rust_misses.jsonl"
    locomo_jsonl = out_dir / "locomo_full_rust_context.jsonl"
    locomo_backend = out_dir / "locomo_full_rust_backend.json"
    shared_store_root = Path("/tmp/ts-three-node-locomo-scale-shared-store")
    secondary_root = Path("/tmp/ts-three-node-locomo-secondary-raft")

    started = time.time()
    if not args.reuse_existing:
        if not args.skip_build:
            run(["cargo", "build", "--release", "-p", "temporalstore-rust", "--bins"], cwd=worktree)
        run(
            [
                str(worktree / "target/release/scale_harness"),
                "--nodes",
                str(args.nodes),
                "--string-ops",
                str(args.string_ops),
                "--hash-ops",
                str(args.hash_ops),
                "--sequence-keys",
                str(args.sequence_keys),
                "--sequence-len",
                str(args.sequence_len),
                "--scale-events",
                str(args.scale_events),
                "--failover-every",
                str(args.failover_every),
                "--read-sample-every",
                str(args.read_sample_every),
                "--compare-shared-store",
                "true",
                "--shared-store-ops",
                str(args.shared_store_ops),
                "--shared-store-flush-every",
                str(args.shared_store_flush_every),
                "--shared-store-root",
                str(shared_store_root),
            ],
            cwd=worktree,
            stdout_path=scale_report,
        )
        run(
            [
                str(worktree / "target/release/raft_secondary_replication_harness"),
                "--root",
                str(secondary_root),
                "--heartbeat-ms",
                str(args.heartbeat_ms),
            ],
            cwd=worktree,
            stdout_path=secondary_report,
        )
        run(
            [
                sys.executable,
                str(worktree / "tools/run_locomo_90_hit_rate.py"),
                "--input",
                str(input_path),
                "--threshold-profile",
                "locomo_full",
                "--require-rust-temporalstore",
                "--require-full-rust-temporalstore-replay",
                "--rust-temporalstore-release",
                "--rust-temporalstore-max-cases",
                "0",
                "--rust-temporalstore-source-limit",
                "0",
                "--rust-temporalstore-batch-size",
                str(args.locomo_batch_size),
                "--rust-temporalstore-source-pack-size",
                str(args.locomo_source_pack_size),
                "--rust-temporalstore-timeout-seconds",
                str(args.locomo_timeout_seconds),
                "--report",
                str(locomo_report),
                "--misses",
                str(locomo_misses),
                "--rust-temporalstore-jsonl",
                str(locomo_jsonl),
                "--rust-temporalstore-report",
                str(locomo_backend),
            ],
            cwd=worktree,
        )

    scale = load_json(scale_report)
    secondary = load_json(secondary_report)
    locomo = load_json(locomo_report)
    backend = load_json(locomo_backend)
    shared_store = scale.get("shared_store") or {}
    rollout = secondary.get("temporal_raft_process_rollout") or {}
    max_async_lag = (
        args.max_async_shared_store_lag
        if args.max_async_shared_store_lag is not None
        else max(args.shared_store_flush_every - 1, 0)
    )

    blockers: list[str] = []
    require(args.nodes == 3, "gate must run exactly three data nodes", blockers)
    require(locomo.get("all_pipelines_use_rust_temporalstore") is True, "LOCOMO did not use Rust TemporalStore", blockers)
    require(locomo.get("rust_temporalstore_backend_ready") is True, "Rust TemporalStore backend not ready", blockers)
    require(locomo.get("rust_temporalstore_full_replay_ready") is True, "full Rust LOCOMO replay not ready", blockers)
    require(locomo.get("python_only_diagnostic") is False, "LOCOMO ran as Python-only diagnostic", blockers)
    require(locomo.get("benchmark_threshold_passed") is True, "LOCOMO threshold failed", blockers)
    require(scale.get("replication_healthy") is True, "three-node scale replication is unhealthy", blockers)
    require(int(scale.get("max_replica_lag") or 0) <= args.max_raft_replica_lag, "Raft replica lag exceeded limit", blockers)
    require(int(shared_store.get("sync_max_lag") or 0) == 0, "sync shared-store lag was nonzero", blockers)
    require(
        int(shared_store.get("async_max_lag") or 0) <= max_async_lag,
        "async shared-store lag exceeded flush-window bound",
        blockers,
    )
    require((secondary.get("failover") or {}).get("status", {}).get("ok") is True, "secondary failover failed", blockers)
    require(rollout.get("ready") is True, "TemporalRaft process rollout was not ready", blockers)
    require(rollout.get("multi_process_log_store_validated") is True, "multi-process log store not validated", blockers)
    require(rollout.get("restart_recovery_validated") is True, "restart recovery not validated", blockers)
    require(rollout.get("applied_fence_validated") is True, "applied fence not validated", blockers)

    combined = {
        "schema": "temporalstore_three_node_locomo_scale_gate_v1",
        "ready": not blockers,
        "blockers": blockers,
        "elapsed_seconds": round(time.time() - started, 3),
        "worktree": str(worktree),
        "input": str(input_path),
        "reports": {
            "scale": str(scale_report),
            "secondary_replication": str(secondary_report),
            "locomo": str(locomo_report),
            "locomo_backend": str(locomo_backend),
            "locomo_misses": str(locomo_misses),
        },
        "locomo": {
            "case_count": locomo.get("case_count"),
            "conversation_count": locomo.get("conversation_count"),
            "input_sha256": locomo.get("input_sha256"),
            "all_pipelines_use_rust_temporalstore": locomo.get("all_pipelines_use_rust_temporalstore"),
            "rust_temporalstore_backend_ready": locomo.get("rust_temporalstore_backend_ready"),
            "rust_temporalstore_full_replay_ready": locomo.get("rust_temporalstore_full_replay_ready"),
            "hit_at_k": locomo.get("benchmark_hit_at_k"),
            "reader_hit_rate": locomo.get("reader_hit_rate"),
            "mrr": locomo.get("benchmark_mean_reciprocal_rank"),
            "token_reduction_percent": locomo.get("benchmark_token_reduction_percent"),
            "retrieval_p95_ms": locomo.get("benchmark_retrieval_p95_ms"),
            "reader_p95_ms": locomo.get("benchmark_reader_p95_ms"),
            "zero_hit_queries": locomo.get("zero_hit_queries"),
            "threshold_passed": locomo.get("benchmark_threshold_passed"),
            "reader_mode": locomo.get("reader_mode_effective"),
        },
        "rust_backend": {
            "returncode": backend.get("returncode"),
            "batch_replay_used": backend.get("batch_replay_used"),
            "build_profile": backend.get("rust_build_profile"),
            "all_source_replay": backend.get("rust_temporalstore_all_source_replay"),
            "ingested_source_sets": backend.get("rust_temporalstore_ingested_source_sets"),
            "retrieved_source_sets": backend.get("rust_temporalstore_retrieved_source_sets"),
            "total_retrieved_blocks": backend.get("rust_temporalstore_total_retrieved_blocks"),
            "source_packing": backend.get("source_packing"),
        },
        "three_node_scale": {
            "nodes": args.nodes,
            "final_nodes": scale.get("final_nodes"),
            "commit_index": scale.get("commit_index"),
            "failovers": scale.get("failovers"),
            "scale_events": scale.get("scale_events"),
            "write_ops_per_sec": scale.get("write_ops_per_sec"),
            "max_replica_lag": scale.get("max_replica_lag"),
            "replication_healthy": scale.get("replication_healthy"),
            "raft_write_latency": scale.get("raft_write_latency"),
            "raft_replica_read_latency": scale.get("raft_replica_read_latency"),
            "slo_report": scale.get("slo_report"),
        },
        "secondary_replication": {
            "restarted_secondary": secondary.get("restarted_secondary"),
            "lagging_follower": secondary.get("lagging_follower"),
            "failover_status": (secondary.get("failover") or {}).get("status"),
            "temporal_raft_process_rollout": rollout,
            "partition": secondary.get("partition"),
        },
        "shared_store": {
            "sync_max_lag": shared_store.get("sync_max_lag"),
            "async_max_lag": shared_store.get("async_max_lag"),
            "async_flush_every": shared_store.get("async_flush_every"),
            "async_lag_bound": max_async_lag,
            "sync_primary_write_latency": shared_store.get("sync_primary_write_latency"),
            "async_primary_write_latency": shared_store.get("async_primary_write_latency"),
            "sync_replica_read_latency": shared_store.get("sync_replica_read_latency"),
            "async_replica_read_latency": shared_store.get("async_replica_read_latency"),
        },
        "honesty_note": (
            "LOCOMO uses Rust TemporalStore full replay; three-node data-node scale and secondary "
            "replication are validated in the same gate, but the current LOCOMO runner does not "
            "route each query through three external server processes."
        ),
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(combined, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(combined, indent=2, sort_keys=True))
    return 0 if not blockers else 1


if __name__ == "__main__":
    sys.exit(main())
