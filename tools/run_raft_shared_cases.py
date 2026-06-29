#!/usr/bin/env python3
"""Validate and optionally run shared Raft parity evidence cases.

The Raft corpus cases are harness-oriented ``existing_test`` entries. This
runner verifies that each shared Raft case has C++ required paths plus Rust
process/harness evidence, and it can run the combined Rust distributed Raft
parity gate once. Native C++ execution remains optional through
``TS_CPP_RAFT_NATIVE_CMD``.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "compat" / "unified_temporalstore_cases.json"
RAFT_SUITE = "cpp_data_raft_parity"
RAFT_CASES = {
    "storage_data_raft_replication_gtest",
    "raft_metaserver_membership_failover_snapshot",
    "raft_data_node_scale_failover_snapshot",
    "raft_data_node_mixed_rw_and_membership",
    "raft_data_node_leader_election_failover",
    "raft_data_node_snapshot_restart_follower_lag",
    "raft_data_node_membership_secondary_reads",
    "raft_metaserver_leader_snapshot_restart",
    "raft_metaserver_membership_add_promote_remove",
    "raft_temporal_raft_process_rollout_evidence",
    "raft_production_gate",
    "raft_rustraft_read_safety_policy",
    "raft_rustraft_metrics_admin_pipeline_status",
    "raft_rustraft_snapshot_lifecycle_depth",
    "raft_rustraft_replication_backpressure",
    "raft_rustraft_election_controls",
    "raft_rustraft_packet_loss_fault_harness",
    "raft_rustraft_slow_wal_fsync_fault_harness",
    "raft_rustraft_snapshot_during_membership_fault_harness",
    "raft_rustraft_leader_transfer_high_write_fault_harness",
    "raft_rustraft_follower_rejoin_compacted_logs_fault_harness",
    "raft_rustraft_rolling_restart_joint_consensus_fault_harness",
    "raft_rustraft_shared_fault_gate",
}
COMBINED_GATE = "tools/run_raft_distributed_parity.sh"
COMBINED_VALIDATOR_JOB = "temporalstore-raft-distributed-parity-validation"
RUSTRAFT_FAULT_ACCEPTANCE_KEYWORDS = {
    "raft_rustraft_packet_loss_fault_harness": [
        ["majority", "continues"],
        ["minority", "rejects", "stale", "reads"],
        ["healed", "catches up"],
    ],
    "raft_rustraft_slow_wal_fsync_fault_harness": [
        ["backpressure", "activates"],
        ["no committed write", "lost"],
        ["lag", "latency", "pressure"],
    ],
    "raft_rustraft_snapshot_during_membership_fault_harness": [
        ["snapshot floor", "consistent"],
        ["membership generation", "consistent"],
        ["restart", "snapshot floor", "membership generation"],
    ],
    "raft_rustraft_leader_transfer_high_write_fault_harness": [
        ["commit exactly once", "fail safely"],
        ["committed write", "lost", "duplicated"],
        ["final leader", "all committed entries"],
    ],
    "raft_rustraft_follower_rejoin_compacted_logs_fault_harness": [
        ["installs snapshot"],
        ["replays retained", "tail"],
        ["read-eligible", "catch-up"],
    ],
    "raft_rustraft_rolling_restart_joint_consensus_fault_harness": [
        ["joint consensus", "survives"],
        ["completes safely", "rolls back safely"],
        ["membership state", "not lost"],
    ],
}


def load_raft_cases(corpus_path: Path) -> list[dict]:
    with corpus_path.open("r", encoding="utf-8") as handle:
        corpus = json.load(handle)
    cases_by_name = {case.get("name"): case for case in corpus.get("cases", [])}
    missing = sorted(RAFT_CASES - set(cases_by_name))
    if missing:
        raise SystemExit(f"{corpus_path}: missing Raft shared cases: {', '.join(missing)}")
    return [cases_by_name[name] for name in sorted(RAFT_CASES)]


def validate_case(case: dict, cpp_repo: Path | None) -> tuple[int, int]:
    steps = case.get("steps")
    if not isinstance(steps, list) or not steps:
        raise SystemExit(f"{case.get('name')}: Raft case has no steps")
    rust_runners = 0
    cpp_paths = 0
    for step in steps:
        command = step.get("command", {})
        location = f"{case.get('name')}/{step.get('name')}"
        if command.get("kind") != "existing_test":
            raise SystemExit(f"{location}: Raft step must use existing_test command")
        if command.get("suite") != RAFT_SUITE:
            raise SystemExit(f"{location}: unexpected suite {command.get('suite')!r}")
        if command.get("mode") not in {"runtime", "stress"}:
            raise SystemExit(f"{location}: Raft mode must be runtime or stress")
        validate_rustraft_fault_acceptance(case, step)
        rust_runner = command.get("rust_runner")
        if not isinstance(rust_runner, str) or not rust_runner:
            raise SystemExit(f"{location}: missing rust_runner")
        if not any(token in rust_runner for token in [
            "distributed_raft_harness",
            "raft_secondary_replication_harness",
            "metaserver_raft_harness",
            "run_raft_distributed_parity.sh",
            "cargo test -p temporalstore-rust",
        ]):
            raise SystemExit(f"{location}: rust_runner is not a known Raft evidence command")
        rust_validator = command.get("rust_validator", "")
        if "validate_aws_validation_log.py" not in rust_validator and case.get("name") != "raft_temporal_raft_process_rollout_evidence":
            raise SystemExit(f"{location}: rust_validator must use validate_aws_validation_log.py")
        if case.get("name") in {
            "raft_data_node_scale_failover_snapshot",
            "raft_data_node_mixed_rw_and_membership",
        }:
            if case.get("rust_parity_gate") != COMBINED_GATE:
                raise SystemExit(f"{case.get('name')}: missing combined Rust parity gate")
            if COMBINED_VALIDATOR_JOB not in case.get("rust_parity_validator", ""):
                raise SystemExit(f"{case.get('name')}: missing combined Rust parity validator")
        required_paths = command.get("required_paths")
        if not isinstance(required_paths, list) or not required_paths:
            raise SystemExit(f"{location}: missing C++ required_paths")
        cpp_paths += len(required_paths)
        if cpp_repo is not None:
            for required_path in required_paths:
                if not (cpp_repo / required_path).exists():
                    raise SystemExit(f"{location}: C++ required path missing: {required_path}")
        rust_runners += 1
    return rust_runners, cpp_paths


def validate_rustraft_fault_acceptance(case: dict, step: dict) -> None:
    expected = RUSTRAFT_FAULT_ACCEPTANCE_KEYWORDS.get(case.get("name"))
    if expected is None:
        return
    location = f"{case.get('name')}/{step.get('name')}"
    criteria = step.get("command", {}).get("acceptance_criteria")
    if not isinstance(criteria, list) or len(criteria) < len(expected):
        raise SystemExit(f"{location}: RustRaft fault case must declare acceptance_criteria")
    normalized = [" ".join(str(item).lower().split()) for item in criteria]
    for keyword_group in expected:
        if not any(all(keyword in criterion for keyword in keyword_group) for criterion in normalized):
            raise SystemExit(
                f"{location}: acceptance_criteria missing keywords "
                + ", ".join(keyword_group)
            )


def run_command(command: str, cwd: Path) -> None:
    print(f"+ {command}", flush=True)
    subprocess.run(command, cwd=cwd, shell=True, check=True)


def render_cpp_native_command(template: str, corpus: Path, case: str, cpp_repo: Path | None) -> str:
    values = {
        "corpus": shlex.quote(str(corpus)),
        "case": shlex.quote(case),
    }
    if cpp_repo is not None:
        values["cpp_repo"] = shlex.quote(str(cpp_repo))
    if any(f"{{{key}}}" in template for key in values):
        return template.format(**values)
    return f"{template} --corpus {shlex.quote(str(corpus))} --case {shlex.quote(case)}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, default=CORPUS)
    parser.add_argument("--cpp-repo", type=Path)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument(
        "--rust-combined",
        action="store_true",
        help="run the combined Rust data-node plus metaserver Raft parity gate once",
    )
    parser.add_argument(
        "--cpp-native",
        action="store_true",
        help="run TS_CPP_RAFT_NATIVE_CMD for every Raft case",
    )
    parser.add_argument(
        "--require-cpp-native",
        action="store_true",
        help="fail unless TS_CPP_RAFT_NATIVE_CMD is configured",
    )
    args = parser.parse_args()

    cpp_repo = args.cpp_repo.resolve() if args.cpp_repo else None
    corpus = args.corpus.resolve()
    cases = load_raft_cases(corpus)
    total_rust_runners = 0
    total_cpp_paths = 0
    for case in cases:
        rust_runners, cpp_paths = validate_case(case, cpp_repo)
        total_rust_runners += rust_runners
        total_cpp_paths += cpp_paths

    print(f"raft_shared_cases={len(cases)}")
    print(f"raft_rust_runners={total_rust_runners}")
    print(f"raft_cpp_required_path_refs={total_cpp_paths}")
    print(f"raft_combined_rust_gate={COMBINED_GATE}")

    if args.validate_only:
        return 0

    if args.rust_combined:
        run_command(COMBINED_GATE, ROOT)

    native_template = os.environ.get("TS_CPP_RAFT_NATIVE_CMD")
    if args.require_cpp_native and not native_template:
        raise SystemExit("TS_CPP_RAFT_NATIVE_CMD is required for native C++ Raft execution")
    if args.cpp_native:
        if not native_template:
            raise SystemExit("set TS_CPP_RAFT_NATIVE_CMD to run C++ Raft cases")
        cwd = cpp_repo or ROOT
        for case in cases:
            print(f"== c++ Raft case: {case['name']} ==")
            run_command(render_cpp_native_command(native_template, corpus, case["name"], cpp_repo), cwd)

    return 0


if __name__ == "__main__":
    sys.exit(main())
