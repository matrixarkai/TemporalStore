#!/usr/bin/env python3
"""Run the C++/Rust storage-engine parity gates phase by phase.

This is the umbrella gate for the nine storage parity phases:

1. canonical public contract
2. read/write/cold-scan sequences
3. StorageManager/StoreManager lifecycle
4. page/block/slot/index behavior
5. multi-layer cache behavior
6. eviction, GC, compaction, and reclaim
7. public config parity
8. metrics/report parity
9. shared storage/proxy/Raft evidence

It intentionally composes the focused validators instead of duplicating their
logic, so a failing phase points back to the exact lower-level gate to fix.
"""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
from dataclasses import dataclass


ROOT = pathlib.Path(__file__).resolve().parents[1]
TOOLS = ROOT / "tools"


@dataclass(frozen=True)
class Phase:
    number: int
    name: str
    validators: tuple[str, ...]


PHASES: tuple[Phase, ...] = (
    Phase(
        1,
        "canonical public contract",
        (
            "validate_storage_lifecycle_parity.py",
            "validate_page_address_compatibility_corpus.py",
        ),
    ),
    Phase(
        2,
        "read/write/cold-scan sequences",
        ("validate_storage_lifecycle_parity.py",),
    ),
    Phase(
        3,
        "StorageManager/StoreManager lifecycle",
        (
            "validate_storage_lifecycle_parity.py",
            "validate_raft_storage_parity_evidence.py",
        ),
    ),
    Phase(
        4,
        "page/block/slot/index behavior",
        (
            "validate_page_address_compatibility_corpus.py",
            "validate_page_block_metrics_parity.py",
            "validate_storage_proxy_client_parity_coverage.py",
        ),
    ),
    Phase(
        5,
        "multi-layer cache behavior",
        (
            "validate_storage_lifecycle_parity.py",
            "validate_grafana_metrics_parity.py",
        ),
    ),
    Phase(
        6,
        "eviction, GC, compaction, and reclaim",
        (
            "validate_storage_lifecycle_parity.py",
            "validate_page_address_compatibility_corpus.py",
            "validate_raft_storage_parity_evidence.py",
        ),
    ),
    Phase(
        7,
        "public config parity",
        ("validate_storage_tuning_parity.py",),
    ),
    Phase(
        8,
        "metrics/report parity",
        (
            "validate_page_block_metrics_parity.py",
            "validate_grafana_metrics_parity.py",
            "test_temporalstore_performance_artifact_audit.py",
            "test_temporalstore_next_performance_workflow.py",
            "test_temporalstore_performance_evidence_import.py",
            "validate_temporalstore_cpp_rust_performance_parity.py",
        ),
    ),
    Phase(
        9,
        "shared storage/proxy/Raft evidence",
        (
            "validate_temporalstore_cpp_rust_feature_execution.py",
            "validate_storage_proxy_client_parity_coverage.py",
            "validate_raft_storage_parity_evidence.py",
            "validate_page_address_compatibility_corpus.py",
            "validate_temporalstore_cpp_rust_goal_parity.py",
        ),
    ),
)


def run_validator(script_name: str) -> None:
    script = TOOLS / script_name
    if not script.exists():
        raise FileNotFoundError(f"missing validator: {script}")
    completed = subprocess.run(
        [sys.executable, str(script)],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if completed.stdout:
        print(completed.stdout.rstrip())
    if completed.returncode != 0:
        raise RuntimeError(f"{script_name} failed with exit code {completed.returncode}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the nine C++/Rust storage-engine parity phases."
    )
    parser.add_argument(
        "--loops",
        type=int,
        default=1,
        help="Repeat the full nine-phase sequence this many times.",
    )
    args = parser.parse_args()
    if args.loops < 1:
        parser.error("--loops must be at least 1")

    total_validator_runs = 0
    for loop in range(1, args.loops + 1):
        print(f"storage_engine_parity_loop={loop}/{args.loops}")
        for phase in PHASES:
            print(f"phase_{phase.number}: {phase.name}")
            for validator in phase.validators:
                print(f"  validator={validator}")
                run_validator(validator)
                total_validator_runs += 1

    print(
        "storage_engine_9_phase_parity_passed "
        f"loops={args.loops} phases={len(PHASES)} "
        f"validator_runs={total_validator_runs}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
