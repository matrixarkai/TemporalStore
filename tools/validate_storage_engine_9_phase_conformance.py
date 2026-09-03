#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
"""Run the conformance storage-engine gates phase by phase.

This is the umbrella gate for the nine storage conformance phases:

1. canonical public contract
2. read/write/cold-scan sequences
3. StorageManager/StoreManager lifecycle
4. page/block/slot/index behavior
5. multi-layer cache behavior
6. eviction, GC, compaction, and reclaim
7. public config agreement
8. metrics/report agreement
9. shared storage/proxy/Raft evidence

It intentionally composes the focused validators instead of duplicating their
logic, so a failing phase points back to the exact lower-level gate to fix.
"""

from __future__ import annotations

import argparse
import json
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
            "validate_storage_lifecycle_conformance.py",
            "validate_page_address_compatibility_corpus.py",
        ),
    ),
    Phase(
        2,
        "read/write/cold-scan sequences",
        ("validate_storage_lifecycle_conformance.py",),
    ),
    Phase(
        3,
        "StorageManager/StoreManager lifecycle",
        (
            "validate_storage_lifecycle_conformance.py",
            "validate_raft_storage_conformance_evidence.py",
        ),
    ),
    Phase(
        4,
        "page/block/slot/index behavior",
        (
            "validate_page_address_compatibility_corpus.py",
            "validate_page_block_metrics_conformance.py",
            "validate_storage_proxy_client_conformance_coverage.py",
        ),
    ),
    Phase(
        5,
        "multi-layer cache behavior",
        (
            "validate_storage_lifecycle_conformance.py",
            "validate_grafana_metrics_conformance.py",
        ),
    ),
    Phase(
        6,
        "eviction, GC, compaction, and reclaim",
        (
            "validate_storage_lifecycle_conformance.py",
            "validate_page_address_compatibility_corpus.py",
            "validate_raft_storage_conformance_evidence.py",
        ),
    ),
    Phase(
        7,
        "public config agreement",
        ("validate_storage_tuning_conformance.py",),
    ),
    Phase(
        8,
        "metrics/report agreement",
        (
            "validate_page_block_metrics_conformance.py",
            "validate_grafana_metrics_conformance.py",
            "test_storage_conformance_report_artifacts.py",
            "validate_storage_conformance_report_artifacts.py",
            "test_temporalstore_performance_artifact_audit.py",
            "test_temporalstore_next_performance_workflow.py",
            "test_temporalstore_next_performance_plan_validator.py",
            "validate_temporalstore_next_performance_plan.py",
            "test_temporalstore_performance_execution_redaction.py",
            "validate_temporalstore_performance_execution_redaction.py",
            "test_temporalstore_performance_evidence_import.py",
            "validate_temporalstore_rust_performance_parity.py",
        ),
    ),
    Phase(
        9,
        "shared storage/proxy/Raft evidence",
        (
            "validate_temporalstore_rust_feature_execution.py",
            "validate_storage_proxy_client_conformance_coverage.py",
            "validate_storage_unified_case_report_pair.py",
            "validate_raft_storage_conformance_evidence.py",
            "validate_page_address_compatibility_corpus.py",
            "validate_temporalstore_rust_goal_parity.py",
        ),
    ),
)


def _coverage_state_note() -> str:
    """What the case corpus says about its own wiring, quoted rather than paraphrased.

    Without this the summary above reads as decay -- eight validators absent, eight failing, as
    though something rotted. It is the opposite: these gates were written ahead of the runner they
    check against, and `compat/unified_temporalstore_cases.json` records that per family, in a
    `status` of `temporary_static_surface_gate` and a `blocker` naming the missing piece.

    Printed from the corpus rather than restated here, so it cannot drift out of step with what the
    corpus actually claims.
    """
    corpus = ROOT / "compat" / "unified_temporalstore_cases.json"
    if not corpus.exists():
        return ""
    try:
        rows = json.loads(corpus.read_text(encoding="utf-8"))["coverage"]["native_adapter_coverage"]
    except (KeyError, ValueError, OSError):
        return ""
    counts: dict[str, int] = {}
    blocker = ""
    for row in rows:
        if not isinstance(row, dict):
            continue
        counts[str(row.get("status"))] = counts.get(str(row.get("status")), 0) + 1
        if not blocker and row.get("blocker"):
            blocker = str(row["blocker"])
    if not counts:
        return ""
    tally = ", ".join(f"{name}={count}" for name, count in sorted(counts.items()))
    lines = [
        "",
        "coverage state, from compat/unified_temporalstore_cases.json:",
        f"  families by status: {tally}",
    ]
    if blocker:
        lines.append(f"  blocker: {blocker}")
    lines.append(
        "  These gates are ahead of the runner they check against, not decayed. An absent "
        "validator here is work not yet wired, which the corpus records per family."
    )
    return "\n".join(lines)


def run_validator(script_name: str) -> str:
    """Run one validator and say what happened: "ok", "absent" or "failed".

    This used to raise on the first problem, which made the gate report whichever validator it
    reached first and nothing about the rest. Three of the validators it names have never existed
    in this repository, so the first phase raised FileNotFoundError and the other eight phases were
    never even attempted -- the output looked like one broken file rather than an umbrella gate
    that cannot complete here.

    Absent and failed are kept apart on purpose. A failing validator is a conformance result. An
    absent one is a statement about which files this repository contains, and the two want
    different decisions.
    """
    script = TOOLS / script_name
    if not script.exists():
        print(f"    ABSENT: {script_name} is not in this repository")
        return "absent"
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
        print(f"    FAILED: {script_name} exited {completed.returncode}")
        return "failed"
    return "ok"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the nine conformance storage-engine phases."
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
    absent: list[str] = []
    failed: list[str] = []
    for loop in range(1, args.loops + 1):
        print(f"storage_engine_loop={loop}/{args.loops}")
        for phase in PHASES:
            print(f"phase_{phase.number}: {phase.name}")
            for validator in phase.validators:
                print(f"  validator={validator}")
                outcome = run_validator(validator)
                total_validator_runs += 1
                if outcome == "absent" and validator not in absent:
                    absent.append(validator)
                elif outcome == "failed" and validator not in failed:
                    failed.append(validator)

    print(
        "storage_engine_9_phase_summary "
        f"loops={args.loops} phases={len(PHASES)} "
        f"validator_runs={total_validator_runs} "
        f"absent={len(absent)} failed={len(failed)}"
    )
    for name in absent:
        print(f"  absent: {name}")
    for name in failed:
        print(f"  failed: {name}")
    note = _coverage_state_note()
    if note:
        print(note)
    if absent or failed:
        # Not "passed". An umbrella gate that reports success while naming validators it could not
        # run is worse than one that says plainly it could not complete.
        return 1
    print("storage_engine_9_phase_passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
