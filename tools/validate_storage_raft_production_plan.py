#!/usr/bin/env python3
"""Validate storage/Raft production-readiness gate wiring."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


REQUIRED_SCRIPT_SNIPPETS = (
    "storage_fault_matrix_harness",
    "storage_production_harness",
    "storage_modes_harness",
    "readiness_gate -- --service raft_replication",
    "distributed_raft_harness",
    "raft_secondary_replication_harness",
    "external_chaos_gate",
    "validate_raft_storage_parity_evidence.py",
)

REQUIRED_READINESS_SNIPPETS = (
    "local storage production harness combines dump, cache pressure, restart recovery, shared-store replay, and Raft movement",
    "local storage dump/load fault matrix harness rejects checksum mismatch, partial manifests, missing segments, stale manifests, and corrupt page segments",
    "real OpenRaft or raft-rs data-node FSM/storage implementation",
    "networked metaserver Raft transport and scheduler loop",
)

REQUIRED_DOC_SNIPPETS = (
    "Storage recovery/fault matrix hardening",
    "Slot dump/load atomicity and manifest rejection",
    "Follower-safe GC and cache pressure",
    "Real Raft FSM/storage selection and integration",
    "Raft snapshot/restart/failover harness",
    "Combined storage+Raft production harness",
    "Update unified C++/Rust corpus and readiness docs",
)


def require_snippets(path: Path, snippets: tuple[str, ...], label: str) -> int:
    if not path.exists():
        raise SystemExit(f"{label}: missing {path.relative_to(ROOT)}")
    text = path.read_text(encoding="utf-8", errors="ignore")
    missing = [snippet for snippet in snippets if snippet not in text]
    if missing:
        raise SystemExit(
            f"{label}: {path.relative_to(ROOT)} missing snippets: {', '.join(missing)}"
        )
    return len(snippets)


def main() -> None:
    count = 0
    count += require_snippets(
        ROOT / "tools" / "run_storage_raft_production_readiness.sh",
        REQUIRED_SCRIPT_SNIPPETS,
        "storage_raft_script",
    )
    count += require_snippets(
        ROOT / "crates" / "temporalstore-rust" / "src" / "readiness.rs",
        REQUIRED_READINESS_SNIPPETS,
        "readiness",
    )
    count += require_snippets(
        ROOT / "docs" / "storage_raft_production_readiness_plan.md",
        REQUIRED_DOC_SNIPPETS,
        "storage_raft_doc",
    )
    print("storage_raft_production_plan=true")
    print(f"validated_snippets={count}")


if __name__ == "__main__":
    main()
