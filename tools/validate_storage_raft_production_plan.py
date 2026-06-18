#!/usr/bin/env python3
"""Validate storage/Raft production-readiness gate wiring."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


REQUIRED_SCRIPT_SNIPPETS = (
    "storage_fault_matrix_harness",
    "storage_production_harness",
    "storage_modes_harness",
    "--features openraft-engine openraft_ --lib",
    "readiness_gate -- --service raft_replication",
    "distributed_raft_harness",
    "metaserver_raft_harness",
    "raft_secondary_replication_harness",
    "build_raft_distributed_parity_summary.py",
    "raft-distributed-parity-validation",
    "external_chaos_gate",
    "validate_raft_storage_parity_evidence.py",
)

REQUIRED_RAFT_PARITY_SCRIPT_SNIPPETS = (
    "distributed_raft_harness",
    "raft_secondary_replication_harness",
    "metaserver_raft_harness",
    "build_raft_distributed_parity_summary.py",
    "raft-distributed-parity-validation",
)

REQUIRED_READINESS_SNIPPETS = (
    "local storage production harness combines dump, cache pressure, restart recovery, shared-store replay, and Raft movement",
    "local storage dump/load fault matrix harness rejects checksum mismatch, partial manifests, missing segments, stale manifests, restart-during-install recovery, and corrupt page segments",
    "OpenRaft-backed data-node and metaserver adapter is available behind the openraft-engine feature",
    "ProductionRaftEngineKind::OpenRaft",
    "Raft OpenRaft rollout readiness covers adapter presence, data-node/metaserver startup selection, and durable local log state while keeping real multi-process data-node/metaserver log-store rollout fail-closed",
    "RaftStorageApplyFence persists shard, term, committed/applied index, snapshot id, storage epoch, and checksum",
    "Raft atomic apply readiness covers storage apply fence persistence, WAL fence recovery validation, storage mutation atomic commit, snapshot-install atomic commit, and snapshot lifecycle reporting while keeping real multi-process data-node OpenRaft rollout validation fail-closed",
    "RaftSnapshotInstallReport exposes freeze, flush, manifest verify, checksum verify, install, tail replay, and rollback status",
    "metaserver-owned data-Raft membership workflow reports learner add, catch-up verification, promotion, leader transfer, and voter removal",
    "Raft metaserver membership readiness covers topology membership plans, data-Raft apply reports, learner catch-up/promotion, leader transfer, and voter removal while keeping networked scheduler transport and persisted real-group execution fail-closed",
    "Raft transport security readiness covers auth-token validation, mTLS cert/key/CA config validation, authenticated HTTP transport, and plaintext-only local chaos guardrails while keeping service-process mTLS enforcement fail-closed",
    "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, and storage replay gates while keeping external packet-loss/disk-pressure/process-chaos fail-closed",
    "storage migration corpus readiness covers Rust-local converted corpus replay through engine, shared-store, Raft read paths, and the unified C++/Rust runner while keeping external C++ artifact publication fail-closed",
    "local/shared-store object manifest dependency matrix covers local file objects, checkpoint manifests, oplog cursor retention, page segment manifests, and follower-cursor retention",
    "storage cache dependency matrix keeps live ByteStore/S3 object-store readiness fail-closed",
    "storage SSD cache pressure readiness covers local memory read-through, disk block cache, admission/eviction counters, slot warmup, cache invalidation, and tiny-cache pressure harness evidence",
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
        ROOT / "tools" / "run_raft_distributed_parity.sh",
        REQUIRED_RAFT_PARITY_SCRIPT_SNIPPETS,
        "raft_distributed_parity_script",
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
