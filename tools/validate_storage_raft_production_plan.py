#!/usr/bin/env python3
"""Validate storage/Raft production-readiness gate wiring."""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


REQUIRED_SCRIPT_SNIPPETS = (
    "storage_fault_matrix_harness",
    "storage_production_harness",
    "storage_modes_harness",
    "export_cpp_storage_migration_artifacts.py",
    "storage-migration-artifacts",
    "--features openraft-engine openraft_ --lib",
    "readiness_gate -- --service raft_replication",
    "distributed_raft_harness",
    "metaserver_raft_harness",
    "raft_secondary_replication_harness",
    "build_raft_distributed_parity_summary.py",
    "raft-distributed-parity-validation",
    "run_cpp_raft_cases_on_rust.py",
    "cpp-raft-cases-on-rust.json",
    "build_storage_raft_production_proof.py",
    "storage-raft-production-proof.json",
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
    "Raft OpenRaft rollout readiness covers adapter presence, data-node/metaserver startup selection, durable local log state, and real multi-process data-node/metaserver log-store rollout evidence",
    "RaftStorageApplyFence persists shard, term, committed/applied index, snapshot id, storage epoch, and checksum",
    "Raft atomic apply readiness covers storage apply fence persistence, WAL fence recovery validation, production runtime data-node atomic durability reports, storage mutation atomic commit, snapshot-install atomic commit, and snapshot lifecycle reporting",
    "RaftSnapshotInstallReport exposes freeze, flush, manifest verify, checksum verify, install, tail replay, and rollback status",
    "metaserver-owned data-Raft membership workflow reports learner add, catch-up verification, promotion, leader transfer, and voter removal",
    "Raft metaserver membership readiness covers topology membership plans, data-Raft apply reports, learner catch-up/promotion, leader transfer, voter removal, networked scheduler /raft/membership/apply transport, persisted scheduler task state, and real data-node group execution under follower lag, failover, scale up/down, and secondary replication",
    "Raft transport security readiness covers auth-token validation, mTLS cert/key/CA config validation, service-process mTLS runtime selection, authenticated HTTP transport, and plaintext-only local chaos guardrails",
    "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, storage replay, external packet-loss, disk-pressure, and process-chaos gates",
    "storage migration corpus readiness covers Rust-local converted corpus replay through engine restart, Redis/admin, shared-store sync/async replay, cache warmup, Raft read paths, external C++ binary-artifact export, CI-published golden artifacts, and the unified C++/Rust runner",
    "local/shared-store object manifest dependency matrix covers local file objects, checkpoint manifests, oplog cursor retention, page segment manifests, follower-cursor retention, and Raft snapshot manifest retention",
    "storage cache dependency matrix keeps live external ByteStore/S3 object-store integration explicitly out of scope while local/shared-store is the production target",
    "storage SSD cache pressure readiness covers local memory read-through, disk block cache, admission/eviction counters, slot warmup, cache invalidation, tiering policy, admission tuning, and long-running pressure validation evidence",
    "Docker/AWS SLO report covers metaserver, proxy, client, data-node, Raft failover, storage pressure, cache pressure, proxy convergence, workload replay, p50/p95/p99, throughput, error budget, CPU/memory/disk/network collectors, replica lag, failover count, and scale events",
    "under follower lag, failover, scale up/down, and secondary replication",
)

REQUIRED_DOC_SNIPPETS = (
    "Storage recovery/fault matrix hardening",
    "Slot dump/load atomicity and manifest rejection",
    "Follower-safe GC and cache pressure",
    "Real Raft FSM/storage selection and integration",
    "Raft snapshot/restart/failover harness",
    "Combined storage+Raft production harness",
    "storage-raft-production-proof.json",
    "cpp-raft-cases-on-rust.json",
    "C++ Raft scenario comparison",
    "Update unified C++/Rust corpus and readiness docs",
    "scale_slo_report.storage_deployment_scale_slo_ready",
)

REQUIRED_EXTERNAL_CHAOS_SNIPPETS = (
    "external_packet_loss_partition_heal",
    "external_disk_pressure_storage_faults",
    "external_process_chaos_restart_failover",
)

REQUIRED_SCALE_SLO_SNIPPETS = (
    "slo_report",
    "ScaleSloReport",
    "storage_deployment_scale_slo_ready",
    "docker_or_aws_slo_evidence_ready",
    "p50_write_us",
    "p95_write_us",
    "p99_write_us",
    "error_budget_remaining_percent",
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
    count += require_snippets(
        ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "external_chaos_gate.rs",
        REQUIRED_EXTERNAL_CHAOS_SNIPPETS,
        "external_chaos_gate",
    )
    count += require_snippets(
        ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "scale_harness.rs",
        REQUIRED_SCALE_SLO_SNIPPETS,
        "scale_harness_slo",
    )
    print("storage_raft_production_plan=true")
    print(f"validated_snippets={count}")


if __name__ == "__main__":
    main()
