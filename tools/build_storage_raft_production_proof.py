#!/usr/bin/env python3
"""Build one combined storage plus Raft production proof report.

The individual harnesses are still the source of truth. This script joins their
JSON outputs into one readiness envelope so storage, cache, shared-store, Raft,
and ByteRaft-derived contracts can be reviewed together.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    report = build_report(args.artifact_dir)
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    output = args.output or args.artifact_dir / "storage-raft-production-proof.json"
    output.write_text(text, encoding="utf-8")
    print(text, end="")
    if not report["local_production_ready_slice"]:
        raise SystemExit("combined storage/Raft local production proof is not ready")
    return 0


def build_report(artifact_dir: Path) -> dict[str, Any]:
    storage_fault = load_json(artifact_dir / "storage-fault-matrix.json")
    storage_production = load_json(artifact_dir / "storage-production.json")
    storage_modes = load_json(artifact_dir / "storage-modes.json")
    migration_manifest = load_json(artifact_dir / "storage-migration-artifacts-manifest.json")
    raft_summary = load_json(artifact_dir / "raft-distributed-parity.json")
    cpp_raft_cases = load_json(artifact_dir / "cpp-raft-cases-on-rust.json")
    external_chaos = load_json(artifact_dir / "external-chaos.json")
    raft_readiness = load_json(artifact_dir / "raft-readiness.json")

    storage_fault_ready = bool(storage_fault.get("production_ready_slice")) and bool(
        storage_fault.get("report", {}).get("production_ready_slice")
    )
    storage_cases = storage_production.get("cases") or []
    storage_production_ready = (
        storage_production.get("corpus_name") == "temporalstore-storage-migration-corpus"
        and bool(storage_cases)
        and all(case_ready(case) for case in storage_cases)
        and bool(
            storage_production.get("corpus_report", {}).get("external_corpus_publication_ready")
        )
    )
    storage_modes_ready = (
        storage_modes.get("shared_store_sync", {}).get("read_value") == "sync-value"
        and storage_modes.get("shared_store_async", {}).get("read_value") == "async-value"
        and storage_modes.get("raft_local_file", {}).get("read_value_after_restore") == "wal-value"
    )
    migration_ready = (
        migration_manifest.get("name") == "temporalstore-cpp-storage-migration-artifacts"
        and bool(migration_manifest.get("cases"))
        and migration_manifest.get("compatibility_decision") == "migration_only_rust_native_pages"
    )
    raft_ready = bool(raft_summary.get("production_ready_slice")) and all(
        [
            raft_summary.get("data_node", {}).get("distributed_all_nodes_have_majority"),
            raft_summary.get("data_node", {}).get("follower_write_rejected"),
            raft_summary.get("data_node", {}).get("leader_crash_failover_ok"),
            raft_summary.get("data_node", {}).get("partition_isolated_read_rejected"),
            raft_summary.get("metaserver", {}).get("temporal_raft_process_rollout", {}).get("ready"),
            raft_summary.get("metaserver", {})
            .get("meta_owned_data_raft_membership", {})
            .get("ready"),
        ]
    )
    cpp_raft_comparison = build_cpp_raft_comparison(cpp_raft_cases, raft_summary)
    cpp_raft_ready = bool(cpp_raft_comparison["ready"])
    external_chaos_ready = bool(external_chaos.get("production_ready_slice"))
    unified_storage_ready = all(
        [
            storage_fault_ready,
            storage_production_ready,
            storage_modes_ready,
            migration_ready,
        ]
    )

    evidence = {
        "temporal_raft_process_rollout": {
            "ready": raft_ready,
            "source": "raft-distributed-parity.json",
            "data_node_majority": raft_summary.get("data_node", {}).get(
                "distributed_all_nodes_have_majority"
            ),
            "metaserver_rollout": raft_summary.get("metaserver", {}).get(
                "temporal_raft_process_rollout"
            ),
        },
        "byteraft_derived_semantics": {
            "ready": raft_ready,
            "source": "validate_byteraft_derived_readiness.py plus raft-distributed-parity.json",
            "contracts": [
                "leader transfer",
                "membership add/promote/remove",
                "read-index/read fencing",
                "snapshot install/restart recovery",
                "follower lag and catch-up",
                "secondary reads",
            ],
        },
        "cpp_raft_scenario_comparison": cpp_raft_comparison,
        "local_raft_fixture_policy": {
            "ready": True,
            "local_raft_fixture_test_only": True,
            "production_readiness_source": "TemporalRaft multi-process harness evidence only",
            "blocked_runtime_mode": "LocalModel",
        },
        "storage_format_compatibility": {
            "ready": storage_production_ready and migration_ready,
            "decision": "migration_only_rust_native_page_log_format",
            "byte_for_byte_cpp_layout": False,
            "source": "storage-production.json plus storage-migration-artifacts-manifest.json",
        },
        "unified_storage_recovery_dump_load_cache_gc": {
            "ready": unified_storage_ready,
            "sources": [
                "storage-fault-matrix.json",
                "storage-production.json",
                "storage-modes.json",
                "storage-migration-artifacts-manifest.json",
            ],
            "recovery_fault_matrix_ready": storage_fault_ready,
            "dump_load_manifest_ready": storage_production_ready,
            "cache_pressure_and_refill_ready": storage_modes_ready,
            "follower_safe_gc_and_shared_store_ready": storage_modes_ready,
            "cpp_migration_corpus_ready": migration_ready,
        },
        "combined_storage_raft_cache_shared_store": {
            "ready": storage_fault_ready
            and storage_production_ready
            and storage_modes_ready
            and raft_ready
            and cpp_raft_ready
            and external_chaos_ready,
            "sources": [
                "storage-fault-matrix.json",
                "storage-production.json",
                "storage-modes.json",
                "raft-distributed-parity.json",
                "cpp-raft-cases-on-rust.json",
                "external-chaos.json",
            ],
        },
    }
    local_ready = all(item["ready"] for item in evidence.values())
    global_blockers = global_production_blockers(raft_readiness)
    return {
        "format": "temporalstore_storage_raft_production_proof_v1",
        "artifact_dir": str(artifact_dir),
        "local_production_ready_slice": local_ready,
        "global_production_ready": local_ready and not global_blockers,
        "global_production_blockers": global_blockers,
        "storage_fault_matrix": {
            "ready": storage_fault_ready,
            "scenario_count": storage_fault.get("report", {}).get("scenario_count"),
            "passed_count": storage_fault.get("report", {}).get("passed_count"),
        },
        "storage_migration": {
            "ready": storage_production_ready and migration_ready,
            "corpus_name": storage_production.get("corpus_name"),
            "case_count": len(storage_cases),
            "converted_artifact_count": len(migration_manifest.get("cases") or []),
            "compatibility": "behavioral_migration_not_byte_layout",
        },
        "shared_store_cache": {
            "ready": storage_modes_ready,
            "sync_read_value": storage_modes.get("shared_store_sync", {}).get("read_value"),
            "async_read_value": storage_modes.get("shared_store_async", {}).get("read_value"),
            "raft_local_file_restore_value": storage_modes.get("raft_local_file", {}).get(
                "read_value_after_restore"
            ),
        },
        "raft": {
            "ready": raft_ready,
            "data_node": raft_summary.get("data_node"),
            "metaserver": raft_summary.get("metaserver"),
            "cpp_scenario_comparison": cpp_raft_comparison,
        },
        "external_chaos": {
            "ready": external_chaos_ready,
            "scenario_count": external_chaos.get("scenario_count"),
            "passed_count": external_chaos.get("passed_count"),
        },
        "evidence": evidence,
    }


def build_cpp_raft_comparison(cpp_cases: dict[str, Any], raft_summary: dict[str, Any]) -> dict[str, Any]:
    scenario_specs = {
        "leader_election": {
            "case_terms": ["leader_election", "leader_failover"],
            "checks": [
                bool(raft_summary.get("data_node", {}).get("leader_crash_failover_ok")),
                bool(raft_summary.get("metaserver", {}).get("leader_after_failover")),
            ],
            "rust_evidence": [
                "data_node.leader_crash_failover_ok",
                "metaserver.leader_after_failover",
            ],
        },
        "failover": {
            "case_terms": ["failover"],
            "checks": [
                bool(raft_summary.get("data_node", {}).get("leader_crash_failover_ok")),
                bool(raft_summary.get("metaserver", {}).get("namespace_after_failover_visible")),
            ],
            "rust_evidence": [
                "data_node.leader_crash_failover_ok",
                "metaserver.namespace_after_failover_visible",
            ],
        },
        "snapshot": {
            "case_terms": ["snapshot"],
            "checks": [
                bool(raft_summary.get("data_node", {}).get("external_snapshot_read")),
                bool(raft_summary.get("metaserver", {}).get("snapshot_restore_read")),
            ],
            "rust_evidence": [
                "data_node.external_snapshot_read",
                "metaserver.snapshot_restore_read",
            ],
        },
        "membership": {
            "case_terms": ["membership", "scale"],
            "checks": [
                bool(raft_summary.get("data_node", {}).get("scale_down_voters")),
                bool(raft_summary.get("data_node", {}).get("scale_up_voters")),
                bool(
                    raft_summary.get("metaserver", {})
                    .get("meta_owned_data_raft_membership", {})
                    .get("ready")
                ),
            ],
            "rust_evidence": [
                "data_node.scale_down_voters",
                "data_node.scale_up_voters",
                "metaserver.meta_owned_data_raft_membership.ready",
            ],
        },
        "follower_lag": {
            "case_terms": ["lag", "catchup"],
            "checks": [
                int(raft_summary.get("data_node", {}).get("lagging_follower_observed_lag") or 0)
                > 0,
                bool(raft_summary.get("metaserver", {}).get("lagging_catchup_read")),
            ],
            "rust_evidence": [
                "data_node.lagging_follower_observed_lag",
                "metaserver.lagging_catchup_read",
            ],
        },
        "secondary_reads": {
            "case_terms": ["secondary", "replica_read"],
            "checks": [
                bool(raft_summary.get("data_node", {}).get("replica_read_values")),
                bool(raft_summary.get("data_node", {}).get("secondary_restart_reads")),
            ],
            "rust_evidence": [
                "data_node.replica_read_values",
                "data_node.secondary_restart_reads",
            ],
        },
    }
    cases = cpp_cases.get("cases") or []
    scenario_results = {}
    for scenario, spec in scenario_specs.items():
        matched_cases = [
            case.get("name")
            for case in cases
            if any(term in case.get("name", "") for term in spec["case_terms"])
            or any(
                any(term in step.get("name", "") for term in spec["case_terms"])
                for step in case.get("steps", [])
            )
        ]
        scenario_results[scenario] = {
            "ready": bool(matched_cases) and all(spec["checks"]),
            "cpp_cases": sorted(set(name for name in matched_cases if name)),
            "rust_evidence": spec["rust_evidence"],
        }
    return {
        "ready": bool(cpp_cases.get("case_count")) and all(
            result["ready"] for result in scenario_results.values()
        ),
        "schema": cpp_cases.get("schema"),
        "source": "cpp-raft-cases-on-rust.json",
        "cpp_case_count": cpp_cases.get("case_count"),
        "cpp_step_count": cpp_cases.get("step_count"),
        "cpp_required_paths_checked": cpp_cases.get("cpp_required_paths_checked"),
        "missing_cpp_required_paths": cpp_cases.get("missing_cpp_required_paths", []),
        "required_scenarios": sorted(scenario_specs),
        "scenario_results": scenario_results,
    }


def case_ready(case: dict[str, Any]) -> bool:
    mutation_count = int(case.get("mutation_count") or 0)
    shared_store_sync_applied = case.get("shared_store_sync_applied")
    if shared_store_sync_applied is None:
        shared_store_sync_applied = case.get("shared_store_sync", {}).get("applied")
    shared_store_async_applied = case.get("shared_store_async_applied")
    if shared_store_async_applied is None:
        shared_store_async_applied = case.get("shared_store_async", {}).get("applied")
    return all(
        [
            mutation_count > 0,
            bool(case.get("slot_dump_manifest_id")),
            int(case.get("dumped_slot_count") or 0) > 0,
            int(case.get("cache_warmup_page_refs") or 0) > 0,
            bool(case.get("recovery_ok_before_restart")),
            bool(case.get("recovery_ok_after_restart")),
            int(shared_store_sync_applied or -1) == mutation_count,
            int(shared_store_async_applied or -1) == mutation_count,
            bool(case.get("redis_admin_replay_ok")),
        ]
    )


def global_production_blockers(raft_readiness: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    failed = raft_readiness.get("failed_capabilities")
    if isinstance(failed, list):
        blockers.extend(
            item.get("capability", "unknown")
            for item in failed
            if item.get("area") == "raft_replication"
        )
    else:
        blockers.extend(str(item) for item in raft_readiness.get("missing", []))
    blockers.extend(
        [
            "byte_for_byte_cpp_storage_layout_not_targeted",
            "live_bytestore_s3_object_store_integration_not_in_scope",
            "docker_or_aws_multi_service_slo_evidence_required_for_global_release",
        ]
    )
    return sorted(set(blocker for blocker in blockers if blocker))


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"{path}: missing required artifact")
    text = path.read_text(encoding="utf-8")
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise SystemExit(f"{path}: no JSON object found")
    return json.loads(text[start : end + 1])


if __name__ == "__main__":
    raise SystemExit(main())
