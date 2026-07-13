#!/usr/bin/env python3
"""Validate metaserver Raft and root/meta management readiness wiring.

This is a static/evidence-shape gate. It does not claim the live cluster is production-ready by
itself; it fails if the repo stops requiring the root/meta-server responsibilities that production
TemporalStore needs.
"""

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


REQUIRED_COMMON_VALIDATED_FIELDS = (
    "initial_membership",
    "membership_after_add",
    "membership_after_remove",
    "unsupported_role_rejected",
    "wait_for_log_applied_index",
    "snapshot_index",
    "snapshot_restore_read",
    "lagging_write_hidden_before_catchup",
    "lagging_snapshot_restore_missed_tail",
    "lagging_catchup_read",
    "leader_after_transfer",
    "leader_after_failover",
    "namespace_after_failover_visible",
    "membership_replace_after_failover",
    "membership_scale_down_after_replace",
    "post_replace_route_read",
    "post_scale_down_route_read",
    "unavailable_without_majority",
    "scheduler_execution_coverage",
    "temporal_raft_process_rollout",
    "meta_owned_data_raft_membership",
)

REQUIRED_HARNESS_ONLY_FIELDS = (
    "data_node_process_rollout",
    "final_process_path_readiness",
)

REQUIRED_SCHEDULER_FIELDS = (
    "networked_multi_process_raft_ready",
    "missing_primary_repair_ready",
    "under_replicated_repair_ready",
    "stale_dead_server_repair_ready",
    "load_reload_unload_ready",
    "post_failover_replacement_ready",
    "post_failover_scale_down_ready",
    "post_replacement_route_read_ready",
    "post_scale_down_route_read_ready",
    "cooldown_and_safe_mode_ready",
    "scheduler_task_replay_ready",
    "scheduler_generation_token_ready",
    "membership_change_ready",
    "durable_data_raft_membership_ready",
    "stale_scheduler_token_rejection_ready",
)

REQUIRED_PARITY_SUMMARY_FIELDS = (
    "namespace_after_failover_visible",
    "post_replace_route_read",
    "post_scale_down_route_read",
    "unavailable_without_majority",
    "scheduler_execution_coverage",
    "temporal_raft_process_rollout",
    "meta_owned_data_raft_membership",
)

REQUIRED_DOC_SNIPPETS = (
    "Namespace/table lifecycle is Raft-committed",
    "Slot and shard placement are assigned through metaserver state",
    "Primary placement and replacement are explicit scheduler decisions",
    "Topology readiness waits for slot/primary assignment",
    "Server heartbeat/liveness feeds scheduler repair decisions",
    "No-majority writes fail closed",
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
        ROOT / "crates" / "temporalstore-rust" / "src" / "bin" / "metaserver_raft_harness.rs",
        REQUIRED_COMMON_VALIDATED_FIELDS + REQUIRED_HARNESS_ONLY_FIELDS + REQUIRED_SCHEDULER_FIELDS,
        "metaserver_raft_harness",
    )
    count += require_snippets(
        ROOT / "tools" / "build_raft_distributed_parity_summary.py",
        REQUIRED_PARITY_SUMMARY_FIELDS,
        "raft_distributed_parity_summary",
    )
    count += require_snippets(
        ROOT / "tools" / "validate_aws_validation_log.py",
        REQUIRED_COMMON_VALIDATED_FIELDS + REQUIRED_SCHEDULER_FIELDS,
        "aws_validation_log",
    )
    count += require_snippets(
        ROOT / "docs" / "metaserver_production_meta_management.md",
        REQUIRED_DOC_SNIPPETS,
        "metaserver_meta_management_doc",
    )
    print("metaserver_production_meta_management_gate=true")
    print(f"validated_snippets={count}")


if __name__ == "__main__":
    main()
