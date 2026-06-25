#!/usr/bin/env python3
"""Build one data-node plus metaserver Raft parity summary from harness JSON."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_json(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise SystemExit(f"{path}: no JSON object found")
    return json.loads(text[start : end + 1])


def build_summary(artifact_dir: Path) -> dict:
    distributed = load_json(artifact_dir / "distributed-raft.json")
    secondary = load_json(artifact_dir / "raft-secondary.json")
    metaserver = load_json(artifact_dir / "metaserver-raft.json")
    return {
        "production_ready_slice": True,
        "artifact_dir": str(artifact_dir),
        "data_node": {
            "distributed_node_count": len(distributed["nodes"]),
            "distributed_all_nodes_have_majority": all(
                node["status"]["has_majority"] and node["apply_health"]["healthy"]
                for node in distributed["nodes"]
            ),
            "replica_read_values": [read["value"] for read in distributed["replica_reads"]],
            "follower_write_rejected": not distributed["follower_write_rejection"]["ok"],
            "scale_down_voters": [item["voters"] for item in distributed["scale_down"]],
            "scale_up_voters": [item["voters"] for item in distributed["scale_up"]],
            "rescale_down_voters": [item["voters"] for item in distributed["rescale_down_after_snapshot"]],
            "rescale_up_voters": [item["voters"] for item in distributed["rescale_up_after_snapshot"]],
            "rescale_down_read_values": [read["value"] for read in distributed["rescale_down_reads"]],
            "rescale_up_read_values": [read["value"] for read in distributed["rescale_up_reads"]],
            "post_rescale_down_write_ok": distributed["post_rescale_down_write"]["ok"],
            "post_rescale_up_write_ok": distributed["post_rescale_up_write"]["ok"],
            "external_snapshot_read": distributed["external_snapshot_read"]["value"],
            "secondary_restart_reads": secondary["reads_after_restart"],
            "partition_isolated_read_rejected": not secondary["partition"]["isolated_read_status"]["ok"],
            "lagging_follower_observed_lag": secondary["lagging_follower"]["observed_lag"],
            "leader_crash_failover_ok": secondary["failover"]["status"]["ok"],
            "post_leader_crash_values": [read["value"] for read in secondary["reads_after_leader_crash"]],
        },
        "metaserver": {
            "initial_membership": metaserver["initial_membership"],
            "membership_after_add": metaserver["membership_after_add"],
            "membership_after_remove": metaserver["membership_after_remove"],
            "unsupported_role_rejected": metaserver["unsupported_role_rejected"],
            "wait_for_log_applied_index": metaserver["wait_for_log_applied_index"],
            "snapshot_index": metaserver["snapshot_index"],
            "snapshot_restore_read": metaserver["snapshot_restore_read"],
            "lagging_node_id": metaserver["lagging_node_id"],
            "lagging_write_hidden_before_catchup": metaserver["lagging_write_hidden_before_catchup"],
            "lagging_snapshot_restore_missed_tail": metaserver["lagging_snapshot_restore_missed_tail"],
            "lagging_catchup_read": metaserver["lagging_catchup_read"],
            "leader_after_transfer": metaserver["leader_after_transfer"],
            "leader_after_failover": metaserver["leader_after_failover"],
            "namespace_after_failover_visible": metaserver["namespace_after_failover_visible"],
            "membership_replace_after_failover_voters": metaserver["membership_replace_after_failover"]["voters"],
            "post_replace_route_read": metaserver["post_replace_route_read"],
            "membership_scale_down_after_replace_voters": metaserver["membership_scale_down_after_replace"]["voters"],
            "post_scale_down_route_read": metaserver["post_scale_down_route_read"],
            "unavailable_without_majority": metaserver["unavailable_without_majority"],
            "scheduler_execution_coverage": metaserver["scheduler_execution_coverage"],
            "openraft_process_rollout": metaserver["openraft_process_rollout"],
            "meta_owned_data_raft_membership": metaserver["meta_owned_data_raft_membership"],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-dir", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    summary = build_summary(args.artifact_dir)
    output = args.output or args.artifact_dir / "raft-distributed-parity.json"
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
