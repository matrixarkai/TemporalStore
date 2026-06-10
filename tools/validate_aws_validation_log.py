#!/usr/bin/env python3
import argparse
import json


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def load_summary(path):
    text = open(path, "r", encoding="utf-8").read()
    start = text.find("{")
    end = text.rfind("}")
    require(start >= 0 and end > start, f"{path}: no JSON summary found")
    return json.loads(text[start : end + 1])


def validate_raft(job, summary):
    for field in [
        "proposal_status",
        "post_transfer_write",
        "post_scale_down_write",
        "post_scale_up_write",
        "external_snapshot_publish",
        "external_snapshot_bootstrap",
    ]:
        require(summary[field]["ok"], f"{job}: {field} failed: {summary[field]}")
    require(summary["follower_write_rejection"]["ok"] is False, f"{job}: follower write was not rejected")
    require(
        summary["external_snapshot_read"]["value"] == "from-external-snapshot",
        f"{job}: external snapshot read mismatch",
    )
    for read in summary["replica_reads"]:
        require(read["value"] == "replicated-value", f"{job}: replica read mismatch: {read}")
    for read in summary["scale_down_reads"]:
        require(read["value"] == "after-scale-down", f"{job}: scale-down read mismatch: {read}")
    for read in summary["scale_up_reads"]:
        require(read["value"] == "after-scale-up", f"{job}: scale-up read mismatch: {read}")
    for node in summary["nodes"]:
        require(node["status"]["has_majority"], f"{job}: node {node['node_id']} has no majority")
        require(node["apply_health"]["healthy"], f"{job}: node {node['node_id']} apply health unhealthy")


def validate_scale(job, summary):
    require(summary["replication_healthy"], f"{job}: replication health is false")
    require(summary["max_replica_lag"] == 0, f"{job}: max replica lag is {summary['max_replica_lag']}")
    require(summary["write_ops_per_sec"] > 0, f"{job}: write_ops_per_sec is not positive")
    require(summary["raft_replica_read_latency"]["samples"] > 0, f"{job}: no raft replica read samples")
    require(summary["shared_store"] is not None, f"{job}: shared-store comparison missing")
    require(summary["shared_store"]["sync_max_lag"] == 0, f"{job}: sync shared-store lag is non-zero")


def validate_storage(job, summary):
    require(summary["shared_store_sync"]["read_value"] == "sync-value", f"{job}: sync shared-store read mismatch")
    require(summary["shared_store_async"]["read_value"] == "async-value", f"{job}: async shared-store read mismatch")
    raft = summary["raft_local_file"]
    require(raft["read_value_after_restore"] == "wal-value", f"{job}: raft local-file restore read mismatch")
    require(
        raft["commit_index_after_restore"] >= raft["commit_index_before_restore"],
        f"{job}: raft restore regressed commit index",
    )
    require(len(raft["wal_files"]) > 0, f"{job}: no raft WAL files found")


def main():
    parser = argparse.ArgumentParser(description="Validate TemporalStore AWS validation job JSON logs")
    parser.add_argument("--job", required=True)
    parser.add_argument("--log", required=True)
    args = parser.parse_args()

    summary = load_summary(args.log)
    if args.job.endswith("raft-validation"):
        validate_raft(args.job, summary)
    elif args.job.endswith("scale-validation"):
        validate_scale(args.job, summary)
    elif args.job.endswith("storage-validation"):
        validate_storage(args.job, summary)
    else:
        raise SystemExit(f"{args.job}: unknown validation job")
    print(f"{args.job}: JSON validation passed")


if __name__ == "__main__":
    main()
