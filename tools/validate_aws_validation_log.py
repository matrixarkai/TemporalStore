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
    require(
        summary["raft_write_latency"]["samples"] > 0,
        f"{job}: no raft write latency samples",
    )
    require(summary["raft_replica_read_latency"]["samples"] > 0, f"{job}: no raft replica read samples")
    require(
        summary["raft_write_qps"]["ops"] > 0 and summary["raft_write_qps"]["ops_per_sec"] > 0,
        f"{job}: raft write qps is not positive: {summary.get('raft_write_qps')}",
    )
    require(
        summary["raft_read_qps"]["ops"] > 0 and summary["raft_read_qps"]["ops_per_sec"] > 0,
        f"{job}: raft read qps is not positive: {summary.get('raft_read_qps')}",
    )
    require(summary["shared_store"] is not None, f"{job}: shared-store comparison missing")
    shared = summary["shared_store"]
    require(shared["sync_max_lag"] == 0, f"{job}: sync shared-store lag is non-zero")
    require(
        shared["sync_primary_write_qps"]["ops"] > 0
        and shared["sync_primary_write_qps"]["ops_per_sec"] > 0,
        f"{job}: sync primary write qps is not positive: {shared.get('sync_primary_write_qps')}",
    )
    require(
        shared["async_primary_write_qps"]["ops"] > 0
        and shared["async_primary_write_qps"]["ops_per_sec"] > 0,
        f"{job}: async primary write qps is not positive: {shared.get('async_primary_write_qps')}",
    )
    require(
        shared["sync_replica_read_qps"]["ops"] > 0
        and shared["sync_replica_read_qps"]["ops_per_sec"] > 0,
        f"{job}: sync replica read qps is not positive: {shared.get('sync_replica_read_qps')}",
    )
    require(
        shared["async_replica_read_qps"]["ops"] > 0
        and shared["async_replica_read_qps"]["ops_per_sec"] > 0,
        f"{job}: async replica read qps is not positive: {shared.get('async_replica_read_qps')}",
    )
    require(
        shared["sync_primary_write_latency"]["p99_us"] >= shared["sync_storage_write_latency"]["p50_us"],
        f"{job}: sync primary p99 is unexpectedly below sync storage p50",
    )
    flush_every = max(1, shared["async_flush_every"])
    require(
        shared["async_max_lag"] <= flush_every - 1,
        f"{job}: async shared-store lag {shared['async_max_lag']} exceeds flush window {flush_every}",
    )


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


def validate_storage_production(job, summary):
    require(summary["corpus_name"] == "temporalstore-storage-migration-corpus", f"{job}: unexpected corpus")
    require(summary["cases"], f"{job}: no storage production cases")
    for case in summary["cases"]:
        require(case["mutation_count"] > 0, f"{job}: no mutations in case {case['case_name']}")
        require(case["slot_dump_manifest_id"], f"{job}: no slot dump manifest in case {case['case_name']}")
        require(case["dumped_slot_count"] > 0, f"{job}: no dumped slots in case {case['case_name']}")
        require(case["cache_warmup_page_refs"] > 0, f"{job}: no cache warmup refs in case {case['case_name']}")
        require(case["recovery_ok_before_restart"], f"{job}: pre-restart recovery failed in {case['case_name']}")
        require(case["recovery_ok_after_restart"], f"{job}: post-restart recovery failed in {case['case_name']}")
        require(
            case["shared_store_sync_applied"] == case["mutation_count"],
            f"{job}: sync shared-store replay mismatch in {case['case_name']}",
        )
        require(
            case["shared_store_async_applied"] == case["mutation_count"],
            f"{job}: async shared-store replay mismatch in {case['case_name']}",
        )
        require(case["raft_leader_after_transfer"] == 2, f"{job}: raft leader did not transfer in {case['case_name']}")


def validate_context_workflow(job, summary):
    require(summary["extraction_ok"], f"{job}: context extraction failed")
    require(summary["retrieve_block_count"] >= 2, f"{job}: not enough retrieved context blocks")
    require(summary["selected_block_count"] > 0, f"{job}: no injected context blocks selected")
    require(summary["audit_selected_ref_count"] == summary["selected_block_count"], f"{job}: audit selected refs mismatch")
    require(summary["injected_prompt_contains_context"], f"{job}: injected prompt missing context wrapper")
    require(summary["provider_name"], f"{job}: provider name missing")


def validate_raft_secondary(job, summary):
    require(summary["writes"], f"{job}: no writes recorded")
    for write in summary["writes"]:
        require(write["status"]["ok"], f"{job}: write failed: {write}")
    require(summary["reads_after_restart"], f"{job}: no restart reads recorded")
    expected_values = {
        "secondary-before-restart": "v1",
        "secondary-while-down": "v3",
        "secondary-after-restart": "v4",
    }
    for read in summary["reads_after_restart"]:
        require(read["status"]["ok"], f"{job}: restart read failed: {read}")
        require(
            read["value"] == expected_values[read["key"]],
            f"{job}: restart read mismatch: {read}",
        )
    for node in summary["nodes"]:
        require(node["status"]["has_majority"], f"{job}: node {node['node_id']} has no majority")
        require(node["apply_health"]["healthy"], f"{job}: node {node['node_id']} apply health unhealthy")
    require(
        summary["partition"]["isolated_read_status"]["ok"] is False,
        f"{job}: isolated partition read unexpectedly succeeded",
    )
    require(
        summary["partition"]["healed_read"]["value"] == "v-partition",
        f"{job}: healed partition read mismatch",
    )
    require(summary["lagging_follower"]["observed_lag"] > 0, f"{job}: follower lag was not observed")
    for read in summary["lagging_follower"]["catchup_reads"]:
        require(read["status"]["ok"], f"{job}: lagging follower catch-up read failed: {read}")
    require(
        summary["network_vote"]["stale_response"]["vote_granted"] is False,
        f"{job}: stale vote was granted",
    )
    require(
        summary["network_vote"]["valid_response"]["vote_granted"] is True,
        f"{job}: valid vote was rejected",
    )
    require(summary["rolling_restart"]["restarted_nodes"], f"{job}: rolling restart did not run")
    for write in summary["rolling_restart"]["writes_after_each_restart"]:
        require(write["status"]["ok"], f"{job}: rolling restart write failed: {write}")
    for read in summary["rolling_restart"]["reads_after_each_restart"]:
        require(read["status"]["ok"], f"{job}: rolling restart read failed: {read}")
    require(summary["failover"]["status"]["ok"], f"{job}: failover status failed")
    for read in summary["reads_after_leader_crash"]:
        require(read["status"]["ok"], f"{job}: post-leader-crash read failed: {read}")
        require(read["value"] == "v5", f"{job}: post-leader-crash read mismatch: {read}")


def main():
    parser = argparse.ArgumentParser(description="Validate TemporalStore AWS validation job JSON logs")
    parser.add_argument("--job", required=True)
    parser.add_argument("--log", required=True)
    args = parser.parse_args()

    summary = load_summary(args.log)
    if args.job.endswith("context-workflow-validation"):
        validate_context_workflow(args.job, summary)
    elif args.job.endswith("storage-production-validation"):
        validate_storage_production(args.job, summary)
    elif args.job.endswith("raft-secondary-validation"):
        validate_raft_secondary(args.job, summary)
    elif args.job.endswith("raft-validation"):
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
