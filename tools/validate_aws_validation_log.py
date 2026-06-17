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


def validate_storage_fault_matrix(job, summary):
    require(summary["production_ready_slice"], f"{job}: storage fault matrix failed")
    report = summary["report"]
    require(report["production_ready_slice"], f"{job}: nested report failed")
    require(report["scenario_count"] == 6, f"{job}: expected 6 fault scenarios")
    require(report["passed_count"] == report["scenario_count"], f"{job}: not all fault scenarios passed")
    require(not report["failed_scenarios"], f"{job}: failed scenarios present")
    expected = {
        "checksum_mismatch": "slot_dump_checksum_mismatch",
        "partial_manifest": "slot_dump_partial_manifest",
        "missing_page_segment": "slot_dump_missing_page_segments",
        "stale_manifest": "slot_dump_stale_manifest",
        "restart_during_install_roll_forward": "slot_dump_restart_roll_forward",
        "corrupt_page_segment": "corrupt_page_segments",
    }
    observed = {scenario["scenario"]: scenario for scenario in report["scenarios"]}
    require(set(observed) == set(expected), f"{job}: unexpected scenarios {sorted(observed)}")
    for name, expected_code in expected.items():
        scenario = observed[name]
        require(scenario["passed"], f"{job}: scenario {name} did not pass")
        require(
            scenario["actual_code"] == expected_code,
            f"{job}: scenario {name} code mismatch {scenario}",
        )
        if name in {"missing_page_segment", "stale_manifest", "corrupt_page_segment"}:
            require(not scenario["install_safe"], f"{job}: scenario {name} was install-safe")


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


def validate_metaserver_raft(job, summary):
    require(
        summary["initial_membership"] == [10, 11, 12],
        f"{job}: initial membership mismatch: {summary['initial_membership']}",
    )
    require(
        summary["membership_after_add"] == [10, 11, 12, 13],
        f"{job}: add-node membership mismatch: {summary['membership_after_add']}",
    )
    require(
        summary["membership_after_remove"] == [10, 11, 13],
        f"{job}: remove-node membership mismatch: {summary['membership_after_remove']}",
    )
    require(summary["unsupported_role_rejected"], f"{job}: unsupported learner/witness was not rejected")
    require(summary["wait_for_log_applied_index"] >= 1, f"{job}: read-index wait did not advance")
    require(summary["snapshot_index"] >= summary["wait_for_log_applied_index"], f"{job}: stale snapshot index")
    require(
        summary["snapshot_restore_read"] == "meta-snapshot-server",
        f"{job}: snapshot restore read mismatch",
    )
    require(
        summary["leader_after_transfer"] == 11,
        f"{job}: leader transfer target mismatch: {summary['leader_after_transfer']}",
    )
    require(
        summary["leader_after_failover"] != summary["leader_after_transfer"],
        f"{job}: leader did not change after failover",
    )
    require(summary["namespace_after_failover_visible"], f"{job}: post-failover namespace missing")
    require(summary["unavailable_without_majority"], f"{job}: write without majority was not rejected")


def validate_raft_distributed_parity(job, summary):
    require(summary["production_ready_slice"], f"{job}: production_ready_slice is false")
    data_node = summary["data_node"]
    require(
        data_node["distributed_node_count"] >= 4,
        f"{job}: expected at least four data-node raft nodes",
    )
    require(
        data_node["distributed_all_nodes_have_majority"],
        f"{job}: not all data-node raft nodes have healthy majority/apply state",
    )
    require(data_node["follower_write_rejected"], f"{job}: follower write was not rejected")
    require(
        set(data_node["replica_read_values"]) == {"replicated-value"},
        f"{job}: data-node replica reads diverged: {data_node['replica_read_values']}",
    )
    require(
        data_node["scale_down_voters"] and all(voters == [1, 2, 3] for voters in data_node["scale_down_voters"]),
        f"{job}: scale-down voters mismatch: {data_node['scale_down_voters']}",
    )
    require(
        data_node["scale_up_voters"] and all(voters == [1, 2, 3, 4] for voters in data_node["scale_up_voters"]),
        f"{job}: scale-up voters mismatch: {data_node['scale_up_voters']}",
    )
    require(
        data_node["external_snapshot_read"] == "from-external-snapshot",
        f"{job}: data-node external snapshot read mismatch",
    )
    for read in data_node["secondary_restart_reads"]:
        require(read["status"]["ok"], f"{job}: secondary restart read failed: {read}")
    require(
        data_node["partition_isolated_read_rejected"],
        f"{job}: isolated partition read was not rejected",
    )
    require(
        data_node["lagging_follower_observed_lag"] > 0,
        f"{job}: lagging follower was not observed",
    )
    require(data_node["leader_crash_failover_ok"], f"{job}: leader crash failover failed")
    require(
        set(data_node["post_leader_crash_values"]) == {"v5"},
        f"{job}: post-leader-crash reads diverged: {data_node['post_leader_crash_values']}",
    )

    metaserver = summary["metaserver"]
    require(
        metaserver["initial_membership"] == [10, 11, 12],
        f"{job}: metaserver initial membership mismatch",
    )
    require(
        metaserver["membership_after_add"] == [10, 11, 12, 13],
        f"{job}: metaserver add membership mismatch",
    )
    require(
        metaserver["membership_after_remove"] == [10, 11, 13],
        f"{job}: metaserver remove membership mismatch",
    )
    require(metaserver["unsupported_role_rejected"], f"{job}: metaserver unsupported role accepted")
    require(metaserver["wait_for_log_applied_index"] >= 1, f"{job}: metaserver read-index did not advance")
    require(
        metaserver["snapshot_index"] >= metaserver["wait_for_log_applied_index"],
        f"{job}: metaserver snapshot index is stale",
    )
    require(
        metaserver["snapshot_restore_read"] == "meta-snapshot-server",
        f"{job}: metaserver snapshot restore read mismatch",
    )
    require(metaserver["leader_after_transfer"] == 11, f"{job}: metaserver leader transfer mismatch")
    require(
        metaserver["leader_after_failover"] != metaserver["leader_after_transfer"],
        f"{job}: metaserver leader did not change after failover",
    )
    require(metaserver["namespace_after_failover_visible"], f"{job}: metaserver post-failover state missing")
    require(metaserver["unavailable_without_majority"], f"{job}: metaserver no-majority write committed")


def main():
    parser = argparse.ArgumentParser(description="Validate TemporalStore AWS validation job JSON logs")
    parser.add_argument("--job", required=True)
    parser.add_argument("--log", required=True)
    args = parser.parse_args()

    summary = load_summary(args.log)
    if args.job.endswith("context-workflow-validation"):
        validate_context_workflow(args.job, summary)
    elif args.job.endswith("storage-fault-matrix-validation"):
        validate_storage_fault_matrix(args.job, summary)
    elif args.job.endswith("storage-production-validation"):
        validate_storage_production(args.job, summary)
    elif args.job.endswith("raft-secondary-validation"):
        validate_raft_secondary(args.job, summary)
    elif args.job.endswith("raft-distributed-parity-validation"):
        validate_raft_distributed_parity(args.job, summary)
    elif args.job.endswith("metaserver-raft-validation"):
        validate_metaserver_raft(args.job, summary)
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
