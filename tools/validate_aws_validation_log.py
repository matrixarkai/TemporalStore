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
    slo = summary.get("slo_report")
    require(slo is not None, f"{job}: slo_report missing")
    require(slo["replication_healthy"], f"{job}: SLO replication health is false")
    require(slo["max_replica_lag"] == 0, f"{job}: SLO replica lag is {slo['max_replica_lag']}")
    require(slo["p99_write_us"] >= slo["p50_write_us"], f"{job}: SLO write percentiles are invalid")
    require(slo["p99_read_us"] >= slo["p50_read_us"], f"{job}: SLO read percentiles are invalid")
    require(slo["write_ops_per_sec"] > 0, f"{job}: SLO write throughput is not positive")
    require(slo["read_ops_per_sec"] > 0, f"{job}: SLO read throughput is not positive")
    require(
        "cpu_observed" in slo and "memory_observed" in slo and "disk_observed" in slo and "network_observed" in slo,
        f"{job}: SLO resource collector placeholders missing",
    )
    for field in [
        "docker_or_aws_slo_evidence_ready",
        "storage_deployment_scale_slo_ready",
        "metaserver_process_ready",
        "proxy_process_ready",
        "client_process_ready",
        "data_node_process_ready",
        "raft_failover_ready",
        "storage_pressure_ready",
        "cache_pressure_ready",
        "proxy_convergence_ready",
        "workload_replay_ready",
    ]:
        require(field in slo, f"{job}: SLO evidence field {field} missing")
    require(
        slo["storage_deployment_scale_slo_ready"],
        f"{job}: storage deployment scale SLO evidence is false",
    )
    require(
        all(
            slo[field]
            for field in [
                "metaserver_process_ready",
                "proxy_process_ready",
                "client_process_ready",
                "data_node_process_ready",
                "raft_failover_ready",
                "storage_pressure_ready",
                "cache_pressure_ready",
                "proxy_convergence_ready",
                "workload_replay_ready",
            ]
        ),
        f"{job}: not all SLO process/fault/workload evidence fields are true",
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
    require(summary["context_pipeline_ready"], f"{job}: context pipeline parity evidence is not ready")
    require(summary["management_ready"], f"{job}: context management report is not ready")
    require(summary["ingest_extract_ready"], f"{job}: context ingest/extract pipeline is not ready")
    require(summary["retrieve_pipeline_ready"], f"{job}: context retrieval pipeline handoff is not ready")
    require(summary["ingest_extract_accepted"] > 0, f"{job}: no context ingest/extract sources were accepted")
    require(summary["ingest_extract_failed"] == 0, f"{job}: context ingest/extract sources failed")
    require(summary["ingest_extract_source_count"] == summary["ingest_extract_accepted"], f"{job}: context ingest/extract source accounting mismatch")
    require(summary["ingest_extract_unique_nodes"] == summary["ingest_extract_accepted"], f"{job}: context ingest/extract unique node accounting mismatch")
    require(summary["ingest_extract_source_kind_counts"].get("incident", 0) >= 1, f"{job}: context incident source kind missing")
    require(summary["ingest_extract_source_kind_counts"].get("ticket", 0) >= 1, f"{job}: context ticket source kind missing")
    require(summary["ingest_extract_provider_counts"].get("mock-openai-compatible", 0) >= 1, f"{job}: context provider accounting missing")
    require(summary["pipeline_stage_ready_count"] == len(summary["pipeline_stages"]), f"{job}: not all context pipeline stages are ready")
    require("tenant isolation" in summary["policy_controls"], f"{job}: context policy controls missing tenant isolation")
    require("mock-openai-compatible" in summary["provider_names"], f"{job}: context provider names missing mock provider")
    require(summary["benchmark_ready"], f"{job}: context benchmark is not ready")
    require(summary["benchmark_profile"], f"{job}: context benchmark profile missing")
    require(summary["benchmark_workload_signature"] != 0, f"{job}: context benchmark workload signature missing")
    require(summary["benchmark_topic_count"] == summary["benchmark_query_count"], f"{job}: context benchmark topic/query count mismatch")
    require(summary["benchmark_min_sources_per_topic"] > 0, f"{job}: context benchmark topic source coverage missing")
    require(summary["benchmark_max_sources_per_topic"] >= summary["benchmark_min_sources_per_topic"], f"{job}: context benchmark topic source range invalid")
    require(summary["benchmark_source_kind_coverage_count"] >= 3, f"{job}: context benchmark source-kind coverage too small")
    require(summary["benchmark_source_count"] >= 16, f"{job}: context benchmark source count too small")
    require(summary["benchmark_query_count"] >= 1, f"{job}: context benchmark query count missing")
    require(summary["benchmark_hit_at_k"] >= 1.0, f"{job}: context benchmark hit@k is below 1.0")
    require(summary["benchmark_mean_reciprocal_rank"] >= 0.0, f"{job}: context benchmark MRR metric missing")
    require(summary["benchmark_recall_at_k"] >= 1.0, f"{job}: context benchmark recall proxy is below 1.0")
    require(summary["benchmark_token_reduction_percent"] > 0.0, f"{job}: context benchmark token reduction missing")
    require(summary["benchmark_ingest_sources_per_sec"] > 0.0, f"{job}: context benchmark ingest throughput missing")
    require(summary["benchmark_retrieve_queries_per_sec"] > 0.0, f"{job}: context benchmark retrieve throughput missing")
    require(summary["benchmark_inject_queries_per_sec"] > 0.0, f"{job}: context benchmark inject throughput missing")
    require(summary["benchmark_per_query_count"] == summary["benchmark_query_count"], f"{job}: context benchmark per-query report count mismatch")
    require(summary["benchmark_retrieve_p95_ms"] >= summary["benchmark_retrieve_p50_ms"], f"{job}: context benchmark latency percentiles invalid")
    require(summary["benchmark_inject_p95_ms"] >= summary["benchmark_inject_p50_ms"], f"{job}: context benchmark injection latency percentiles invalid")
    require(summary["benchmark_avg_retrieved_blocks_per_query"] > 0.0, f"{job}: context benchmark retrieved block average missing")
    require(summary["benchmark_avg_selected_blocks_per_query"] > 0.0, f"{job}: context benchmark selected block average missing")
    require(summary["benchmark_avg_selected_tokens_per_query"] > 0.0, f"{job}: context benchmark selected token average missing")
    require(summary["benchmark_max_selected_tokens_per_query"] >= summary["benchmark_avg_selected_tokens_per_query"], f"{job}: context benchmark selected token max below average")
    require(summary["benchmark_zero_hit_queries"] == 0, f"{job}: context benchmark has zero-hit queries")
    require(summary["benchmark_threshold_passed"], f"{job}: context benchmark thresholds failed")
    require(summary["benchmark_threshold_violation_count"] == 0, f"{job}: context benchmark threshold violations present")
    thresholds = summary["benchmark_thresholds"]
    require(thresholds["min_hit_at_k"] >= 1.0, f"{job}: context benchmark hit@k threshold too low")
    require(thresholds["min_recall_at_k"] >= 1.0, f"{job}: context benchmark recall threshold too low")
    require(thresholds["min_token_reduction_percent"] > 0.0, f"{job}: context benchmark token threshold missing")
    require(thresholds["max_retrieve_p95_ms"] >= thresholds["max_retrieve_p50_ms"], f"{job}: context benchmark threshold latency percentiles invalid")
    require(summary["benchmark_sweep_ready"], f"{job}: context benchmark sweep is not ready")
    require(summary["benchmark_sweep_profile_count"] >= 2, f"{job}: context benchmark sweep profile count too small")
    require(summary["benchmark_sweep_profile_signature_count"] == summary["benchmark_sweep_profile_count"], f"{job}: context benchmark sweep signature count mismatch")
    require(summary["benchmark_sweep_min_sources_per_topic"] > 0, f"{job}: context benchmark sweep topic source coverage missing")
    require(summary["benchmark_sweep_max_sources_per_topic"] >= summary["benchmark_sweep_min_sources_per_topic"], f"{job}: context benchmark sweep topic source range invalid")
    require(summary["benchmark_sweep_min_source_kind_coverage_count"] >= 3, f"{job}: context benchmark sweep source-kind coverage too small")
    require(summary["benchmark_sweep_total_sources"] >= summary["benchmark_source_count"], f"{job}: context benchmark sweep source coverage too small")
    require(summary["benchmark_sweep_total_queries"] >= summary["benchmark_query_count"], f"{job}: context benchmark sweep query coverage too small")
    require(summary["benchmark_sweep_min_hit_at_k"] >= 1.0, f"{job}: context benchmark sweep hit@k is below 1.0")
    require(summary["benchmark_sweep_min_mean_reciprocal_rank"] >= 0.0, f"{job}: context benchmark sweep MRR metric missing")
    require(summary["benchmark_sweep_min_token_reduction_percent"] > 0.0, f"{job}: context benchmark sweep token reduction missing")
    require(summary["benchmark_sweep_max_retrieve_p95_ms"] >= 0, f"{job}: context benchmark sweep latency coverage invalid")
    require(summary["benchmark_sweep_max_inject_p95_ms"] >= 0, f"{job}: context benchmark sweep injection latency coverage invalid")
    require(summary["benchmark_sweep_total_zero_hit_queries"] == 0, f"{job}: context benchmark sweep has zero-hit queries")
    require(summary["benchmark_sweep_avg_selected_tokens_per_query"] > 0.0, f"{job}: context benchmark sweep selected token average missing")
    require(summary["benchmark_sweep_all_thresholds_passed"], f"{job}: context benchmark sweep thresholds failed")
    require(summary["benchmark_sweep_threshold_violation_count"] == 0, f"{job}: context benchmark sweep threshold violations present")
    require(summary["external_benchmark_ready"], f"{job}: external LOCOMO/LongMemEval-style benchmark is not ready")
    require(summary["external_benchmark_case_count"] > 0, f"{job}: external context benchmark has no cases")
    require(summary["external_benchmark_hit_at_k"] >= 1.0, f"{job}: external context benchmark hit@k regressed")
    require(summary["external_benchmark_mean_reciprocal_rank"] >= 0.0, f"{job}: external context benchmark MRR metric missing")
    require(summary["external_benchmark_missing_expected_terms"] == 0, f"{job}: external context benchmark missing expected terms")
    require(summary["external_benchmark_missing_expected_refs"] == 0, f"{job}: external context benchmark missing expected refs")
    require(summary["external_benchmark_zero_hit_queries"] == 0, f"{job}: external context benchmark has zero-hit queries")
    require(summary["external_benchmark_source"], f"{job}: external context benchmark source missing")
    require("/context/manage" in summary["managed_routes"], f"{job}: context management route missing")
    require("/context/ingest_extract" in summary["managed_routes"], f"{job}: context ingest/extract route missing")
    for field in [
        "restart_replay_ready",
        "shared_store_sync_ready",
        "shared_store_async_ready",
        "raft_read_ready",
        "unified_corpus_ready",
    ]:
        require(summary[field], f"{job}: context pipeline field {field} is false")
    parity = summary["parity"]
    require(parity["pipeline_ready"], f"{job}: context parity report is not ready")
    for field in [
        "cpp_context_models_ready",
        "cpp_context_model_ids_ready",
        "cpp_context_timeline_semantics_ready",
        "cpp_context_validation_limits_ready",
        "openviking_tiers_ready",
        "extraction_stage_ready",
        "retrieval_stage_ready",
        "injection_stage_ready",
        "index_refs_ready",
        "pack_audit_ready",
        "summary_dirty_ready",
        "restart_replay_ready",
        "shared_store_sync_ready",
        "shared_store_async_ready",
        "raft_read_ready",
        "unified_corpus_ready",
    ]:
        require(parity[field], f"{job}: context parity report field {field} is false")
    require(
        any("OpenViking-style L0/L1/L2" in item for item in summary["parity_evidence"]),
        f"{job}: context parity evidence missing OpenViking tier coverage",
    )
    require(
        any("model ids 9-13" in item for item in summary["parity_evidence"]),
        f"{job}: context parity evidence missing C++ model id coverage",
    )


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
    validate_byteraft_process_semantics(
        job,
        summary["openraft_process_rollout"],
        "data-node",
        require_membership=True,
        require_secondary_lag=True,
    )


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
    require(summary["lagging_node_id"] == 13, f"{job}: lagging metaserver node mismatch")
    require(
        summary["lagging_write_hidden_before_catchup"],
        f"{job}: lagging metaserver saw tail write before catch-up",
    )
    require(
        summary["lagging_snapshot_restore_missed_tail"],
        f"{job}: stale metaserver snapshot included post-snapshot tail",
    )
    require(
        summary["lagging_catchup_read"] == "meta-after-lag",
        f"{job}: lagging metaserver catch-up read mismatch",
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
    require(
        summary["membership_replace_after_failover"]["voters"] == [10, 12, 13],
        f"{job}: post-failover replacement membership mismatch",
    )
    require(
        summary["post_replace_route_read"] == "meta-after-replace",
        f"{job}: post-replacement route read mismatch",
    )
    require(
        summary["membership_scale_down_after_replace"]["voters"] == [10, 13],
        f"{job}: second metaserver scale-down membership mismatch",
    )
    require(
        summary["post_scale_down_route_read"] == "meta-after-second-scale-down",
        f"{job}: post-second-scale-down route read mismatch",
    )
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
    require(data_node["post_rescale_down_write_ok"], f"{job}: post-rescale-down write failed")
    require(data_node["post_rescale_up_write_ok"], f"{job}: post-rescale-up write failed")
    require(
        data_node["rescale_down_voters"] and all(voters == [1, 2, 3] for voters in data_node["rescale_down_voters"]),
        f"{job}: rescale-down voters mismatch: {data_node['rescale_down_voters']}",
    )
    require(
        data_node["rescale_up_voters"] and all(voters == [1, 2, 3, 4] for voters in data_node["rescale_up_voters"]),
        f"{job}: rescale-up voters mismatch: {data_node['rescale_up_voters']}",
    )
    require(
        set(data_node["rescale_down_read_values"]) == {"after-rescale-down"},
        f"{job}: rescale-down reads diverged: {data_node['rescale_down_read_values']}",
    )
    require(
        set(data_node["rescale_up_read_values"]) == {"after-rescale-up"},
        f"{job}: rescale-up reads diverged: {data_node['rescale_up_read_values']}",
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
    data_rollout = data_node["openraft_process_rollout"]
    validate_byteraft_process_semantics(
        job,
        data_rollout,
        "data-node",
        require_membership=True,
        require_secondary_lag=True,
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
    require(metaserver["lagging_node_id"] == 13, f"{job}: metaserver lagging node mismatch")
    require(
        metaserver["lagging_write_hidden_before_catchup"],
        f"{job}: metaserver lagging voter saw tail before catch-up",
    )
    require(
        metaserver["lagging_snapshot_restore_missed_tail"],
        f"{job}: metaserver stale snapshot included post-snapshot tail",
    )
    require(
        metaserver["lagging_catchup_read"] == "meta-after-lag",
        f"{job}: metaserver lagging catch-up read mismatch",
    )
    require(metaserver["leader_after_transfer"] == 11, f"{job}: metaserver leader transfer mismatch")
    require(
        metaserver["leader_after_failover"] != metaserver["leader_after_transfer"],
        f"{job}: metaserver leader did not change after failover",
    )
    require(metaserver["namespace_after_failover_visible"], f"{job}: metaserver post-failover state missing")
    require(
        metaserver["membership_replace_after_failover_voters"] == [10, 12, 13],
        f"{job}: metaserver replacement membership mismatch",
    )
    require(
        metaserver["post_replace_route_read"] == "meta-after-replace",
        f"{job}: metaserver post-replacement route read mismatch",
    )
    require(
        metaserver["membership_scale_down_after_replace_voters"] == [10, 13],
        f"{job}: metaserver second scale-down membership mismatch",
    )
    require(
        metaserver["post_scale_down_route_read"] == "meta-after-second-scale-down",
        f"{job}: metaserver second scale-down route read mismatch",
    )
    require(metaserver["unavailable_without_majority"], f"{job}: metaserver no-majority write committed")
    scheduler = metaserver["scheduler_execution_coverage"]
    require(scheduler["ready"], f"{job}: metaserver scheduler execution coverage is not ready")
    for field in [
        "networked_multi_process_raft_ready",
        "missing_primary_repair_ready",
        "under_replicated_repair_ready",
        "stale_dead_server_repair_ready",
        "load_reload_unload_ready",
        "cooldown_and_safe_mode_ready",
        "scheduler_task_replay_ready",
        "membership_change_ready",
        "durable_data_raft_membership_ready",
        "stale_scheduler_token_rejection_ready",
    ]:
        require(scheduler[field], f"{job}: metaserver scheduler field {field} is false")
    rollout = metaserver["openraft_process_rollout"]
    require(rollout["ready"], f"{job}: metaserver OpenRaft process rollout is not ready")
    require(rollout["multi_process_log_store_validated"], f"{job}: metaserver log-store rollout missing")
    require(rollout["data_node_membership_results_ready"], f"{job}: data-node membership results missing")
    validate_byteraft_process_semantics(
        job,
        rollout,
        "metaserver",
        require_membership=True,
        require_secondary_lag=False,
    )
    membership = metaserver["meta_owned_data_raft_membership"]
    require(membership["ready"], f"{job}: meta-owned data-Raft membership report is not ready")
    workflow = membership["workflow"]
    for field in [
        "learner_added",
        "catch_up_verified",
        "promoted_to_voter",
        "membership_committed",
        "leader_transferred",
        "voter_removed",
    ]:
        require(workflow[field], f"{job}: meta-owned data-Raft workflow field {field} is false")
    require(workflow["learner_id"] == 4, f"{job}: data-Raft learner id mismatch")
    require(workflow["removed_voter_id"] == 1, f"{job}: data-Raft removed voter mismatch")
    require(workflow["requested_leader_id"] == 4, f"{job}: data-Raft leader-transfer target mismatch")
    require(workflow["final_leader_id"] == 4, f"{job}: data-Raft final leader mismatch")
    require(workflow["final_voters"] == [2, 3, 4], f"{job}: data-Raft final voters mismatch")
    for field in [
        "follower_lag_validated",
        "failover_validated",
        "scale_up_validated",
        "scale_down_validated",
        "secondary_replication_validated",
        "networked_process_api_used",
        "persisted_through_meta_raft_replay",
        "stale_scheduler_token_rejected",
    ]:
        require(membership[field], f"{job}: meta-owned data-Raft membership field {field} is false")


def validate_byteraft_process_semantics(
    job,
    rollout,
    label,
    *,
    require_membership,
    require_secondary_lag,
):
    require(rollout["ready"], f"{job}: {label} OpenRaft process rollout is not ready")
    require(
        rollout["multi_process_log_store_validated"],
        f"{job}: {label} multi-process log-store validation missing",
    )
    require(
        rollout.get("real_process_path_evidence_validated") is True,
        f"{job}: {label} real spawned-process durable-store evidence missing",
    )
    require(
        rollout.get("spawned_process_count", 0) >= 3,
        f"{job}: {label} fewer than three spawned processes reported",
    )
    require(
        rollout.get("independent_wal_dirs") is True,
        f"{job}: {label} independent WAL dirs missing",
    )
    require(
        rollout.get("independent_snapshot_dirs") is True,
        f"{job}: {label} independent snapshot dirs missing",
    )
    require(
        rollout.get("restarted_node_count", 0) >= rollout.get("spawned_process_count", 0),
        f"{job}: {label} restart recovery was not observed for every process",
    )
    require(
        rollout.get("per_node_log_store_inspection_count", 0)
        >= rollout.get("spawned_process_count", 0),
        f"{job}: {label} per-node log-store inspection did not cover every process",
    )
    semantics = rollout.get("byteraft_process_semantics")
    require(isinstance(semantics, dict), f"{job}: {label} ByteRaft process semantics missing")
    require(semantics.get("ready") is True, f"{job}: {label} ByteRaft process semantics not ready")
    require(
        semantics.get("observed_process_requests", 0) >= rollout.get("spawned_process_count", 0),
        f"{job}: {label} process requests were not observed for every process",
    )
    require(
        semantics.get("read_index_responses_observed", 0) > 0,
        f"{job}: {label} read-index responses were not observed",
    )
    if label == "data-node":
        write_ids = rollout.get("leader_transfer_write_ids_observed", [])
        commit_indexes = rollout.get("leader_transfer_commit_indexes_observed", [])
        require(
            rollout.get("leader_transfer_under_load_observed") is True,
            f"{job}: {label} leader transfer under load was not observed",
        )
        require(
            rollout.get("leader_transfer_exact_once_observed") is True,
            f"{job}: {label} leader transfer exact-once writes were not observed",
        )
        require(len(write_ids) >= 6, f"{job}: {label} leader transfer write IDs missing")
        require(
            len(set(write_ids)) == len(write_ids),
            f"{job}: {label} duplicate leader transfer write IDs observed",
        )
        require(
            len(commit_indexes) >= 2,
            f"{job}: {label} leader transfer commit indexes missing",
        )
        require(
            all(index > 0 for index in commit_indexes),
            f"{job}: {label} leader transfer commit indexes were not committed",
        )
    for field in [
        "per_peer_pipeline_state_observed",
        "append_pipeline_state_observed",
        "replicate_inflight_limits_observed",
        "max_replicate_bytes_observed",
        "oversized_log_rejection_observed",
        "apply_batch_backpressure_observed",
        "append_queue_depth_observed",
        "replication_pressure_counters_observed",
        "max_disk_replicate_log_num_observed",
        "snapshot_lifecycle_observed",
        "snapshot_chunk_retry_backpressure_observed",
        "snapshot_send_timeout_observed",
        "snapshot_install_progress_observed",
        "snapshot_install_rollback_observed",
        "snapshot_membership_change_observed",
        "snapshot_rejoin_after_compacted_log_observed",
        "wal_segment_lifecycle_observed",
        "bounded_stale_partition_reads_observed",
        "follower_lease_expiration_observed",
        "wal_segment_release_rules_observed",
        "wal_first_last_index_status_observed",
        "wal_slow_fsync_backpressure_observed",
        "restart_log_store_comparison_observed",
        "fsm_apply_atomicity_observed",
        "apply_fence_recovery_observed",
        "snapshot_install_apply_fence_recovery_observed",
        "storage_wal_snapshot_crash_recovery_observed",
        "restart_recovery_observed",
        "failover_observed",
    ]:
        require(semantics.get(field) is True, f"{job}: {label} semantics field {field} is false")
    if require_membership:
        require(
            semantics.get("membership_change_observed") is True,
            f"{job}: {label} membership-change semantics missing",
        )
    if require_secondary_lag:
        require(
            semantics.get("secondary_lag_observed") is True,
            f"{job}: {label} secondary-lag semantics missing",
        )


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
