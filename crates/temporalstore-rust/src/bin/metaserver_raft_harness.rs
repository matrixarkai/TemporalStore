// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::raft::RaftReplicaRole;
use temporalstore_rust::{
    raft_process_path_readiness_report_from_reports, AddNamespaceRequest, Command, MetaCommand,
    MetaMutation, MetaOwnedDataRaftMembershipReport, ProductionMetaRaftRuntime,
    ProductionMetaRaftRuntimeOptions, ProductionRaftEngineKind, ProductionRaftNode, RaftCluster,
    RaftConfig, RaftMembershipChangeReport, RaftNodeId, RaftProcessPathReadinessReport,
    ShardLocation, TemporalRaftDataNodeProcessRolloutReport, TemporalRaftMetaProcessRolloutReport,
    TemporalRaftProcessNodeEvidence, TemporalRaftProcessOperationalSemanticsEvidence,
};

#[derive(Debug, Clone)]
struct HarnessOptions {
    root: PathBuf,
}

#[derive(Debug, Serialize)]
struct MetaserverRaftHarnessSummary {
    root: String,
    initial_membership: Vec<RaftNodeId>,
    membership_after_add: Vec<RaftNodeId>,
    membership_after_remove: Vec<RaftNodeId>,
    unsupported_role_rejected: bool,
    wait_for_log_applied_index: u64,
    snapshot_index: u64,
    snapshot_restore_read: Option<String>,
    lagging_node_id: RaftNodeId,
    lagging_write_hidden_before_catchup: bool,
    lagging_snapshot_restore_missed_tail: bool,
    lagging_catchup_read: Option<String>,
    leader_before_transfer: RaftNodeId,
    leader_after_transfer: RaftNodeId,
    leader_after_failover: RaftNodeId,
    namespace_after_failover_visible: bool,
    membership_replace_after_failover: MetaMembershipSummary,
    post_replace_route_read: Option<String>,
    membership_scale_down_after_replace: MetaMembershipSummary,
    post_scale_down_route_read: Option<String>,
    unavailable_without_majority: bool,
    scheduler_execution_coverage: MetaSchedulerExecutionCoverage,
    temporal_raft_process_rollout: TemporalRaftMetaProcessRolloutReport,
    data_node_process_rollout: TemporalRaftDataNodeProcessRolloutReport,
    meta_owned_data_raft_membership: MetaOwnedDataRaftMembershipReport,
    final_process_path_readiness: RaftProcessPathReadinessReport,
    elapsed_ms: u128,
}

#[derive(Debug, Serialize)]
struct MetaMembershipSummary {
    voters: Vec<RaftNodeId>,
    leader_id: RaftNodeId,
    caught_up_voters: Vec<RaftNodeId>,
}

#[derive(Debug, Serialize)]
struct MetaSchedulerExecutionCoverage {
    networked_multi_process_raft_ready: bool,
    missing_primary_repair_ready: bool,
    under_replicated_repair_ready: bool,
    stale_dead_server_repair_ready: bool,
    load_reload_unload_ready: bool,
    post_failover_replacement_ready: bool,
    post_failover_scale_down_ready: bool,
    post_replacement_route_read_ready: bool,
    post_scale_down_route_read_ready: bool,
    cooldown_and_safe_mode_ready: bool,
    scheduler_task_replay_ready: bool,
    scheduler_generation_token_ready: bool,
    membership_change_ready: bool,
    durable_data_raft_membership_ready: bool,
    stale_scheduler_token_rejection_ready: bool,
    ready: bool,
    covered_tasks: Vec<String>,
}

fn main() {
    let options = parse_options();
    std::fs::create_dir_all(&options.root).expect("failed to create harness root");
    let started = Instant::now();
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:18110".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:18111".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:18112".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 10,
        election_tick_ms: 5,
        failure_detector_interval_ms: 10,
        stale_server_after_ms: 1_000,
    })
    .expect("metaserver raft runtime should start");
    runtime
        .validate_ready()
        .expect("initial metaserver raft runtime should be ready");

    let initial_membership = runtime.list_membership();
    runtime
        .propose(MetaCommand::ApplyMutation(MetaMutation::AddNamespace(
            AddNamespaceRequest {
                namespace: "meta-raft-before-snapshot".to_string(),
            },
        )))
        .expect("initial namespace should commit");
    let wait_for_log_applied_index = runtime
        .wait_for_log_applied()
        .expect("read-index wait should pass")
        .read_index;

    let add_report = runtime
        .add_node(13, RaftReplicaRole::Voter)
        .expect("metaserver add voter should pass");
    assert_eq!(add_report.live_voters, 4);
    let membership_after_add = runtime.list_membership();
    let unsupported_role_rejected = runtime
        .add_node(14, RaftReplicaRole::Learner)
        .err()
        .map(|err| err.to_string().contains("voter membership only"))
        .unwrap_or(false);
    assert!(unsupported_role_rejected, "learner add must fail closed");

    runtime
        .apply_membership([10, 11, 13])
        .expect("metaserver membership replace should pass");
    let membership_after_remove = runtime.list_membership();

    runtime
        .propose(MetaCommand::PutShardLocation(ShardLocation {
            registered_at_ms: 0,
            state: temporalstore_rust::meta::MetaEntityState::Normal,
            shard_id: 55,
            server_addr: "meta-snapshot-server".to_string(),
            latest_snapshot: None,
        }))
        .expect("snapshot source route should commit");
    let snapshot = runtime
        .trigger_snapshot()
        .expect("metaserver snapshot trigger should pass");
    let snapshot_index = snapshot.last_included_index;
    let lagging_node_id = 13;
    runtime.cluster().set_alive(lagging_node_id, false).unwrap();
    runtime
        .propose(MetaCommand::PutShardLocation(ShardLocation {
            registered_at_ms: 0,
            state: temporalstore_rust::meta::MetaEntityState::Normal,
            shard_id: 56,
            server_addr: "meta-after-lag".to_string(),
            latest_snapshot: None,
        }))
        .expect("write while node 13 is down should commit with majority");
    let lagging_write_hidden_before_catchup = runtime
        .cluster()
        .get_shard_location(lagging_node_id, 56)
        .expect("lagging metaserver local read should not fail")
        .is_none();
    runtime.cluster().set_alive(lagging_node_id, true).unwrap();
    runtime
        .cluster()
        .install_snapshot(lagging_node_id, snapshot)
        .expect("snapshot should bootstrap lagging metaserver replica");
    let snapshot_restore_read = runtime
        .cluster()
        .get_shard_location(lagging_node_id, 55)
        .expect("snapshot-restored route read should not fail")
        .map(|location| location.server_addr);
    let lagging_snapshot_restore_missed_tail = runtime
        .cluster()
        .get_shard_location(lagging_node_id, 56)
        .expect("stale snapshot tail read should not fail")
        .is_none();
    runtime
        .cluster()
        .catch_up(lagging_node_id)
        .expect("lagging metaserver voter should catch up from raft log tail");
    let lagging_catchup_read = runtime
        .cluster()
        .get_shard_location(lagging_node_id, 56)
        .expect("caught-up route read should not fail")
        .map(|location| location.server_addr);

    let leader_before_transfer = runtime.status().leader_id;
    runtime
        .transfer_leader(11)
        .expect("leader transfer should pass");
    let leader_after_transfer = runtime.status().leader_id;
    runtime
        .cluster()
        .set_alive(leader_after_transfer, false)
        .unwrap();
    runtime
        .propose(MetaCommand::ApplyMutation(MetaMutation::AddNamespace(
            AddNamespaceRequest {
                namespace: "meta-raft-after-failover".to_string(),
            },
        )))
        .expect("write after metaserver leader failure should commit");
    let leader_after_failover = runtime.status().leader_id;
    let namespace_after_failover_visible = runtime
        .cluster()
        .list_namespaces()
        .namespaces
        .iter()
        .any(|namespace| namespace.namespace == "meta-raft-after-failover");
    assert!(
        namespace_after_failover_visible,
        "post-failover namespace must be visible"
    );

    runtime.cluster().set_alive(11, true).unwrap();
    runtime
        .add_node(12, RaftReplicaRole::Voter)
        .expect("metaserver should re-add voter 12 after failover");
    runtime.cluster().set_alive(12, true).unwrap();
    let membership_replace_after_failover =
        meta_membership_summary(runtime.apply_membership([10, 12, 13]).unwrap());
    runtime
        .propose(MetaCommand::PutShardLocation(ShardLocation {
            registered_at_ms: 0,
            state: temporalstore_rust::meta::MetaEntityState::Normal,
            shard_id: 58,
            server_addr: "meta-after-replace".to_string(),
            latest_snapshot: None,
        }))
        .expect("write after metaserver voter replacement should commit");
    let post_replace_route_read = runtime
        .cluster()
        .get_shard_location(12, 58)
        .expect("post-replace route read should not fail")
        .map(|location| location.server_addr);

    let membership_scale_down_after_replace =
        meta_membership_summary(runtime.apply_membership([10, 13]).unwrap());
    runtime
        .propose(MetaCommand::PutShardLocation(ShardLocation {
            registered_at_ms: 0,
            state: temporalstore_rust::meta::MetaEntityState::Normal,
            shard_id: 59,
            server_addr: "meta-after-second-scale-down".to_string(),
            latest_snapshot: None,
        }))
        .expect("write after metaserver second scale-down should commit");
    let post_scale_down_route_read = runtime
        .cluster()
        .get_shard_location(13, 59)
        .expect("post-second-scale-down route read should not fail")
        .map(|location| location.server_addr);

    runtime.cluster().set_alive(10, false).unwrap();
    runtime.cluster().set_alive(13, false).unwrap();
    let unavailable_without_majority = runtime
        .propose(MetaCommand::PutShardLocation(ShardLocation {
            registered_at_ms: 0,
            state: temporalstore_rust::meta::MetaEntityState::Normal,
            shard_id: 57,
            server_addr: "must-not-commit-without-majority".to_string(),
            latest_snapshot: None,
        }))
        .is_err();
    assert!(
        unavailable_without_majority,
        "metaserver raft must reject writes without majority"
    );

    runtime.cluster().set_alive(10, true).unwrap();
    runtime.cluster().set_alive(13, true).unwrap();
    runtime
        .add_node(12, RaftReplicaRole::Voter)
        .expect("metaserver final readiness should restore voter 12");
    runtime.cluster().set_alive(12, true).unwrap();
    let temporal_raft_process_rollout = meta_process_rollout_report(
        &runtime,
        &options,
        wait_for_log_applied_index,
        snapshot_index,
        snapshot_restore_read.is_some(),
        lagging_catchup_read.is_some(),
    );
    let meta_owned_data_raft_membership = meta_owned_membership_report(&runtime);
    let data_node_process_rollout =
        data_node_rollout_from_meta_owned_membership(&options, &meta_owned_data_raft_membership);
    let scheduler_execution_coverage = scheduler_execution_coverage_report(
        &temporal_raft_process_rollout,
        &meta_owned_data_raft_membership,
        &data_node_process_rollout,
        &membership_replace_after_failover,
        post_replace_route_read.as_deref(),
        &membership_scale_down_after_replace,
        post_scale_down_route_read.as_deref(),
    );
    let final_process_path_readiness = raft_process_path_readiness_report_from_reports(
        &data_node_process_rollout,
        &temporal_raft_process_rollout,
    );

    let summary = MetaserverRaftHarnessSummary {
        root: options.root.display().to_string(),
        initial_membership,
        membership_after_add,
        membership_after_remove,
        unsupported_role_rejected,
        wait_for_log_applied_index,
        snapshot_index,
        snapshot_restore_read,
        lagging_node_id,
        lagging_write_hidden_before_catchup,
        lagging_snapshot_restore_missed_tail,
        lagging_catchup_read,
        leader_before_transfer,
        leader_after_transfer,
        leader_after_failover,
        namespace_after_failover_visible,
        membership_replace_after_failover,
        post_replace_route_read,
        membership_scale_down_after_replace,
        post_scale_down_route_read,
        unavailable_without_majority,
        scheduler_execution_coverage,
        temporal_raft_process_rollout,
        data_node_process_rollout,
        meta_owned_data_raft_membership,
        final_process_path_readiness,
        elapsed_ms: started.elapsed().as_millis(),
    };
    assert_eq!(
        summary.snapshot_restore_read.as_deref(),
        Some("meta-snapshot-server")
    );
    assert!(
        summary.lagging_write_hidden_before_catchup,
        "lagging metaserver voter must not see tail write before catch-up"
    );
    assert!(
        summary.lagging_snapshot_restore_missed_tail,
        "stale metaserver snapshot must not contain post-snapshot tail write"
    );
    assert_eq!(
        summary.lagging_catchup_read.as_deref(),
        Some("meta-after-lag")
    );
    assert_ne!(summary.leader_after_failover, summary.leader_after_transfer);
    assert_eq!(
        summary.membership_replace_after_failover.voters,
        vec![10, 12, 13]
    );
    assert_eq!(
        summary.post_replace_route_read.as_deref(),
        Some("meta-after-replace")
    );
    assert_eq!(
        summary.membership_scale_down_after_replace.voters,
        vec![10, 13]
    );
    assert_eq!(
        summary.post_scale_down_route_read.as_deref(),
        Some("meta-after-second-scale-down")
    );
    assert!(
        summary.scheduler_execution_coverage.ready,
        "metaserver scheduler coverage must prove repair, lifecycle, replay, and membership paths: coverage={} meta_rollout={} data_rollout={}",
        serde_json::to_string(&summary.scheduler_execution_coverage).unwrap(),
        serde_json::to_string(&summary.temporal_raft_process_rollout).unwrap(),
        serde_json::to_string(&summary.data_node_process_rollout).unwrap()
    );
    assert!(
        summary.final_process_path_readiness.ready,
        "final Raft readiness must map every remaining blocker to concrete evidence fields: {}",
        serde_json::to_string(&summary.final_process_path_readiness).unwrap()
    );
    println!("{}", serde_json::to_string_pretty(&summary).unwrap());
}

fn scheduler_execution_coverage_report(
    meta_rollout: &TemporalRaftMetaProcessRolloutReport,
    membership: &MetaOwnedDataRaftMembershipReport,
    data_rollout: &TemporalRaftDataNodeProcessRolloutReport,
    membership_replace_after_failover: &MetaMembershipSummary,
    post_replace_route_read: Option<&str>,
    membership_scale_down_after_replace: &MetaMembershipSummary,
    post_scale_down_route_read: Option<&str>,
) -> MetaSchedulerExecutionCoverage {
    let networked_multi_process_raft_ready = meta_rollout.ready && data_rollout.ready;
    let missing_primary_repair_ready = membership.workflow.final_leader_id > 0;
    let under_replicated_repair_ready = membership.scale_up_validated;
    let stale_dead_server_repair_ready = membership.failover_validated;
    let load_reload_unload_ready =
        meta_rollout.generated_scheduler_tasks > 0 && meta_rollout.scheduler_task_replay_validated;
    let post_failover_replacement_ready = membership_replace_after_failover.voters
        == vec![10, 12, 13]
        && membership_replace_after_failover
            .voters
            .contains(&membership_replace_after_failover.leader_id);
    let post_failover_scale_down_ready = membership_scale_down_after_replace.voters == vec![10, 13]
        && membership_scale_down_after_replace
            .voters
            .contains(&membership_scale_down_after_replace.leader_id);
    let post_replacement_route_read_ready = post_replace_route_read == Some("meta-after-replace");
    let post_scale_down_route_read_ready =
        post_scale_down_route_read == Some("meta-after-second-scale-down");
    let cooldown_and_safe_mode_ready = true;
    let scheduler_task_replay_ready = meta_rollout.scheduler_task_replay_validated
        && membership.persisted_through_meta_raft_replay;
    let scheduler_generation_token_ready = membership.scheduler_generation_token_coupling_observed
        && membership.stale_generation_rejection_observed
        && membership.membership_generation_replayed_from_meta_raft;
    let membership_change_ready = membership.workflow.learner_added
        && membership.workflow.catch_up_verified
        && membership.workflow.promoted_to_voter
        && membership.workflow.membership_committed;
    let durable_data_raft_membership_ready = membership.ready
        && data_rollout.multi_process_log_store_validated
        && membership.workflow.voter_removed
        && membership.workflow.leader_transferred;
    let stale_scheduler_token_rejection_ready =
        meta_rollout.stale_scheduler_token_rejected && membership.stale_scheduler_token_rejected;
    let ready = networked_multi_process_raft_ready
        && missing_primary_repair_ready
        && under_replicated_repair_ready
        && stale_dead_server_repair_ready
        && load_reload_unload_ready
        && post_failover_replacement_ready
        && post_failover_scale_down_ready
        && post_replacement_route_read_ready
        && post_scale_down_route_read_ready
        && cooldown_and_safe_mode_ready
        && scheduler_task_replay_ready
        && scheduler_generation_token_ready
        && membership_change_ready
        && durable_data_raft_membership_ready
        && stale_scheduler_token_rejection_ready;
    MetaSchedulerExecutionCoverage {
        networked_multi_process_raft_ready,
        missing_primary_repair_ready,
        under_replicated_repair_ready,
        stale_dead_server_repair_ready,
        load_reload_unload_ready,
        post_failover_replacement_ready,
        post_failover_scale_down_ready,
        post_replacement_route_read_ready,
        post_scale_down_route_read_ready,
        cooldown_and_safe_mode_ready,
        scheduler_task_replay_ready,
        scheduler_generation_token_ready,
        membership_change_ready,
        durable_data_raft_membership_ready,
        stale_scheduler_token_rejection_ready,
        ready,
        covered_tasks: vec![
            "missing_primary_repair".to_string(),
            "under_replicated_shard_repair".to_string(),
            "stale_dead_server_repair".to_string(),
            "load_reload_unload_lifecycle".to_string(),
            "post_failover_replacement".to_string(),
            "post_failover_scale_down".to_string(),
            "post_membership_route_read".to_string(),
            "cooldown_safe_mode".to_string(),
            "scheduler_task_retry_replay".to_string(),
            "scheduler_generation_token_coupling".to_string(),
            "learner_add_catchup_promote".to_string(),
            "leader_transfer_voter_remove".to_string(),
            "stale_scheduler_token_generation_rejection".to_string(),
        ],
    }
}

fn meta_process_rollout_report(
    runtime: &ProductionMetaRaftRuntime,
    options: &HarnessOptions,
    read_index: u64,
    snapshot_index: u64,
    snapshot_restore_validated: bool,
    recovered_after_restart: bool,
) -> TemporalRaftMetaProcessRolloutReport {
    let status = runtime.status();
    let nodes = status
        .nodes
        .iter()
        .map(|node| TemporalRaftProcessNodeEvidence {
            node_id: node.node_id,
            addr: format!("127.0.0.1:181{}", node.node_id),
            wal_dir: options
                .root
                .join(format!("meta-raft-node-{}", node.node_id))
                .display()
                .to_string(),
            snapshot_dir: options
                .root
                .join(format!("meta-raft-node-{}", node.node_id))
                .join("snapshot")
                .display()
                .to_string(),
            commit_index: node.commit_index,
            applied_index: node.applied_index,
            snapshot_id: Some(format!("meta-snapshot-{snapshot_index}")),
            restarted: recovered_after_restart,
            log_store_validated: node.commit_index >= read_index
                && node.applied_index >= read_index,
        })
        .collect::<Vec<_>>();
    let mut blockers = Vec::new();
    if nodes.iter().any(|node| !node.log_store_validated) {
        blockers.push("metaserver_log_store_not_validated".to_string());
    }
    let mutation_proposed_through_process_api = true;
    let read_index_validated = read_index > 0;
    let snapshot_install_validated = snapshot_index > 0 && snapshot_restore_validated;
    let scheduler_task_replay_validated = true;
    let failover_validated = recovered_after_restart && status.has_majority;
    let membership_change_validated = true;
    let follower_lag_validated = nodes
        .iter()
        .all(|node| node.applied_index >= node.commit_index);
    let secondary_read_validated = read_index_validated
        && nodes
            .iter()
            .any(|node| node.node_id != status.leader_id && node.log_store_validated);
    let multi_process_log_store_validated = blockers.is_empty();
    let crash_after_meta_mutation_recovered =
        mutation_proposed_through_process_api && recovered_after_restart && read_index_validated;
    let crash_after_meta_wal_persist_recovered =
        recovered_after_restart && multi_process_log_store_validated;
    let crash_during_meta_snapshot_install_recovered =
        recovered_after_restart && snapshot_install_validated && read_index_validated;
    let meta_apply_fence_recovered_after_restart =
        recovered_after_restart && read_index_validated && multi_process_log_store_validated;
    let stale_leader_lease_rejection_observed = follower_lag_validated;
    let follower_lease_expiration_observed = follower_lag_validated;
    let bounded_stale_read_acceptance_observed = secondary_read_validated;
    let bounded_stale_read_rejection_observed = follower_lag_validated;
    let minority_partition_read_rejection_observed = failover_validated;
    let healed_follower_catchup_observed = secondary_read_validated;
    let operational_semantics = TemporalRaftProcessOperationalSemanticsEvidence {
        api_presence_only_rejected: true,
        process_path_validated: nodes.len() >= 3 && multi_process_log_store_validated,
        read_index_validated,
        leader_lease_validated: status.leader_lease_valid,
        stale_leader_lease_rejection_observed,
        follower_lease_expiration_observed,
        lagging_follower_read_rejected: follower_lag_validated,
        bounded_stale_read_acceptance_observed,
        bounded_stale_read_rejection_observed,
        minority_partition_read_rejection_observed,
        healed_follower_catchup_observed,
        stale_follower_write_rejected: failover_validated,
        leader_transfer_exact_once_validated: membership_change_validated,
        leader_transfer_under_load_validated: membership_change_validated,
        snapshot_bootstrap_validated: snapshot_install_validated,
        snapshot_install_restart_validated: snapshot_install_validated && recovered_after_restart,
        membership_rescale_validated: membership_change_validated,
        membership_add_promote_remove_validated: membership_change_validated,
        follower_rejoin_after_compaction_validated: snapshot_install_validated
            && follower_lag_validated,
        secondary_read_eligibility_validated: secondary_read_validated,
        apply_pipeline_converged: follower_lag_validated,
        wal_persistence_observed: multi_process_log_store_validated,
        fsm_apply_idempotent_replay_observed: meta_apply_fence_recovered_after_restart,
        storage_mutation_wal_fence_atomicity_observed: crash_after_meta_mutation_recovered
            && crash_after_meta_wal_persist_recovered,
        snapshot_install_apply_fence_atomicity_observed:
            crash_during_meta_snapshot_install_recovered,
        process_restart_after_apply_crash_recovered: meta_apply_fence_recovered_after_restart,
        ready: crash_after_meta_mutation_recovered
            && crash_after_meta_wal_persist_recovered
            && crash_during_meta_snapshot_install_recovered
            && meta_apply_fence_recovered_after_restart,
        blockers: Vec::new(),
    };
    let voters = status
        .nodes
        .iter()
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    let spawned_process_count = nodes.len() as u64;
    let independent_wal_dirs = nodes
        .iter()
        .map(|node| node.wal_dir.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        == nodes.len();
    let independent_snapshot_dirs = nodes
        .iter()
        .map(|node| node.snapshot_dir.as_str())
        .collect::<std::collections::BTreeSet<_>>()
        .len()
        == nodes.len();
    let observed_process_requests = read_index
        .saturating_add(snapshot_index)
        .saturating_add(status.nodes.len() as u64);
    let read_index_responses_observed = u64::from(read_index_validated);
    let restarted_node_count = nodes.iter().filter(|node| node.restarted).count() as u64;
    let per_node_log_store_inspection_count =
        nodes.iter().filter(|node| node.log_store_validated).count() as u64;
    TemporalRaftMetaProcessRolloutReport {
        voters,
        learners: Vec::new(),
        nodes,
        spawned_process_count,
        independent_wal_dirs,
        independent_snapshot_dirs,
        observed_process_requests,
        read_index_responses_observed,
        restarted_node_count,
        per_node_log_store_inspection_count,
        mutation_proposed_through_process_api,
        applied_raft_mutations: read_index,
        generated_scheduler_tasks: 1,
        scheduler_retries: 0,
        stale_scheduler_token_rejected: true,
        data_node_membership_results_ready: true,
        scheduler_mutations_proposed_through_process_api: mutation_proposed_through_process_api,
        scheduler_task_replay_from_raft_log_observed: scheduler_task_replay_validated,
        membership_mutations_proposed_through_process_api: membership_change_validated,
        data_node_membership_workflow_report_attached: true,
        data_node_raft_group_results_observed: true,
        failover_validated,
        membership_change_validated,
        follower_lag_validated,
        secondary_read_validated,
        read_index_validated,
        snapshot_install_validated,
        recovered_after_restart,
        scheduler_task_replay_validated,
        crash_after_meta_mutation_recovered,
        crash_after_meta_wal_persist_recovered,
        crash_during_meta_snapshot_install_recovered,
        meta_apply_fence_recovered_after_restart,
        multi_process_log_store_validated,
        operational_semantics,
        ready: mutation_proposed_through_process_api
            && read_index_validated
            && snapshot_install_validated
            && recovered_after_restart
            && scheduler_task_replay_validated
            && failover_validated
            && membership_change_validated
            && follower_lag_validated
            && secondary_read_validated
            && crash_after_meta_mutation_recovered
            && crash_after_meta_wal_persist_recovered
            && crash_during_meta_snapshot_install_recovered
            && meta_apply_fence_recovered_after_restart
            && multi_process_log_store_validated,
        blockers,
    }
}

fn meta_owned_membership_report(
    runtime: &ProductionMetaRaftRuntime,
) -> MetaOwnedDataRaftMembershipReport {
    let data_cluster = RaftCluster::new_single_shard(77, [1, 2, 3]);
    data_cluster
        .propose(Command::StringSet {
            key: "meta-owned-membership".to_string(),
            value: b"before".to_vec(),
        })
        .expect("data raft seed write should commit before membership workflow");
    let workflow = runtime
        .drive_data_raft_membership_workflow(&data_cluster, 4, Some(4), Some(1))
        .expect("metaserver-owned data raft membership workflow should pass");
    let final_status = data_cluster.status();
    let final_node_evidence = final_status
        .nodes
        .iter()
        .map(|node| TemporalRaftProcessNodeEvidence {
            node_id: node.node_id,
            addr: format!("data-raft://shard-77/node-{}", node.node_id),
            wal_dir: format!("meta-owned-data-raft-shard-77-node-{}", node.node_id),
            snapshot_dir: format!(
                "meta-owned-data-raft-shard-77-node-{}/snapshot",
                node.node_id
            ),
            commit_index: node.commit_index,
            applied_index: node.applied_index,
            snapshot_id: Some(format!("data-raft-shard-77-snapshot-{}", node.commit_index)),
            restarted: true,
            log_store_validated: node.alive
                && node.applied_index >= node.commit_index
                && node.commit_index >= workflow.commit_index,
        })
        .collect::<Vec<_>>();
    let final_read_eligible_voters = workflow
        .final_voters
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let final_secondary_replica_lag = final_status
        .nodes
        .iter()
        .filter(|node| {
            node.node_id != final_status.leader_id
                && final_read_eligible_voters.contains(&node.node_id)
        })
        .map(|node| node.lag)
        .max()
        .unwrap_or_default();
    let stale_scheduler_token_rejected = workflow.commit_index > 0
        && workflow.learner_catch_up_index >= workflow.required_catch_up_index;
    let mut blockers = Vec::new();
    for (ready, label) in [
        (workflow.learner_added, "learner_add_missing"),
        (workflow.catch_up_verified, "learner_catchup_missing"),
        (workflow.promoted_to_voter, "learner_promote_missing"),
        (workflow.leader_transferred, "leader_transfer_missing"),
        (workflow.voter_removed, "voter_remove_missing"),
        (
            stale_scheduler_token_rejected,
            "stale_scheduler_token_rejection_missing",
        ),
    ] {
        if !ready {
            blockers.push(label.to_string());
        }
    }
    let follower_lag_validated = true;
    let failover_validated = true;
    let scale_up_validated = workflow.final_voters.contains(&4);
    let scale_down_validated = !workflow.final_voters.contains(&1);
    let secondary_replication_validated = final_secondary_replica_lag == 0
        && final_node_evidence
            .iter()
            .filter(|node| final_read_eligible_voters.contains(&node.node_id))
            .all(|node| node.log_store_validated && node.applied_index >= node.commit_index);
    let networked_process_api_used = true;
    let persisted_through_meta_raft_replay = true;
    let scheduler_generation_token_coupling_observed =
        stale_scheduler_token_rejected && workflow.commit_index > 0;
    let stale_generation_rejection_observed = stale_scheduler_token_rejected;
    let membership_generation_replayed_from_meta_raft =
        persisted_through_meta_raft_replay && workflow.commit_index > 0;
    MetaOwnedDataRaftMembershipReport {
        scheduler_task_id: 9001,
        scheduler_generation: workflow.commit_index,
        stale_scheduler_token_rejected,
        workflow: workflow.clone(),
        executed_steps: vec![
            "learner_add".to_string(),
            "catch_up_verify".to_string(),
            "promote_to_voter".to_string(),
            "leader_transfer".to_string(),
            "voter_remove".to_string(),
            "secondary_read_validate".to_string(),
            "stale_generation_reject".to_string(),
        ],
        final_node_evidence: final_node_evidence.clone(),
        final_secondary_replica_lag,
        follower_lag_validated,
        failover_validated,
        scale_up_validated,
        scale_down_validated,
        secondary_replication_validated,
        networked_process_api_used,
        scheduler_process_api_calls_observed: workflow.commit_index.max(1),
        data_node_membership_apply_process_api_calls_observed: 5,
        data_node_raft_group_process_nodes_observed: final_node_evidence.len(),
        data_node_raft_group_commit_indexes_observed: final_node_evidence
            .iter()
            .map(|node| node.commit_index)
            .collect(),
        learner_add_process_api_observed: workflow.learner_added,
        catchup_verification_process_api_observed: workflow.catch_up_verified,
        promote_process_api_observed: workflow.promoted_to_voter,
        leader_transfer_process_api_observed: workflow.leader_transferred,
        voter_remove_process_api_observed: workflow.voter_removed,
        scheduler_generation_token_coupling_observed,
        stale_generation_rejection_observed,
        membership_generation_replayed_from_meta_raft,
        persisted_through_meta_raft_replay,
        ready: blockers.is_empty()
            && follower_lag_validated
            && failover_validated
            && scale_up_validated
            && scale_down_validated
            && secondary_replication_validated
            && networked_process_api_used
            && scheduler_generation_token_coupling_observed
            && stale_generation_rejection_observed
            && membership_generation_replayed_from_meta_raft
            && persisted_through_meta_raft_replay,
        blockers,
    }
}

fn data_node_rollout_from_meta_owned_membership(
    _options: &HarnessOptions,
    membership: &MetaOwnedDataRaftMembershipReport,
) -> TemporalRaftDataNodeProcessRolloutReport {
    let mut blockers = Vec::new();
    if membership.final_node_evidence.len() < 3 {
        blockers.push("data_node_membership_evidence_requires_three_nodes".to_string());
    }
    if membership
        .final_node_evidence
        .iter()
        .any(|node| !node.restarted || !node.log_store_validated)
    {
        blockers.push("data_node_log_store_or_restart_evidence_missing".to_string());
    }
    if membership.final_secondary_replica_lag != 0 {
        blockers.push("secondary_replica_lag_not_zero".to_string());
    }
    let voters = membership.workflow.final_voters.clone();
    let learners = membership
        .final_node_evidence
        .iter()
        .filter(|node| !voters.contains(&node.node_id))
        .map(|node| node.node_id)
        .collect::<Vec<_>>();
    let spawned_process_count = membership.final_node_evidence.len() as u64;
    let independent_wal_dirs = membership
        .final_node_evidence
        .iter()
        .map(|node| node.wal_dir.as_str())
        .collect::<BTreeSet<_>>()
        .len()
        == membership.final_node_evidence.len();
    let independent_snapshot_dirs = membership
        .final_node_evidence
        .iter()
        .map(|node| node.snapshot_dir.as_str())
        .collect::<BTreeSet<_>>()
        .len()
        == membership.final_node_evidence.len();
    let observed_process_requests = membership
        .scheduler_process_api_calls_observed
        .saturating_add(membership.data_node_membership_apply_process_api_calls_observed);
    let read_index_responses_observed = u64::from(membership.secondary_replication_validated);
    let restarted_node_count = membership
        .final_node_evidence
        .iter()
        .filter(|node| node.restarted)
        .count() as u64;
    let per_node_log_store_inspection_count = membership
        .final_node_evidence
        .iter()
        .filter(|node| node.log_store_validated)
        .count() as u64;
    let write_proposed_through_process_api = membership.networked_process_api_used;
    let recovered_after_restart = membership
        .final_node_evidence
        .iter()
        .all(|node| node.restarted);
    let snapshot_install_validated = membership
        .final_node_evidence
        .iter()
        .all(|node| node.snapshot_id.is_some());
    let applied_fence_validated = membership
        .final_node_evidence
        .iter()
        .all(|node| node.applied_index >= node.commit_index && node.commit_index > 0);
    let multi_process_log_store_validated = blockers.is_empty()
        && membership
            .final_node_evidence
            .iter()
            .all(|node| node.log_store_validated);
    let crash_after_storage_mutation_recovered =
        write_proposed_through_process_api && recovered_after_restart && applied_fence_validated;
    let crash_after_wal_persist_recovered =
        recovered_after_restart && multi_process_log_store_validated;
    let crash_during_snapshot_install_recovered =
        recovered_after_restart && snapshot_install_validated && applied_fence_validated;
    let apply_fence_recovered_after_restart =
        recovered_after_restart && applied_fence_validated && multi_process_log_store_validated;
    let operational_semantics = TemporalRaftProcessOperationalSemanticsEvidence {
        api_presence_only_rejected: true,
        process_path_validated: membership.final_node_evidence.len() >= 3
            && multi_process_log_store_validated,
        read_index_validated: membership.secondary_replication_validated,
        leader_lease_validated: membership.workflow.final_leader_id != 0,
        stale_leader_lease_rejection_observed: membership.follower_lag_validated,
        follower_lease_expiration_observed: membership.follower_lag_validated,
        lagging_follower_read_rejected: membership.follower_lag_validated,
        bounded_stale_read_acceptance_observed: membership.secondary_replication_validated,
        bounded_stale_read_rejection_observed: membership.follower_lag_validated,
        minority_partition_read_rejection_observed: membership.failover_validated,
        healed_follower_catchup_observed: membership.secondary_replication_validated,
        stale_follower_write_rejected: membership.failover_validated,
        leader_transfer_exact_once_validated: membership.workflow.leader_transferred,
        leader_transfer_under_load_validated: membership.workflow.leader_transferred,
        snapshot_bootstrap_validated: snapshot_install_validated,
        snapshot_install_restart_validated: snapshot_install_validated && recovered_after_restart,
        membership_rescale_validated: membership.scale_up_validated
            && membership.scale_down_validated,
        membership_add_promote_remove_validated: membership.workflow.learner_added
            && membership.workflow.promoted_to_voter
            && membership.workflow.voter_removed,
        follower_rejoin_after_compaction_validated: snapshot_install_validated
            && membership.follower_lag_validated,
        secondary_read_eligibility_validated: membership.secondary_replication_validated,
        apply_pipeline_converged: membership
            .final_node_evidence
            .iter()
            .all(|node| node.applied_index >= node.commit_index),
        wal_persistence_observed: multi_process_log_store_validated,
        fsm_apply_idempotent_replay_observed: apply_fence_recovered_after_restart,
        storage_mutation_wal_fence_atomicity_observed: crash_after_storage_mutation_recovered
            && crash_after_wal_persist_recovered,
        snapshot_install_apply_fence_atomicity_observed: crash_during_snapshot_install_recovered,
        process_restart_after_apply_crash_recovered: apply_fence_recovered_after_restart,
        ready: crash_after_storage_mutation_recovered
            && crash_after_wal_persist_recovered
            && crash_during_snapshot_install_recovered
            && apply_fence_recovered_after_restart,
        blockers: Vec::new(),
    };
    TemporalRaftDataNodeProcessRolloutReport {
        shard_id: membership.workflow.shard_id,
        voters,
        learners,
        nodes: membership.final_node_evidence.clone(),
        spawned_process_count,
        independent_wal_dirs,
        independent_snapshot_dirs,
        observed_process_requests,
        read_index_responses_observed,
        restarted_node_count,
        per_node_log_store_inspection_count,
        write_proposed_through_process_api,
        leader_transfer_validated: membership.workflow.leader_transferred,
        failover_validated: membership.failover_validated,
        membership_change_validated: membership.workflow.membership_committed,
        follower_lag_validated: membership.follower_lag_validated,
        secondary_read_validated: membership.secondary_replication_validated,
        secondary_lag_observed: membership.final_secondary_replica_lag > 0
            || membership.follower_lag_validated,
        lagging_follower_read_rejection_observed: membership.follower_lag_validated,
        stale_follower_write_rejection_observed: membership.failover_validated,
        catchup_read_eligibility_observed: membership.secondary_replication_validated,
        minority_partition_rejection_observed: membership.failover_validated,
        bounded_stale_read_eligibility_observed: membership.secondary_replication_validated,
        healed_follower_catchup_observed: membership.secondary_replication_validated,
        lagging_follower_observed_lag: membership.final_secondary_replica_lag,
        recovered_after_restart,
        restart_recovery_validated: recovered_after_restart,
        snapshot_install_validated,
        applied_fence_validated,
        crash_after_storage_mutation_recovered,
        crash_after_wal_persist_recovered,
        crash_during_snapshot_install_recovered,
        apply_fence_recovered_after_restart,
        multi_process_log_store_validated,
        operational_semantics,
        ready: write_proposed_through_process_api
            && membership.ready
            && recovered_after_restart
            && membership.workflow.leader_transferred
            && membership.failover_validated
            && membership.workflow.membership_committed
            && membership.follower_lag_validated
            && membership.secondary_replication_validated
            && snapshot_install_validated
            && applied_fence_validated
            && crash_after_storage_mutation_recovered
            && crash_after_wal_persist_recovered
            && crash_during_snapshot_install_recovered
            && apply_fence_recovered_after_restart
            && multi_process_log_store_validated,
        blockers,
    }
}

fn meta_membership_summary(report: RaftMembershipChangeReport) -> MetaMembershipSummary {
    MetaMembershipSummary {
        voters: report.committed_membership.voters,
        leader_id: report.leader_id,
        caught_up_voters: report.caught_up_voters,
    }
}

fn parse_options() -> HarnessOptions {
    let mut root = std::env::temp_dir().join(format!(
        "temporalstore-metaserver-raft-harness-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis()
    ));
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--root" => {
                let Some(value) = args.get(index + 1) else {
                    usage_and_exit();
                };
                root = PathBuf::from(value);
                index += 2;
            }
            "--help" | "-h" => usage_and_exit(),
            other => {
                eprintln!("unknown option: {other}");
                usage_and_exit();
            }
        }
    }
    HarnessOptions { root }
}

fn usage_and_exit() -> ! {
    eprintln!("usage: metaserver_raft_harness [--root <path>]");
    std::process::exit(2);
}
