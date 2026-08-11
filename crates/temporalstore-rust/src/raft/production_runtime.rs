// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Production Raft/Meta-Raft runtime + timer-handle types and impls, split from raft.rs.
use super::*;

impl ProductionRaftRuntimeOptions {
    pub fn validate(&self) -> Result<(), RaftError> {
        self.config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        if self.nodes.is_empty() {
            return Err(RaftError::InvalidConfig(
                "production raft requires at least one node".to_string(),
            ));
        }
        let mut node_ids = BTreeSet::new();
        let mut node_addrs = BTreeSet::new();
        for node in &self.nodes {
            if node.node_id == 0 {
                return Err(RaftError::InvalidConfig(
                    "production raft node_id must be non-zero".to_string(),
                ));
            }
            if node.addr.trim().is_empty() {
                return Err(RaftError::InvalidConfig(format!(
                    "production raft node {} requires a non-empty addr",
                    node.node_id
                )));
            }
            if !node_ids.insert(node.node_id) {
                return Err(RaftError::InvalidConfig(format!(
                    "production raft node_id {} is duplicated",
                    node.node_id
                )));
            }
            if !node_addrs.insert(node.addr.trim().to_string()) {
                return Err(RaftError::InvalidConfig(format!(
                    "production raft addr {} is duplicated",
                    node.addr
                )));
            }
        }
        if !self
            .nodes
            .iter()
            .any(|node| node.node_id == self.local_node_id)
        {
            return Err(RaftError::InvalidConfig(
                "local_node_id must be present in production raft nodes".to_string(),
            ));
        }
        if self.wal_dir.trim().is_empty() {
            return Err(RaftError::InvalidConfig(
                "production raft requires wal_dir".to_string(),
            ));
        }
        if self.heartbeat_interval_ms == 0 || self.election_tick_ms == 0 {
            return Err(RaftError::InvalidConfig(
                "production raft heartbeat/election intervals must be non-zero".to_string(),
            ));
        }
        if self.max_catchup_entries_per_heartbeat == 0 {
            return Err(RaftError::InvalidConfig(
                "production raft max_catchup_entries_per_heartbeat must be non-zero".to_string(),
            ));
        }
        self.security
            .validate(self.allow_plaintext_for_local_chaos)?;
        Ok(())
    }

    pub(super) fn peer_map(&self) -> BTreeMap<RaftNodeId, String> {
        self.nodes
            .iter()
            .filter(|node| node.node_id != self.local_node_id)
            .map(|node| (node.node_id, node.addr.clone()))
            .collect()
    }

    pub(super) fn node_addr(&self, node_id: RaftNodeId) -> Option<&str> {
        self.nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .map(|node| node.addr.as_str())
    }

    pub(super) fn voter_ids(&self) -> Vec<RaftNodeId> {
        self.nodes.iter().map(|node| node.node_id).collect()
    }
}

#[derive(Debug, Clone)]
pub struct ProductionRaftRuntime {
    options: ProductionRaftRuntimeOptions,
    cluster: RaftCluster,
}

impl ProductionRaftRuntime {
    pub fn start(options: ProductionRaftRuntimeOptions) -> Result<Self, RaftError> {
        options.validate()?;
        let cluster = RaftCluster::restore_single_shard_from_wal(
            &options.wal_dir,
            options.shard_id,
            options.voter_ids(),
            options.config.clone(),
        )?;
        Ok(Self { options, cluster })
    }

    pub fn cluster(&self) -> RaftCluster {
        self.cluster.clone()
    }

    pub fn peer_auth_token(&self) -> Option<&str> {
        self.options.security.auth_token.as_deref()
    }

    pub fn local_node_id(&self) -> RaftNodeId {
        self.options.local_node_id
    }

    pub fn local_apply_health(&self, max_allowed_apply_lag: u64) -> RaftApplyHealth {
        self.cluster
            .observer_apply_health(self.options.local_node_id, max_allowed_apply_lag)
    }

    pub fn data_node_atomic_durability_report(&self) -> RaftDataNodeAtomicDurabilityReport {
        let status = self.cluster.status();
        let local_status = status
            .nodes
            .iter()
            .find(|node| node.node_id == self.options.local_node_id);
        let wal_record = self
            .cluster
            .wal_records()
            .into_iter()
            .find(|(node_id, _)| *node_id == self.options.local_node_id)
            .map(|(_, record)| record);

        let mut blockers = Vec::new();
        let mut commit_index = 0;
        let mut applied_index = 0;
        if let Some(local_status) = local_status {
            commit_index = local_status.commit_index;
            applied_index = local_status.applied_index;
            if local_status.applied_index < local_status.commit_index {
                blockers.push("local_applied_index_lags_commit_index".to_string());
            }
        } else {
            blockers.push("local_node_status_missing".to_string());
        }

        let mut wal_commit_index = 0;
        let mut fence = RaftStorageApplyFence::default();
        let mut storage_apply_fence_valid = false;
        if let Some(record) = wal_record {
            wal_commit_index = record.hard_state.commit_index;
            fence = record.storage_apply_fence.clone();
            match validate_raft_storage_apply_fence(&record) {
                Ok(()) => storage_apply_fence_valid = true,
                Err(err) => blockers.push(format!("storage_apply_fence_invalid:{err}")),
            }
            if record.hard_state.commit_index != commit_index {
                blockers.push("wal_commit_index_mismatch".to_string());
            }
            if record.storage_apply_fence.applied_index != applied_index {
                blockers.push("storage_fence_applied_index_mismatch".to_string());
            }
        } else {
            blockers.push("local_wal_record_missing".to_string());
        }

        let storage_mutation_atomic_commit_present = storage_apply_fence_valid
            && wal_commit_index == commit_index
            && fence.applied_index == applied_index;
        let snapshot_install_atomic_commit_present =
            storage_apply_fence_valid && fence.storage_epoch >= fence.applied_index;
        if !storage_mutation_atomic_commit_present {
            blockers.push("storage_mutation_atomic_commit_missing".to_string());
        }
        if !snapshot_install_atomic_commit_present {
            blockers.push("snapshot_install_atomic_commit_missing".to_string());
        }

        RaftDataNodeAtomicDurabilityReport {
            node_id: self.options.local_node_id,
            shard_id: self.options.shard_id,
            commit_index,
            applied_index,
            wal_commit_index,
            fence_committed_index: fence.committed_index,
            fence_applied_index: fence.applied_index,
            storage_epoch: fence.storage_epoch,
            snapshot_id: fence.snapshot_id,
            storage_apply_fence_valid,
            storage_mutation_atomic_commit_present,
            snapshot_install_atomic_commit_present,
            ready: blockers.is_empty(),
            blockers,
        }
    }

    pub fn transport(&self) -> RaftRpcRuntime<AuthenticatedRaftTransport<HttpRaftTransport>> {
        let http_options = HttpRequestOptions {
            connect_timeout_ms: self.options.rpc.deadline_ms,
            io_timeout_ms: self.options.rpc.deadline_ms,
            max_retries: self.options.rpc.max_retries,
        };
        let http = HttpRaftTransport::with_options(self.options.peer_map(), http_options);
        let auth = AuthenticatedRaftTransport::new(
            http,
            self.options
                .security
                .auth_token
                .clone()
                .expect("validated production raft auth token"),
        );
        RaftRpcRuntime::with_auth_token(
            auth,
            self.options.rpc,
            self.options.security.auth_token.clone(),
        )
    }

    pub fn propose(&self, command: Command) -> Result<CommandResponse, RaftError> {
        if self.cluster.leader_id() != self.options.local_node_id {
            return Err(RaftError::NotLeader {
                node_id: self.options.local_node_id,
            });
        }
        let transport = self.transport();
        self.cluster.propose_distributed(command, &transport)
    }

    pub fn apply_membership_change_safely(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        self.cluster.apply_membership_change_safely(new_voters)
    }

    pub fn transfer_leader(&self, target_id: RaftNodeId) -> Result<(), RaftError> {
        if self.cluster.leader_id() == target_id {
            return Ok(());
        }
        if self.cluster.leader_id() != self.options.local_node_id {
            return Err(RaftError::NotLeader {
                node_id: self.options.local_node_id,
            });
        }
        let transport = self.transport();
        let append = self.cluster.build_append_entries_request(target_id)?;
        match transport.append_entries(append) {
            Ok(response) if response.success => {
                let _ = self
                    .cluster
                    .record_append_entries_response(target_id, &response);
            }
            Ok(response) => {
                let _ = self
                    .cluster
                    .record_append_entries_response(target_id, &response);
                let snapshot = self.cluster.build_install_snapshot_request(target_id)?;
                let response = transport.install_snapshot(snapshot)?;
                if !response.success {
                    return Err(RaftError::Transport(format!(
                        "snapshot install rejected by node {target_id}: {:?}",
                        response.reject_reason
                    )));
                }
            }
            _ => {
                let snapshot = self.cluster.build_install_snapshot_request(target_id)?;
                let response = transport.install_snapshot(snapshot)?;
                if !response.success {
                    return Err(RaftError::Transport(format!(
                        "snapshot install rejected by node {target_id}: {:?}",
                        response.reject_reason
                    )));
                }
            }
        }
        self.cluster.catch_up(target_id)?;
        self.cluster.transfer_leader(target_id)?;
        if target_id != self.options.local_node_id {
            let addr = self
                .options
                .node_addr(target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            let status: Status = post_json_with_options(
                addr,
                "/raft/control/accept_leadership",
                &RaftControlLeadershipRequest { node_id: target_id },
                HttpRequestOptions {
                    connect_timeout_ms: self.options.rpc.deadline_ms,
                    io_timeout_ms: self.options.rpc.deadline_ms,
                    max_retries: self.options.rpc.max_retries,
                },
            )
            .map_err(|err| RaftError::Transport(err.to_string()))?;
            if !status.ok {
                return Err(RaftError::Transport(status.message));
            }
        }
        Ok(())
    }

    pub fn read_local(
        &self,
        node_id: RaftNodeId,
        command: Command,
    ) -> Result<CommandResponse, RaftError> {
        self.cluster.read_index(node_id)?;
        self.cluster.read_from_replica(node_id, command)
    }

    pub fn wait_for_applied_index(
        &self,
        node_id: RaftNodeId,
        index: u64,
        timeout_ms: u64,
    ) -> Result<(), RaftError> {
        self.cluster
            .wait_for_applied_index(node_id, index, timeout_ms)
    }

    pub fn start_timer_loop(&self) -> ProductionRaftTimerHandle {
        let cluster = self.cluster.clone();
        let local_node_id = self.options.local_node_id;
        let heartbeat_interval = Duration::from_millis(self.options.heartbeat_interval_ms);
        let election_tick = Duration::from_millis(self.options.election_tick_ms);
        let max_catchup_entries_per_heartbeat = self.options.max_catchup_entries_per_heartbeat;
        let peer_map = self.options.peer_map();
        let peer_ids = peer_map.keys().copied().collect::<Vec<_>>();
        let http_options = HttpRequestOptions {
            connect_timeout_ms: self.options.rpc.deadline_ms,
            io_timeout_ms: self.options.rpc.deadline_ms,
            max_retries: self.options.rpc.max_retries,
        };
        let rpc_options = self.options.rpc;
        let auth_token = self
            .options
            .security
            .auth_token
            .clone()
            .expect("validated production raft auth token");
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut last_heartbeat = InstantCompat::now();
            while !stop_thread.load(Ordering::SeqCst) {
                let _ = cluster.tick_election();
                if last_heartbeat.elapsed() >= heartbeat_interval {
                    if cluster.leader_id() == local_node_id {
                        let transport = RaftRpcRuntime::with_auth_token(
                            AuthenticatedRaftTransport::new(
                                HttpRaftTransport::with_options(peer_map.clone(), http_options),
                                auth_token.clone(),
                            ),
                            rpc_options,
                            Some(auth_token.clone()),
                        );
                        let mut sent = 0;
                        for target_id in &peer_ids {
                            if sent >= max_catchup_entries_per_heartbeat {
                                break;
                            }
                            let Ok(request) = cluster.build_append_entries_request(*target_id)
                            else {
                                continue;
                            };
                            let entry_count = request.entries.len() as u64;
                            if let Ok(response) = transport.append_entries(request) {
                                let success = response.success;
                                let _ =
                                    cluster.record_append_entries_response(*target_id, &response);
                                if success {
                                    sent += entry_count.max(1);
                                }
                            }
                        }
                    }
                    last_heartbeat = InstantCompat::now();
                }
                thread::sleep(election_tick);
            }
        });
        ProductionRaftTimerHandle { stop, handle }
    }

    pub fn status(&self) -> RaftClusterStatus {
        self.cluster.status()
    }

    pub fn validate_ready(&self) -> Result<(), RaftError> {
        self.options.validate()?;
        let status = self.cluster.status();
        if !status.has_majority {
            return Err(RaftError::NoMajority {
                live: status.live_voters,
                required: status.majority,
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ProductionRaftTimerHandle {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

impl ProductionRaftTimerHandle {
    pub fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        let _ = self.handle.join();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProductionMetaRaftRuntimeOptions {
    pub engine: ProductionRaftEngineKind,
    pub local_node_id: RaftNodeId,
    pub nodes: Vec<ProductionRaftNode>,
    pub config: RaftConfig,
    pub heartbeat_interval_ms: u64,
    pub election_tick_ms: u64,
    pub failure_detector_interval_ms: u64,
    pub stale_server_after_ms: u64,
}

impl ProductionMetaRaftRuntimeOptions {
    pub fn validate(&self) -> Result<(), RaftError> {
        self.config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        if self.nodes.is_empty() {
            return Err(RaftError::InvalidConfig(
                "production meta raft requires at least one node".to_string(),
            ));
        }
        if !self
            .nodes
            .iter()
            .any(|node| node.node_id == self.local_node_id)
        {
            return Err(RaftError::InvalidConfig(
                "local_node_id must be present in production meta raft nodes".to_string(),
            ));
        }
        if self.heartbeat_interval_ms == 0
            || self.election_tick_ms == 0
            || self.failure_detector_interval_ms == 0
        {
            return Err(RaftError::InvalidConfig(
                "production meta raft intervals must be non-zero".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn voter_ids(&self) -> Vec<RaftNodeId> {
        self.nodes.iter().map(|node| node.node_id).collect()
    }
}

#[derive(Debug, Clone)]
pub struct ProductionMetaRaftRuntime {
    options: ProductionMetaRaftRuntimeOptions,
    cluster: MetaRaftCluster,
}

impl ProductionMetaRaftRuntime {
    pub fn start(options: ProductionMetaRaftRuntimeOptions) -> Result<Self, RaftError> {
        options.validate()?;
        let cluster =
            MetaRaftCluster::new_with_config(options.voter_ids(), options.config.clone())?;
        Ok(Self { options, cluster })
    }

    pub fn cluster(&self) -> MetaRaftCluster {
        self.cluster.clone()
    }

    pub fn status(&self) -> RaftClusterStatus {
        self.cluster.status()
    }

    pub fn validate_ready(&self) -> Result<(), RaftError> {
        self.options.validate()?;
        let status = self.status();
        if !status.has_majority {
            return Err(RaftError::NoMajority {
                live: status.live_voters,
                required: status.majority,
            });
        }
        Ok(())
    }

    pub fn propose(&self, command: MetaCommand) -> Result<(), RaftError> {
        self.cluster.propose(command)
    }

    pub fn propose_mutation(&self, mutation: MetaMutation) -> Result<Status, RaftError> {
        self.cluster.propose_mutation(mutation)
    }

    pub fn list_membership(&self) -> Vec<RaftNodeId> {
        self.status()
            .nodes
            .into_iter()
            .filter(|node| node.replica_role.participates_in_quorum())
            .map(|node| node.node_id)
            .collect()
    }

    pub fn add_node(
        &self,
        node_id: RaftNodeId,
        role: RaftReplicaRole,
    ) -> Result<RaftScaleChangeReport, RaftError> {
        if !role.participates_in_quorum() || matches!(role, RaftReplicaRole::Witness) {
            return Err(RaftError::InvalidConfig(format!(
                "metaserver raft currently supports voter membership only, requested {role:?}"
            )));
        }
        self.cluster.add_node_safely(node_id)
    }

    pub fn remove_node(&self, node_id: RaftNodeId) -> Result<RaftScaleChangeReport, RaftError> {
        self.cluster.remove_node_safely(node_id)
    }

    pub fn apply_membership(
        &self,
        new_voters: impl IntoIterator<Item = RaftNodeId>,
    ) -> Result<RaftMembershipChangeReport, RaftError> {
        self.cluster.apply_membership_change_safely(new_voters)
    }

    pub fn drive_data_raft_membership_workflow(
        &self,
        data_cluster: &RaftCluster,
        learner_id: RaftNodeId,
        requested_leader_id: Option<RaftNodeId>,
        remove_voter_id: Option<RaftNodeId>,
    ) -> Result<MetaDataRaftMembershipWorkflowReport, RaftError> {
        self.validate_ready()?;
        let shard_id = data_cluster.shard_id();
        let initial_status = data_cluster.status();
        let initial_voters = initial_status
            .nodes
            .iter()
            .filter(|node| node.replica_role.participates_in_quorum())
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        let required_catch_up_index = initial_status.commit_index;
        let mut learner_added = false;
        if data_cluster.local_status(learner_id).is_err() {
            data_cluster.add_node_with_role(learner_id, RaftReplicaRole::Learner)?;
            learner_added = true;
        }

        data_cluster.catch_up(learner_id)?;
        let learner_status = data_cluster.local_status(learner_id)?;
        let catch_up_verified = learner_status.lag == 0;
        let learner_catch_up_index = learner_status.commit_index;
        if !catch_up_verified {
            return Err(RaftError::ReplicaLagging {
                replica_id: learner_id,
                replica_commit_index: learner_status.commit_index,
                leader_commit_index: data_cluster.status().commit_index,
            });
        }

        let mut target_voters = data_cluster.membership().voters;
        if !target_voters.contains(&learner_id) {
            target_voters.push(learner_id);
        }
        target_voters.sort_unstable();
        target_voters.dedup();
        data_cluster.begin_joint_consensus(target_voters)?;
        data_cluster.promote_learner_to_voter(learner_id)?;
        if let Err(err) = data_cluster.catch_up_live_followers() {
            let _ = data_cluster.abort_joint_consensus();
            return Err(err);
        }
        let committed_membership = match data_cluster.commit_joint_consensus() {
            Ok(membership) => membership,
            Err(err) => {
                let _ = data_cluster.abort_joint_consensus();
                return Err(err);
            }
        };
        let voters_after_promote = committed_membership.voters.clone();

        let mut leader_transferred = false;
        if let Some(target_leader_id) = requested_leader_id {
            data_cluster.transfer_leader(target_leader_id)?;
            leader_transferred = data_cluster.leader_id() == target_leader_id;
        }

        let mut voter_removed = false;
        if let Some(remove_voter_id) = remove_voter_id {
            data_cluster.remove_node_safely(remove_voter_id)?;
            voter_removed = true;
        }

        let final_status = data_cluster.status();
        let final_voters = final_status
            .nodes
            .iter()
            .filter(|node| node.replica_role.participates_in_quorum())
            .map(|node| node.node_id)
            .collect::<Vec<_>>();
        Ok(MetaDataRaftMembershipWorkflowReport {
            shard_id,
            learner_id,
            removed_voter_id: remove_voter_id,
            requested_leader_id,
            initial_voters,
            learner_added,
            catch_up_verified,
            learner_catch_up_index,
            required_catch_up_index,
            promoted_to_voter: true,
            membership_committed: !committed_membership.voters.is_empty(),
            voters_after_promote,
            leader_transferred,
            voter_removed,
            final_leader_id: final_status.leader_id,
            final_voters,
            commit_index: final_status.commit_index,
        })
    }

    pub fn trigger_snapshot(&self) -> Result<MetaRaftSnapshot, RaftError> {
        self.cluster.create_snapshot()
    }

    pub fn wait_for_log_applied(&self) -> Result<ReadIndexResponse, RaftError> {
        self.cluster.read_index(self.options.local_node_id)
    }

    pub fn read_index(&self, node_id: RaftNodeId) -> Result<ReadIndexResponse, RaftError> {
        self.cluster.read_index(node_id)
    }

    pub fn transfer_leader(&self, node_id: RaftNodeId) -> Result<(), RaftError> {
        self.cluster.transfer_leader(node_id)
    }

    pub fn start_timer_loop(&self) -> ProductionRaftTimerHandle {
        let cluster = self.cluster.clone();
        let heartbeat_interval = Duration::from_millis(self.options.heartbeat_interval_ms);
        let election_tick = Duration::from_millis(self.options.election_tick_ms);
        let failure_detector_interval =
            Duration::from_millis(self.options.failure_detector_interval_ms);
        let stale_server_after_ms = self.options.stale_server_after_ms;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let mut last_heartbeat = InstantCompat::now();
            let mut last_failure_detector = InstantCompat::now();
            while !stop_thread.load(Ordering::SeqCst) {
                if last_heartbeat.elapsed() >= heartbeat_interval {
                    let _ = cluster.failover_primary();
                    let _ = cluster.catch_up_live_followers();
                    last_heartbeat = InstantCompat::now();
                }
                if stale_server_after_ms > 0
                    && last_failure_detector.elapsed() >= failure_detector_interval
                {
                    let _ = cluster.freeze_stale_servers(stale_server_after_ms);
                    last_failure_detector = InstantCompat::now();
                }
                thread::sleep(election_tick);
            }
        });
        ProductionRaftTimerHandle { stop, handle }
    }
}
