use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::control::MembershipUpdateRequest;
use crate::raft::RaftNodeId;
use crate::types::{ShardId, Status};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardRole {
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardReplicaState {
    Creating,
    Loading,
    Normal,
    Freezing,
    Frozen,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardReplica {
    pub shard_id: ShardId,
    pub replica_id: u64,
    pub node_id: RaftNodeId,
    pub role: ShardRole,
    pub state: ShardReplicaState,
    pub load_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMovePlan {
    pub shard_id: ShardId,
    pub source_replica_id: u64,
    pub source_node_id: RaftNodeId,
    pub target_replica_id: u64,
    pub target_node_id: RaftNodeId,
    pub load_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RebalanceStep {
    LoadTarget {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
        load_version: u64,
    },
    ReloadTarget {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
        load_version: u64,
    },
    UpdateMembership {
        shard_id: ShardId,
        active_replica_ids: Vec<u64>,
        primary_replica_id: u64,
        membership_version: u64,
    },
    FreezeSource {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
    },
    UnloadSource {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdateTaskOptions {
    pub exclude_self: bool,
    pub success_threshold: usize,
    pub submit_fsm: bool,
}

impl Default for MembershipUpdateTaskOptions {
    fn default() -> Self {
        Self {
            exclude_self: true,
            success_threshold: 0,
            submit_fsm: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipUpdatePeerStatus {
    Ok,
    NotFound,
    Failed { code: String, message: String },
}

impl MembershipUpdatePeerStatus {
    pub fn from_status(status: Status) -> Self {
        if status.ok {
            Self::Ok
        } else if status.code == "not_found" {
            Self::NotFound
        } else {
            Self::Failed {
                code: status.code,
                message: status.message,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdatePeerRequest {
    pub replica_id: u64,
    pub node_id: RaftNodeId,
    pub request: MembershipUpdateRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdateTaskPlan {
    pub shard_id: ShardId,
    pub self_replica_id: u64,
    pub active_replica_ids: Vec<u64>,
    pub primary_replica_id: u64,
    pub membership_version: u64,
    pub requests: Vec<MembershipUpdatePeerRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdateTaskReport {
    pub plan: MembershipUpdateTaskPlan,
    pub success_count: usize,
    pub not_found_count: usize,
    pub failed_count: usize,
    pub success_threshold: usize,
    pub accepted: bool,
    pub should_submit_fsm: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulerTaskResult {
    Ok,
    RetryLater,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulerTaskKind {
    UpdateMembership(MembershipUpdateTaskPlan),
    RebalanceStep(RebalanceStep),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerTask {
    pub id: u64,
    pub priority: i32,
    pub create_time_ms: u64,
    pub next_run_time_ms: u64,
    pub last_run_time_ms: u64,
    pub retry_times: u64,
    pub kind: SchedulerTaskKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerLifecycleToken {
    pub task_id: u64,
    pub shard_id: ShardId,
    pub operation: String,
    pub load_version: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSchedulerOptions {
    pub base_postpone_ms: u64,
    pub max_postpone_ms: u64,
    pub max_retry_times: u64,
    pub max_inflight: usize,
}

impl Default for TaskSchedulerOptions {
    fn default() -> Self {
        Self {
            base_postpone_ms: 1_000,
            max_postpone_ms: 60_000,
            max_retry_times: 10,
            max_inflight: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerRunReport {
    pub task_id: u64,
    pub result: SchedulerTaskResult,
    pub retry_times: u64,
    pub next_run_time_ms: Option<u64>,
    pub queue_len: usize,
    pub aborted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeterministicTaskScheduler {
    queue: BTreeMap<u64, SchedulerTask>,
    next_task_id: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSchedulerSnapshot {
    pub tasks: Vec<SchedulerTask>,
    pub next_task_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebalanceOptions {
    pub max_moves_per_round: usize,
    pub partition_count_safe_gap: usize,
}

impl Default for RebalanceOptions {
    fn default() -> Self {
        Self {
            max_moves_per_round: 10,
            partition_count_safe_gap: 0,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalanceRoundReport {
    pub plans: Vec<ShardMovePlan>,
    pub balance_partition_count: BTreeMap<RaftNodeId, usize>,
    pub total_normal: usize,
    pub safe_line: usize,
    pub placement_fail_count: usize,
    pub placement_fail_reasons: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalanceController {
    replicas: BTreeMap<u64, ShardReplica>,
    membership_version: u64,
    next_replica_id: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RebalanceError {
    #[error("replica not found: {0}")]
    ReplicaNotFound(u64),
    #[error("cannot safely move primary replica {0} without a normal secondary")]
    CannotMovePrimary(u64),
    #[error("target node already hosts shard {shard_id}: node={node_id}")]
    TargetAlreadyHostsShard {
        shard_id: ShardId,
        node_id: RaftNodeId,
    },
    #[error("no target node is available")]
    NoTargetNode,
    #[error("move target is not loading: {0}")]
    TargetNotLoading(u64),
    #[error("active membership is empty for shard {0}")]
    EmptyMembership(ShardId),
    #[error("primary replica {primary_replica_id} is not active for shard {shard_id}")]
    PrimaryNotActive {
        shard_id: ShardId,
        primary_replica_id: u64,
    },
    #[error("task not found: {0}")]
    TaskNotFound(u64),
    #[error("scheduler inflight limit is zero")]
    InvalidSchedulerInflight,
    #[error("scheduler snapshot is corrupt: {0}")]
    CorruptSchedulerSnapshot(String),
}

impl DeterministicTaskScheduler {
    pub fn submit(&mut self, priority: i32, now_ms: u64, kind: SchedulerTaskKind) -> SchedulerTask {
        let id = self.next_task_id;
        self.next_task_id += 1;
        let task = SchedulerTask {
            id,
            priority,
            create_time_ms: now_ms,
            next_run_time_ms: now_ms,
            last_run_time_ms: 0,
            retry_times: 0,
            kind,
        };
        self.queue.insert(id, task.clone());
        task
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn snapshot(&self) -> Vec<SchedulerTask> {
        let mut tasks = self.queue.values().cloned().collect::<Vec<_>>();
        tasks.sort_by_key(|task| (task.priority, task.next_run_time_ms, task.id));
        tasks
    }

    pub fn export_snapshot(&self) -> TaskSchedulerSnapshot {
        TaskSchedulerSnapshot {
            tasks: self.snapshot(),
            next_task_id: self.next_task_id,
        }
    }

    pub fn encode_snapshot(&self) -> Result<Vec<u8>, RebalanceError> {
        serde_json::to_vec(&self.export_snapshot())
            .map_err(|err| RebalanceError::CorruptSchedulerSnapshot(err.to_string()))
    }

    pub fn restore_snapshot(snapshot: TaskSchedulerSnapshot) -> Result<Self, RebalanceError> {
        let mut queue = BTreeMap::new();
        let mut max_task_id = 0;
        for task in snapshot.tasks {
            max_task_id = max_task_id.max(task.id);
            if queue.insert(task.id, task).is_some() {
                return Err(RebalanceError::CorruptSchedulerSnapshot(
                    "duplicate task id".to_string(),
                ));
            }
        }
        Ok(Self {
            queue,
            next_task_id: snapshot.next_task_id.max(max_task_id + 1),
        })
    }

    pub fn decode_snapshot(bytes: &[u8]) -> Result<Self, RebalanceError> {
        let snapshot = serde_json::from_slice(bytes)
            .map_err(|err| RebalanceError::CorruptSchedulerSnapshot(err.to_string()))?;
        Self::restore_snapshot(snapshot)
    }

    pub fn has_update_membership_task(&self, shard_id: ShardId, membership_version: u64) -> bool {
        self.queue.values().any(|task| {
            matches!(
                &task.kind,
                SchedulerTaskKind::UpdateMembership(plan)
                    if plan.shard_id == shard_id
                        && plan.membership_version == membership_version
            )
        })
    }

    pub fn pop_first_available(&mut self, now_ms: u64) -> Option<SchedulerTask> {
        let task_id = self
            .queue
            .values()
            .filter(|task| task.next_run_time_ms <= now_ms)
            .min_by_key(|task| (task.priority, task.next_run_time_ms, task.id))
            .map(|task| task.id)?;
        self.queue.remove(&task_id)
    }

    pub fn run_next(
        &mut self,
        now_ms: u64,
        result: SchedulerTaskResult,
        options: TaskSchedulerOptions,
    ) -> Result<Option<SchedulerRunReport>, RebalanceError> {
        if options.max_inflight == 0 {
            return Err(RebalanceError::InvalidSchedulerInflight);
        }
        let Some(mut task) = self.pop_first_available(now_ms) else {
            return Ok(None);
        };
        task.last_run_time_ms = now_ms;
        let mut aborted = false;
        let next_run_time_ms = match result {
            SchedulerTaskResult::Ok | SchedulerTaskResult::Aborted => None,
            SchedulerTaskResult::RetryLater => {
                task.retry_times += 1;
                if task.retry_times > options.max_retry_times {
                    aborted = true;
                    None
                } else {
                    let postpone = task
                        .retry_times
                        .saturating_mul(options.base_postpone_ms)
                        .min(options.max_postpone_ms);
                    task.next_run_time_ms = now_ms.saturating_add(postpone);
                    let next = task.next_run_time_ms;
                    self.queue.insert(task.id, task.clone());
                    Some(next)
                }
            }
        };
        Ok(Some(SchedulerRunReport {
            task_id: task.id,
            result,
            retry_times: task.retry_times,
            next_run_time_ms,
            queue_len: self.queue.len(),
            aborted,
        }))
    }
}

impl SchedulerTask {
    pub fn lifecycle_token(&self) -> Option<SchedulerLifecycleToken> {
        let SchedulerTaskKind::RebalanceStep(step) = &self.kind else {
            return None;
        };
        let (shard_id, operation, load_version) = match step {
            RebalanceStep::LoadTarget {
                shard_id,
                load_version,
                ..
            } => (*shard_id, "load", *load_version),
            RebalanceStep::ReloadTarget {
                shard_id,
                load_version,
                ..
            } => (*shard_id, "reload", *load_version),
            RebalanceStep::UnloadSource { shard_id, .. } => (*shard_id, "unload", 0),
            RebalanceStep::FreezeSource { shard_id, .. } => (*shard_id, "freeze", 0),
            RebalanceStep::UpdateMembership { shard_id, .. } => (*shard_id, "membership", 0),
        };
        Some(SchedulerLifecycleToken {
            task_id: self.id,
            shard_id,
            operation: operation.to_string(),
            load_version,
            generation: self.create_time_ms,
        })
    }
}

impl RebalanceController {
    pub fn with_replicas(replicas: impl IntoIterator<Item = ShardReplica>) -> Self {
        let mut max_replica_id = 0;
        let mut map = BTreeMap::new();
        for replica in replicas {
            max_replica_id = max_replica_id.max(replica.replica_id);
            map.insert(replica.replica_id, replica);
        }
        Self {
            replicas: map,
            membership_version: 1,
            next_replica_id: max_replica_id + 1,
        }
    }

    pub fn replicas(&self) -> Vec<ShardReplica> {
        self.replicas.values().cloned().collect()
    }

    pub fn membership_version(&self) -> u64 {
        self.membership_version
    }

    pub fn node_loads(&self) -> BTreeMap<RaftNodeId, usize> {
        let mut loads = BTreeMap::new();
        for replica in self
            .replicas
            .values()
            .filter(|replica| matches!(replica.state, ShardReplicaState::Normal))
        {
            *loads.entry(replica.node_id).or_default() += 1;
        }
        loads
    }

    pub fn plan_rebalance_round(
        &self,
        all_node_ids: impl IntoIterator<Item = RaftNodeId>,
        options: RebalanceOptions,
    ) -> Vec<ShardMovePlan> {
        self.plan_rebalance_round_report(all_node_ids, options)
            .plans
    }

    pub fn plan_rebalance_round_report(
        &self,
        all_node_ids: impl IntoIterator<Item = RaftNodeId>,
        options: RebalanceOptions,
    ) -> RebalanceRoundReport {
        let all_nodes = all_node_ids.into_iter().collect::<BTreeSet<_>>();
        let mut simulated_loads = self.node_loads();
        for node_id in &all_nodes {
            simulated_loads.entry(*node_id).or_default();
        }
        let total_normal = simulated_loads.values().sum::<usize>();
        let safe_line = if all_nodes.is_empty() || total_normal == 0 {
            0
        } else {
            total_normal.div_ceil(all_nodes.len()) + options.partition_count_safe_gap
        };
        let mut report = RebalanceRoundReport {
            balance_partition_count: simulated_loads.clone(),
            total_normal,
            safe_line,
            ..RebalanceRoundReport::default()
        };

        if all_nodes.len() < 2 || options.max_moves_per_round == 0 {
            return report;
        }
        if total_normal == 0 {
            return report;
        }
        let mut next_replica_id = self.next_replica_id;
        let mut candidates = self
            .replicas
            .values()
            .filter(|replica| replica.state == ShardReplicaState::Normal)
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|replica| (replica.node_id, replica.shard_id, replica.replica_id));

        for source in candidates {
            if report.plans.len() >= options.max_moves_per_round {
                break;
            }
            if simulated_loads
                .get(&source.node_id)
                .copied()
                .unwrap_or_default()
                <= safe_line
            {
                continue;
            }
            if source.role == ShardRole::Primary && !self.has_normal_secondary(source.shard_id) {
                record_placement_fail(&mut report, "primary_without_normal_secondary");
                continue;
            }
            let Some(target_node_id) = least_loaded_target(
                &simulated_loads,
                &all_nodes,
                source.shard_id,
                &self.replicas,
            ) else {
                record_placement_fail(&mut report, "no_target_node");
                continue;
            };
            if target_node_id == source.node_id {
                record_placement_fail(&mut report, "target_is_source");
                continue;
            }
            report.plans.push(ShardMovePlan {
                shard_id: source.shard_id,
                source_replica_id: source.replica_id,
                source_node_id: source.node_id,
                target_replica_id: next_replica_id,
                target_node_id,
                load_version: source.load_version + 1,
            });
            next_replica_id += 1;
            *simulated_loads.entry(source.node_id).or_default() -= 1;
            *simulated_loads.entry(target_node_id).or_default() += 1;
        }

        report
    }

    pub fn begin_move(
        &mut self,
        plan: &ShardMovePlan,
    ) -> Result<Vec<RebalanceStep>, RebalanceError> {
        let source = self
            .replicas
            .get(&plan.source_replica_id)
            .ok_or(RebalanceError::ReplicaNotFound(plan.source_replica_id))?;
        if source.role == ShardRole::Primary && !self.has_normal_secondary(source.shard_id) {
            return Err(RebalanceError::CannotMovePrimary(source.replica_id));
        }
        if self.replicas.values().any(|replica| {
            replica.shard_id == plan.shard_id
                && replica.node_id == plan.target_node_id
                && replica.state != ShardReplicaState::Frozen
        }) {
            return Err(RebalanceError::TargetAlreadyHostsShard {
                shard_id: plan.shard_id,
                node_id: plan.target_node_id,
            });
        }

        self.replicas.insert(
            plan.target_replica_id,
            ShardReplica {
                shard_id: plan.shard_id,
                replica_id: plan.target_replica_id,
                node_id: plan.target_node_id,
                role: ShardRole::Secondary,
                state: ShardReplicaState::Loading,
                load_version: plan.load_version,
            },
        );
        self.next_replica_id = self.next_replica_id.max(plan.target_replica_id + 1);

        Ok(vec![RebalanceStep::LoadTarget {
            shard_id: plan.shard_id,
            replica_id: plan.target_replica_id,
            node_id: plan.target_node_id,
            load_version: plan.load_version,
        }])
    }

    pub fn finish_target_load(
        &mut self,
        target_replica_id: u64,
    ) -> Result<Vec<RebalanceStep>, RebalanceError> {
        let (shard_id, source_replica_id, source_node_id, primary_replica_id) = {
            let target = self
                .replicas
                .get(&target_replica_id)
                .ok_or(RebalanceError::ReplicaNotFound(target_replica_id))?;
            if target.state != ShardReplicaState::Loading {
                return Err(RebalanceError::TargetNotLoading(target_replica_id));
            }
            let source = self
                .replicas
                .values()
                .find(|replica| {
                    replica.shard_id == target.shard_id
                        && replica.state == ShardReplicaState::Normal
                        && replica.node_id != target.node_id
                })
                .ok_or(RebalanceError::NoTargetNode)?;
            let primary_replica_id = self
                .primary_replica_id(target.shard_id)
                .unwrap_or(target_replica_id);
            (
                target.shard_id,
                source.replica_id,
                source.node_id,
                primary_replica_id,
            )
        };

        self.replicas
            .get_mut(&target_replica_id)
            .expect("checked target")
            .state = ShardReplicaState::Normal;
        self.replicas
            .get_mut(&source_replica_id)
            .expect("checked source")
            .state = ShardReplicaState::Freezing;
        self.membership_version += 1;
        let active_replica_ids = self.active_replica_ids(shard_id);

        Ok(vec![
            RebalanceStep::UpdateMembership {
                shard_id,
                active_replica_ids,
                primary_replica_id,
                membership_version: self.membership_version,
            },
            RebalanceStep::FreezeSource {
                shard_id,
                replica_id: source_replica_id,
                node_id: source_node_id,
            },
        ])
    }

    pub fn finish_source_freeze(
        &mut self,
        source_replica_id: u64,
    ) -> Result<Vec<RebalanceStep>, RebalanceError> {
        let source = self
            .replicas
            .get_mut(&source_replica_id)
            .ok_or(RebalanceError::ReplicaNotFound(source_replica_id))?;
        source.state = ShardReplicaState::Frozen;
        self.membership_version += 1;
        Ok(vec![RebalanceStep::UnloadSource {
            shard_id: source.shard_id,
            replica_id: source.replica_id,
            node_id: source.node_id,
        }])
    }

    pub fn plan_membership_update_task(
        &self,
        self_replica_id: u64,
        active_replica_ids: impl IntoIterator<Item = u64>,
        primary_replica_id: u64,
        membership_version: u64,
        options: MembershipUpdateTaskOptions,
    ) -> Result<MembershipUpdateTaskPlan, RebalanceError> {
        let self_replica = self
            .replicas
            .get(&self_replica_id)
            .ok_or(RebalanceError::ReplicaNotFound(self_replica_id))?;
        let mut active_replica_ids = active_replica_ids.into_iter().collect::<Vec<_>>();
        active_replica_ids.sort_unstable();
        active_replica_ids.dedup();
        if active_replica_ids.is_empty() {
            return Err(RebalanceError::EmptyMembership(self_replica.shard_id));
        }
        if !active_replica_ids.contains(&primary_replica_id) {
            return Err(RebalanceError::PrimaryNotActive {
                shard_id: self_replica.shard_id,
                primary_replica_id,
            });
        }

        let replica_node_ids = active_replica_ids
            .iter()
            .map(|replica_id| {
                self.replicas
                    .get(replica_id)
                    .ok_or(RebalanceError::ReplicaNotFound(*replica_id))
                    .map(|replica| replica.node_id)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let leader_node_id = self
            .replicas
            .get(&primary_replica_id)
            .ok_or(RebalanceError::ReplicaNotFound(primary_replica_id))?
            .node_id;

        let mut requests = Vec::new();
        for replica_id in &active_replica_ids {
            if options.exclude_self && *replica_id == self_replica_id {
                continue;
            }
            let replica = self
                .replicas
                .get(replica_id)
                .ok_or(RebalanceError::ReplicaNotFound(*replica_id))?;
            if replica.shard_id != self_replica.shard_id
                || replica.state == ShardReplicaState::Frozen
            {
                continue;
            }
            requests.push(MembershipUpdatePeerRequest {
                replica_id: *replica_id,
                node_id: replica.node_id,
                request: MembershipUpdateRequest {
                    shard_id: self_replica.shard_id,
                    membership_version,
                    replica_membership_version: membership_version,
                    replica_node_ids: replica_node_ids.clone(),
                    leader_node_id: Some(leader_node_id),
                },
            });
        }

        Ok(MembershipUpdateTaskPlan {
            shard_id: self_replica.shard_id,
            self_replica_id,
            active_replica_ids,
            primary_replica_id,
            membership_version,
            requests,
        })
    }

    pub fn evaluate_membership_update_task(
        &self,
        plan: MembershipUpdateTaskPlan,
        peer_statuses: impl IntoIterator<Item = MembershipUpdatePeerStatus>,
        options: MembershipUpdateTaskOptions,
    ) -> MembershipUpdateTaskReport {
        let mut success_count = 0;
        let mut not_found_count = 0;
        let mut failed_count = 0;
        for status in peer_statuses {
            match status {
                MembershipUpdatePeerStatus::Ok => success_count += 1,
                MembershipUpdatePeerStatus::NotFound => not_found_count += 1,
                MembershipUpdatePeerStatus::Failed { .. } => failed_count += 1,
            }
        }
        let threshold = if options.success_threshold == 0 {
            plan.requests.len()
        } else {
            options.success_threshold
        };
        let accepted = success_count + not_found_count >= threshold;
        MembershipUpdateTaskReport {
            plan,
            success_count,
            not_found_count,
            failed_count,
            success_threshold: threshold,
            accepted,
            should_submit_fsm: accepted && options.submit_fsm,
        }
    }

    pub fn repair_broken_membership_tasks(
        &self,
        scheduler: &mut DeterministicTaskScheduler,
        now_ms: u64,
        priority: i32,
    ) -> Result<Vec<SchedulerTask>, RebalanceError> {
        let mut repaired = Vec::new();
        let mut shard_ids = self
            .replicas
            .values()
            .filter(|replica| replica.state == ShardReplicaState::Freezing)
            .map(|replica| replica.shard_id)
            .collect::<Vec<_>>();
        shard_ids.sort_unstable();
        shard_ids.dedup();

        for shard_id in shard_ids {
            let active_replica_ids = self.active_replica_ids(shard_id);
            let Some(primary_replica_id) = self.primary_replica_id(shard_id) else {
                continue;
            };
            if scheduler.has_update_membership_task(shard_id, self.membership_version) {
                continue;
            }
            let plan = self.plan_membership_update_task(
                primary_replica_id,
                active_replica_ids,
                primary_replica_id,
                self.membership_version,
                MembershipUpdateTaskOptions {
                    success_threshold: 1,
                    submit_fsm: true,
                    ..MembershipUpdateTaskOptions::default()
                },
            )?;
            repaired.push(scheduler.submit(
                priority,
                now_ms,
                SchedulerTaskKind::UpdateMembership(plan),
            ));
        }
        Ok(repaired)
    }

    pub fn rollback_move(&mut self, target_replica_id: u64) -> Result<(), RebalanceError> {
        let target = self
            .replicas
            .get(&target_replica_id)
            .ok_or(RebalanceError::ReplicaNotFound(target_replica_id))?;
        if target.state == ShardReplicaState::Loading || target.state == ShardReplicaState::Creating
        {
            self.replicas.remove(&target_replica_id);
        }
        Ok(())
    }

    fn has_normal_secondary(&self, shard_id: ShardId) -> bool {
        self.replicas.values().any(|replica| {
            replica.shard_id == shard_id
                && replica.role == ShardRole::Secondary
                && replica.state == ShardReplicaState::Normal
        })
    }

    fn primary_replica_id(&self, shard_id: ShardId) -> Option<u64> {
        self.replicas
            .values()
            .find(|replica| replica.shard_id == shard_id && replica.role == ShardRole::Primary)
            .map(|replica| replica.replica_id)
    }

    fn active_replica_ids(&self, shard_id: ShardId) -> Vec<u64> {
        self.replicas
            .values()
            .filter(|replica| {
                replica.shard_id == shard_id
                    && matches!(
                        replica.state,
                        ShardReplicaState::Normal | ShardReplicaState::Freezing
                    )
            })
            .map(|replica| replica.replica_id)
            .collect()
    }
}

fn least_loaded_target(
    loads: &BTreeMap<RaftNodeId, usize>,
    all_nodes: &BTreeSet<RaftNodeId>,
    shard_id: ShardId,
    replicas: &BTreeMap<u64, ShardReplica>,
) -> Option<RaftNodeId> {
    all_nodes
        .iter()
        .filter(|node_id| {
            !replicas.values().any(|replica| {
                replica.shard_id == shard_id
                    && replica.node_id == **node_id
                    && replica.state != ShardReplicaState::Frozen
            })
        })
        .min_by_key(|node_id| (loads.get(node_id).copied().unwrap_or_default(), **node_id))
        .copied()
}

fn record_placement_fail(report: &mut RebalanceRoundReport, reason: &str) {
    report.placement_fail_count += 1;
    *report
        .placement_fail_reasons
        .entry(reason.to_string())
        .or_default() += 1;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replica(
        replica_id: u64,
        shard_id: ShardId,
        node_id: RaftNodeId,
        role: ShardRole,
    ) -> ShardReplica {
        ShardReplica {
            shard_id,
            replica_id,
            node_id,
            role,
            state: ShardReplicaState::Normal,
            load_version: 1,
        }
    }

    #[test]
    fn rebalance_moves_from_overloaded_node_to_low_load_node() {
        let mut controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 10, 2, ShardRole::Secondary),
            replica(3, 20, 1, ShardRole::Primary),
            replica(4, 20, 2, ShardRole::Secondary),
            replica(5, 30, 1, ShardRole::Primary),
            replica(6, 30, 2, ShardRole::Secondary),
        ]);

        let plans = controller.plan_rebalance_round(
            [1, 2, 3],
            RebalanceOptions {
                max_moves_per_round: 1,
                partition_count_safe_gap: 0,
            },
        );
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].source_node_id, 1);
        assert_eq!(plans[0].target_node_id, 3);

        assert!(matches!(
            controller.begin_move(&plans[0]).unwrap().as_slice(),
            [RebalanceStep::LoadTarget { .. }]
        ));
        let steps = controller
            .finish_target_load(plans[0].target_replica_id)
            .unwrap();
        assert!(matches!(steps[0], RebalanceStep::UpdateMembership { .. }));
        assert!(matches!(steps[1], RebalanceStep::FreezeSource { .. }));
        let unload = controller
            .finish_source_freeze(plans[0].source_replica_id)
            .unwrap();
        assert!(matches!(unload[0], RebalanceStep::UnloadSource { .. }));
        assert_eq!(
            controller
                .replicas()
                .into_iter()
                .find(|replica| replica.replica_id == plans[0].source_replica_id)
                .unwrap()
                .state,
            ShardReplicaState::Frozen
        );
    }

    #[test]
    fn rebalance_round_report_exposes_cpp_balance_metrics() {
        let controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 10, 2, ShardRole::Secondary),
            replica(3, 20, 1, ShardRole::Primary),
            replica(4, 20, 2, ShardRole::Secondary),
            replica(5, 30, 1, ShardRole::Primary),
            replica(6, 30, 2, ShardRole::Secondary),
        ]);

        let report = controller.plan_rebalance_round_report(
            [1, 2, 3],
            RebalanceOptions {
                max_moves_per_round: 1,
                partition_count_safe_gap: 0,
            },
        );
        assert_eq!(
            report.balance_partition_count,
            BTreeMap::from([(1, 3), (2, 3), (3, 0)])
        );
        assert_eq!(report.total_normal, 6);
        assert_eq!(report.safe_line, 2);
        assert_eq!(report.placement_fail_count, 0);
        assert_eq!(report.plans.len(), 1);
        assert_eq!(report.plans[0].target_node_id, 3);
    }

    #[test]
    fn rebalance_round_report_counts_placement_failures() {
        let controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 20, 1, ShardRole::Primary),
        ]);

        let report = controller.plan_rebalance_round_report([1, 2], RebalanceOptions::default());
        assert!(report.plans.is_empty());
        assert_eq!(
            report.balance_partition_count,
            BTreeMap::from([(1, 2), (2, 0)])
        );
        assert_eq!(report.total_normal, 2);
        assert_eq!(report.safe_line, 1);
        assert_eq!(report.placement_fail_count, 2);
        assert_eq!(
            report.placement_fail_reasons,
            BTreeMap::from([("primary_without_normal_secondary".to_string(), 2)])
        );
    }

    #[test]
    fn task_scheduler_runs_lower_priority_first_and_skips_postponed_tasks() {
        let mut scheduler = DeterministicTaskScheduler::default();
        let slow = scheduler.submit(
            10,
            0,
            SchedulerTaskKind::RebalanceStep(RebalanceStep::UnloadSource {
                shard_id: 1,
                replica_id: 11,
                node_id: 1,
            }),
        );
        let urgent = scheduler.submit(
            0,
            0,
            SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
                shard_id: 1,
                replica_id: 12,
                node_id: 2,
            }),
        );
        assert_eq!(scheduler.snapshot()[0].id, urgent.id);

        let retry = scheduler
            .run_next(
                10,
                SchedulerTaskResult::RetryLater,
                TaskSchedulerOptions {
                    base_postpone_ms: 100,
                    max_postpone_ms: 250,
                    max_retry_times: 3,
                    max_inflight: 1,
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(retry.task_id, urgent.id);
        assert_eq!(retry.next_run_time_ms, Some(110));

        let done = scheduler
            .run_next(50, SchedulerTaskResult::Ok, TaskSchedulerOptions::default())
            .unwrap()
            .unwrap();
        assert_eq!(done.task_id, slow.id);
        assert_eq!(scheduler.queue_len(), 1);
    }

    #[test]
    fn task_scheduler_retries_with_capped_backoff_and_aborts() {
        let mut scheduler = DeterministicTaskScheduler::default();
        let task = scheduler.submit(
            0,
            0,
            SchedulerTaskKind::RebalanceStep(RebalanceStep::LoadTarget {
                shard_id: 1,
                replica_id: 2,
                node_id: 3,
                load_version: 4,
            }),
        );
        let options = TaskSchedulerOptions {
            base_postpone_ms: 100,
            max_postpone_ms: 150,
            max_retry_times: 2,
            max_inflight: 1,
        };

        let first = scheduler
            .run_next(1, SchedulerTaskResult::RetryLater, options)
            .unwrap()
            .unwrap();
        assert_eq!(first.task_id, task.id);
        assert_eq!(first.retry_times, 1);
        assert_eq!(first.next_run_time_ms, Some(101));

        let second = scheduler
            .run_next(101, SchedulerTaskResult::RetryLater, options)
            .unwrap()
            .unwrap();
        assert_eq!(second.retry_times, 2);
        assert_eq!(second.next_run_time_ms, Some(251));

        let aborted = scheduler
            .run_next(251, SchedulerTaskResult::RetryLater, options)
            .unwrap()
            .unwrap();
        assert!(aborted.aborted);
        assert_eq!(aborted.retry_times, 3);
        assert_eq!(aborted.next_run_time_ms, None);
        assert_eq!(scheduler.queue_len(), 0);
    }

    #[test]
    fn task_scheduler_can_enqueue_update_membership_plan() {
        let controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 10, 2, ShardRole::Secondary),
            replica(3, 10, 3, ShardRole::Secondary),
        ]);
        let plan = controller
            .plan_membership_update_task(1, [1, 2, 3], 1, 7, MembershipUpdateTaskOptions::default())
            .unwrap();
        let mut scheduler = DeterministicTaskScheduler::default();
        let task = scheduler.submit(0, 42, SchedulerTaskKind::UpdateMembership(plan.clone()));
        assert_eq!(task.priority, 0);
        assert_eq!(task.next_run_time_ms, 42);

        let Some(next) = scheduler.pop_first_available(42) else {
            panic!("expected update membership task");
        };
        assert_eq!(next.id, task.id);
        assert_eq!(next.kind, SchedulerTaskKind::UpdateMembership(plan));
    }

    #[test]
    fn scheduler_rebalance_steps_issue_lifecycle_tokens() {
        let mut scheduler = DeterministicTaskScheduler::default();
        let load = scheduler.submit(
            0,
            42,
            SchedulerTaskKind::RebalanceStep(RebalanceStep::LoadTarget {
                shard_id: 7,
                replica_id: 3,
                node_id: 2,
                load_version: 99,
            }),
        );
        let token = load.lifecycle_token().unwrap();
        assert_eq!(token.task_id, load.id);
        assert_eq!(token.shard_id, 7);
        assert_eq!(token.operation, "load");
        assert_eq!(token.load_version, 99);
        assert_eq!(token.generation, 42);

        let reload = scheduler.submit(
            0,
            43,
            SchedulerTaskKind::RebalanceStep(RebalanceStep::ReloadTarget {
                shard_id: 7,
                replica_id: 3,
                node_id: 2,
                load_version: 100,
            }),
        );
        let token = reload.lifecycle_token().unwrap();
        assert_eq!(token.task_id, reload.id);
        assert_eq!(token.shard_id, 7);
        assert_eq!(token.operation, "reload");
        assert_eq!(token.load_version, 100);
        assert_eq!(token.generation, 43);

        let unload = scheduler.submit(
            0,
            44,
            SchedulerTaskKind::RebalanceStep(RebalanceStep::UnloadSource {
                shard_id: 7,
                replica_id: 1,
                node_id: 1,
            }),
        );
        let token = unload.lifecycle_token().unwrap();
        assert_eq!(token.task_id, unload.id);
        assert_eq!(token.operation, "unload");
        assert_eq!(token.load_version, 0);
        assert_eq!(token.generation, 44);
    }

    #[test]
    fn task_scheduler_snapshot_restores_pending_retry_state() {
        let mut scheduler = DeterministicTaskScheduler::default();
        let task = scheduler.submit(
            0,
            10,
            SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
                shard_id: 7,
                replica_id: 2,
                node_id: 2,
            }),
        );
        let retry = scheduler
            .run_next(
                20,
                SchedulerTaskResult::RetryLater,
                TaskSchedulerOptions {
                    base_postpone_ms: 100,
                    max_postpone_ms: 1_000,
                    max_retry_times: 3,
                    max_inflight: 1,
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(retry.next_run_time_ms, Some(120));

        let encoded = scheduler.encode_snapshot().unwrap();
        let mut restored = DeterministicTaskScheduler::decode_snapshot(&encoded).unwrap();
        assert!(restored.pop_first_available(119).is_none());
        let restored_task = restored.pop_first_available(120).unwrap();
        assert_eq!(restored_task.id, task.id);
        assert_eq!(restored_task.retry_times, 1);
        assert_eq!(restored_task.last_run_time_ms, 20);
    }

    #[test]
    fn repair_broken_membership_tasks_resubmits_freezing_shards() {
        let mut controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 10, 2, ShardRole::Secondary),
            replica(3, 20, 1, ShardRole::Primary),
            replica(4, 20, 2, ShardRole::Secondary),
        ]);
        let plan = ShardMovePlan {
            shard_id: 10,
            source_replica_id: 2,
            source_node_id: 2,
            target_replica_id: 5,
            target_node_id: 3,
            load_version: 2,
        };
        controller.begin_move(&plan).unwrap();
        controller
            .finish_target_load(plan.target_replica_id)
            .unwrap();
        assert_eq!(controller.membership_version(), 2);

        let mut scheduler = DeterministicTaskScheduler::default();
        let repaired = controller
            .repair_broken_membership_tasks(&mut scheduler, 100, 0)
            .unwrap();
        assert_eq!(repaired.len(), 1);
        assert_eq!(scheduler.queue_len(), 1);
        let SchedulerTaskKind::UpdateMembership(task_plan) = &repaired[0].kind else {
            panic!("expected update membership task");
        };
        assert_eq!(task_plan.shard_id, 10);
        assert_eq!(task_plan.membership_version, 2);
        assert_eq!(task_plan.primary_replica_id, 1);
        assert_eq!(task_plan.active_replica_ids, vec![1, 2, 5]);

        let report = controller.evaluate_membership_update_task(
            task_plan.clone(),
            [MembershipUpdatePeerStatus::Ok],
            MembershipUpdateTaskOptions {
                success_threshold: 1,
                submit_fsm: true,
                ..MembershipUpdateTaskOptions::default()
            },
        );
        assert!(report.accepted);
        assert!(report.should_submit_fsm);

        let duplicate = controller
            .repair_broken_membership_tasks(&mut scheduler, 200, 0)
            .unwrap();
        assert!(duplicate.is_empty());
    }

    #[test]
    fn rebalance_does_not_move_lonely_primary() {
        let controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 20, 1, ShardRole::Primary),
        ]);

        assert!(controller
            .plan_rebalance_round([1, 2], RebalanceOptions::default())
            .is_empty());
    }

    #[test]
    fn begin_move_rejects_target_that_already_hosts_same_shard() {
        let mut controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 10, 2, ShardRole::Secondary),
        ]);
        let err = controller
            .begin_move(&ShardMovePlan {
                shard_id: 10,
                source_replica_id: 1,
                source_node_id: 1,
                target_replica_id: 3,
                target_node_id: 2,
                load_version: 2,
            })
            .unwrap_err();
        assert_eq!(
            err,
            RebalanceError::TargetAlreadyHostsShard {
                shard_id: 10,
                node_id: 2,
            }
        );
    }

    #[test]
    fn membership_update_task_matches_cpp_threshold_and_not_found_rules() {
        let mut frozen = replica(4, 10, 4, ShardRole::Secondary);
        frozen.state = ShardReplicaState::Frozen;
        let controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 10, 2, ShardRole::Secondary),
            replica(3, 10, 3, ShardRole::Secondary),
            frozen,
        ]);
        let options = MembershipUpdateTaskOptions {
            exclude_self: true,
            success_threshold: 2,
            submit_fsm: true,
        };

        let plan = controller
            .plan_membership_update_task(1, [1, 2, 3, 4], 1, 7, options)
            .unwrap();
        assert_eq!(plan.shard_id, 10);
        assert_eq!(plan.active_replica_ids, vec![1, 2, 3, 4]);
        assert_eq!(plan.requests.len(), 2);
        assert_eq!(
            plan.requests
                .iter()
                .map(|request| request.replica_id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(plan.requests.iter().all(|request| {
            request.request.replica_node_ids == vec![1, 2, 3, 4]
                && request.request.leader_node_id == Some(1)
        }));

        let report = controller.evaluate_membership_update_task(
            plan.clone(),
            [
                MembershipUpdatePeerStatus::Ok,
                MembershipUpdatePeerStatus::NotFound,
            ],
            options,
        );
        assert!(report.accepted);
        assert!(report.should_submit_fsm);
        assert_eq!(report.success_count, 1);
        assert_eq!(report.not_found_count, 1);

        let failed = controller.evaluate_membership_update_task(
            plan,
            [
                MembershipUpdatePeerStatus::Ok,
                MembershipUpdatePeerStatus::Failed {
                    code: "timeout".to_string(),
                    message: "rpc timeout".to_string(),
                },
            ],
            options,
        );
        assert!(!failed.accepted);
        assert!(!failed.should_submit_fsm);
        assert_eq!(failed.failed_count, 1);
    }

    #[test]
    fn membership_update_task_validates_active_primary_and_replica_ids() {
        let controller = RebalanceController::with_replicas([
            replica(1, 10, 1, ShardRole::Primary),
            replica(2, 10, 2, ShardRole::Secondary),
        ]);

        assert_eq!(
            controller
                .plan_membership_update_task(1, [], 1, 1, MembershipUpdateTaskOptions::default())
                .unwrap_err(),
            RebalanceError::EmptyMembership(10)
        );
        assert_eq!(
            controller
                .plan_membership_update_task(1, [2], 1, 1, MembershipUpdateTaskOptions::default())
                .unwrap_err(),
            RebalanceError::PrimaryNotActive {
                shard_id: 10,
                primary_replica_id: 1,
            }
        );
        assert_eq!(
            controller
                .plan_membership_update_task(
                    1,
                    [1, 99],
                    1,
                    1,
                    MembershipUpdateTaskOptions::default()
                )
                .unwrap_err(),
            RebalanceError::ReplicaNotFound(99)
        );
    }
}
