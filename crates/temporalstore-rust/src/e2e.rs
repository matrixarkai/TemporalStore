// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::meta::ShardLocation;
use crate::raft::{MetaCommand, MetaRaftCluster, RaftCluster, RaftError, RaftNodeId};
use crate::shared_store::SharedStoreStorageMode;
use crate::types::{Command, CommandResponse, ExecuteRequest, ExecuteResponse, ShardId, Status};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicaReadPolicy {
    PinPrimary,
    AnyReplica,
    Replica(RaftNodeId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalStoreClientOptions {
    pub pin_primary: bool,
    pub replica_read_policy: ReplicaReadPolicy,
}

impl Default for TemporalStoreClientOptions {
    fn default() -> Self {
        Self {
            pin_primary: true,
            replica_read_policy: ReplicaReadPolicy::PinPrimary,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    Raft,
    SharedStore,
}

impl Default for ReplicationMode {
    fn default() -> Self {
        Self::Raft
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftWriteMode {
    Sync,
    Async,
}

impl Default for RaftWriteMode {
    fn default() -> Self {
        Self::Async
    }
}

impl RaftWriteMode {
    pub fn from_sync_flag(sync: bool) -> Self {
        if sync {
            Self::Sync
        } else {
            Self::Async
        }
    }

    pub fn is_sync(self) -> bool {
        matches!(self, Self::Sync)
    }

    pub fn is_async(self) -> bool {
        matches!(self, Self::Async)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EndToEndWorkflowOptions {
    pub replication_mode: ReplicationMode,
    pub storage_mode: SharedStoreStorageMode,
    pub raft_write_mode: RaftWriteMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillSwitches {
    pub proxy_enabled: bool,
    pub client_enabled: bool,
    pub data_nodes_enabled: bool,
    pub writes_enabled: bool,
    pub reads_enabled: bool,
    pub replication_enabled: bool,
    pub async_storage_enabled: bool,
    pub secondary_promotion_enabled: bool,
    pub scale_changes_enabled: bool,
}

impl Default for KillSwitches {
    fn default() -> Self {
        Self {
            proxy_enabled: true,
            client_enabled: true,
            data_nodes_enabled: true,
            writes_enabled: true,
            reads_enabled: true,
            replication_enabled: true,
            async_storage_enabled: true,
            secondary_promotion_enabled: true,
            scale_changes_enabled: true,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkflowError {
    #[error("kill switch disabled: {0}")]
    KillSwitch(&'static str),
    #[error("metaserver route is missing for shard {0}")]
    MissingRoute(ShardId),
    #[error("raft error: {0}")]
    Raft(String),
}

impl From<RaftError> for WorkflowError {
    fn from(value: RaftError) -> Self {
        WorkflowError::Raft(value.to_string())
    }
}

#[derive(Debug, Default, Clone)]
pub struct AsyncStorageJournal {
    inner: Arc<Mutex<AsyncStorageInner>>,
}

#[derive(Debug, Default)]
struct AsyncStorageInner {
    queued: VecDeque<ExecuteRequest>,
    flushed: Vec<ExecuteRequest>,
}

impl AsyncStorageJournal {
    pub fn enqueue(&self, request: ExecuteRequest) {
        self.inner
            .lock()
            .expect("async storage lock poisoned")
            .queued
            .push_back(request);
    }

    pub fn flush_all(&self) -> usize {
        let mut inner = self.inner.lock().expect("async storage lock poisoned");
        let mut flushed = 0;
        while let Some(request) = inner.queued.pop_front() {
            inner.flushed.push(request);
            flushed += 1;
        }
        flushed
    }

    pub fn queued_len(&self) -> usize {
        self.inner
            .lock()
            .expect("async storage lock poisoned")
            .queued
            .len()
    }

    pub fn flushed_len(&self) -> usize {
        self.inner
            .lock()
            .expect("async storage lock poisoned")
            .flushed
            .len()
    }
}

#[derive(Debug, Clone)]
pub struct EndToEndWorkflow {
    shard_id: ShardId,
    meta: MetaRaftCluster,
    data: RaftCluster,
    switches: Arc<RwLock<KillSwitches>>,
    async_storage: AsyncStorageJournal,
    options: EndToEndWorkflowOptions,
}

impl EndToEndWorkflow {
    pub fn new(shard_id: ShardId, data_nodes: impl IntoIterator<Item = RaftNodeId>) -> Self {
        Self::with_options(shard_id, data_nodes, EndToEndWorkflowOptions::default())
    }

    pub fn with_options(
        shard_id: ShardId,
        data_nodes: impl IntoIterator<Item = RaftNodeId>,
        options: EndToEndWorkflowOptions,
    ) -> Self {
        assert_eq!(
            options.replication_mode,
            ReplicationMode::Raft,
            "Raft is the default and only enabled write-replication path in this workflow"
        );
        let data = RaftCluster::new_single_shard(shard_id, data_nodes);
        let meta = MetaRaftCluster::new([100, 101, 102]);
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            registered_at_ms: 0,
            preferred_location: String::new(),
            state: crate::meta::MetaEntityState::Normal,
            shard_id,
            server_addr: "raft://shard-leader".to_string(),
            latest_snapshot: None,
        }))
        .expect("initial shard route must replicate");
        Self {
            shard_id,
            meta,
            data,
            switches: Arc::new(RwLock::new(KillSwitches::default())),
            async_storage: AsyncStorageJournal::default(),
            options,
        }
    }

    pub fn replication_mode(&self) -> ReplicationMode {
        self.options.replication_mode
    }

    pub fn storage_mode(&self) -> SharedStoreStorageMode {
        self.options.storage_mode
    }

    pub fn raft_write_mode(&self) -> RaftWriteMode {
        self.options.raft_write_mode
    }

    pub fn proxy(&self) -> WorkflowProxy {
        WorkflowProxy {
            client: self.client(),
            switches: Arc::clone(&self.switches),
        }
    }

    pub fn client(&self) -> RoutingClient {
        self.client_with_options(TemporalStoreClientOptions::default())
    }

    pub fn client_with_options(&self, options: TemporalStoreClientOptions) -> RoutingClient {
        RoutingClient {
            shard_id: self.shard_id,
            meta: self.meta.clone(),
            data: self.data.clone(),
            switches: Arc::clone(&self.switches),
            async_storage: self.async_storage.clone(),
            storage_mode: self.options.storage_mode,
            raft_write_mode: self.options.raft_write_mode,
            options,
        }
    }

    pub fn set_kill_switches(&self, switches: KillSwitches) {
        *self.switches.write().expect("kill switch lock poisoned") = switches;
    }

    pub fn async_storage(&self) -> AsyncStorageJournal {
        self.async_storage.clone()
    }

    pub fn set_data_node_alive(
        &self,
        node_id: RaftNodeId,
        alive: bool,
    ) -> Result<(), WorkflowError> {
        Ok(self.data.set_alive(node_id, alive)?)
    }

    pub fn scale_up(&self, node_id: RaftNodeId) -> Result<(), WorkflowError> {
        ensure(
            self.switches.read().unwrap().scale_changes_enabled,
            "scale_changes",
        )?;
        Ok(self.data.add_node(node_id)?)
    }

    pub fn scale_down(&self, node_id: RaftNodeId) -> Result<(), WorkflowError> {
        ensure(
            self.switches.read().unwrap().scale_changes_enabled,
            "scale_changes",
        )?;
        Ok(self.data.remove_node(node_id)?)
    }

    pub fn read_data_node(
        &self,
        node_id: RaftNodeId,
        command: Command,
    ) -> Result<CommandResponse, WorkflowError> {
        Ok(self.data.read_local(node_id, command)?)
    }

    pub fn meta(&self) -> MetaRaftCluster {
        self.meta.clone()
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowProxy {
    client: RoutingClient,
    switches: Arc<RwLock<KillSwitches>>,
}

impl WorkflowProxy {
    pub fn execute(&self, request: ExecuteRequest) -> Result<ExecuteResponse, WorkflowError> {
        ensure(self.switches.read().unwrap().proxy_enabled, "proxy")?;
        self.client.execute(request)
    }
}

#[derive(Debug, Clone)]
pub struct RoutingClient {
    shard_id: ShardId,
    meta: MetaRaftCluster,
    data: RaftCluster,
    switches: Arc<RwLock<KillSwitches>>,
    async_storage: AsyncStorageJournal,
    storage_mode: SharedStoreStorageMode,
    raft_write_mode: RaftWriteMode,
    options: TemporalStoreClientOptions,
}

impl RoutingClient {
    pub fn execute(&self, request: ExecuteRequest) -> Result<ExecuteResponse, WorkflowError> {
        {
            let switches = self.switches.read().expect("kill switch lock poisoned");
            ensure(switches.client_enabled, "client")?;
            ensure(switches.data_nodes_enabled, "data_nodes")?;
        }

        let route = self
            .meta
            .get_shard_location_from_any_live(request.shard_id)?
            .ok_or(WorkflowError::MissingRoute(request.shard_id))?;
        if route.shard_id != self.shard_id {
            return Err(WorkflowError::MissingRoute(request.shard_id));
        }

        if is_write(&request.command) {
            self.execute_write(request)
        } else {
            self.execute_read(request)
        }
    }

    fn execute_write(&self, request: ExecuteRequest) -> Result<ExecuteResponse, WorkflowError> {
        {
            let switches = self.switches.read().expect("kill switch lock poisoned");
            ensure(switches.writes_enabled, "writes")?;
            ensure(switches.replication_enabled, "replication")?;
            if self.storage_mode.is_async() && switches.async_storage_enabled {
                self.async_storage.enqueue(request.clone());
            }
            let _raft_write_mode = self.raft_write_mode;
            if switches.secondary_promotion_enabled {
                let _ = self.data.promote_if_leader_down();
            }
        }

        let response = self.data.propose(request.command)?;
        Ok(ExecuteResponse {
            status: Status::ok(),
            response,
        })
    }

    fn execute_read(&self, request: ExecuteRequest) -> Result<ExecuteResponse, WorkflowError> {
        ensure(self.switches.read().unwrap().reads_enabled, "reads")?;
        let node_id = self.read_node_id();
        let response = self.data.read_from_replica(node_id, request.command)?;
        Ok(ExecuteResponse {
            status: Status::ok(),
            response,
        })
    }

    fn read_node_id(&self) -> RaftNodeId {
        if self.options.pin_primary
            || matches!(
                self.options.replica_read_policy,
                ReplicaReadPolicy::PinPrimary
            )
        {
            return self.data.leader_id();
        }
        match self.options.replica_read_policy {
            ReplicaReadPolicy::PinPrimary => self.data.leader_id(),
            ReplicaReadPolicy::Replica(node_id) => node_id,
            ReplicaReadPolicy::AnyReplica => self
                .data
                .live_replica_ids()
                .into_iter()
                .find(|node_id| *node_id != self.data.leader_id())
                .unwrap_or_else(|| self.data.leader_id()),
        }
    }
}

fn ensure(enabled: bool, name: &'static str) -> Result<(), WorkflowError> {
    if enabled {
        Ok(())
    } else {
        Err(WorkflowError::KillSwitch(name))
    }
}

fn is_write(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::ControlStateIncrement { .. }
            | Command::ControlStateIncrementWithOptions { .. }
            | Command::ControlStateSet { .. }
            | Command::ControlStateSetAndGet { .. }
            | Command::ControlStateSetAndGetWithOptions { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // shared-corpus: storage_data_raft_replication_reference
    #[test]
    fn e2e_uses_raft_replication_by_default() {
        let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
        assert_eq!(workflow.replication_mode(), ReplicationMode::Raft);
        assert_eq!(workflow.storage_mode(), SharedStoreStorageMode::Async);
        assert_eq!(workflow.raft_write_mode(), RaftWriteMode::Async);
        assert!(SharedStoreStorageMode::default().is_async());
        assert!(RaftWriteMode::default().is_async());
    }

    // shared-corpus: storage_data_raft_replication_reference
    #[test]
    fn e2e_allows_explicit_sync_storage_and_raft_modes() {
        let workflow = EndToEndWorkflow::with_options(
            1,
            [1, 2, 3],
            EndToEndWorkflowOptions {
                storage_mode: SharedStoreStorageMode::Sync,
                raft_write_mode: RaftWriteMode::Sync,
                ..EndToEndWorkflowOptions::default()
            },
        );
        assert_eq!(workflow.storage_mode(), SharedStoreStorageMode::Sync);
        assert_eq!(workflow.raft_write_mode(), RaftWriteMode::Sync);
        assert!(SharedStoreStorageMode::from_sync_flag(true).is_sync());
        assert!(RaftWriteMode::from_sync_flag(true).is_sync());
    }

    #[test]
    #[should_panic(expected = "Raft is the default and only enabled write-replication path")]
    fn e2e_rejects_shared_store_as_write_replication_path_for_now() {
        EndToEndWorkflow::with_options(
            1,
            [1, 2, 3],
            EndToEndWorkflowOptions {
                replication_mode: ReplicationMode::SharedStore,
                ..EndToEndWorkflowOptions::default()
            },
        );
    }

    #[test]
    fn proxy_client_datanode_e2e_replicates_write_to_followers() {
        let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
        let proxy = workflow.proxy();
        proxy
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .unwrap();

        for node_id in [1, 2, 3] {
            assert_eq!(
                workflow
                    .read_data_node(
                        node_id,
                        Command::StringGet {
                            key: "k".to_string()
                        },
                    )
                    .unwrap(),
                CommandResponse::Bytes {
                    value: Some(b"v".to_vec())
                }
            );
        }
    }

    #[test]
    fn e2e_primary_down_promotes_secondary_and_continues() {
        let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
        workflow.set_data_node_alive(1, false).unwrap();
        workflow
            .proxy()
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"promoted".to_vec(),
                },
            })
            .unwrap();

        assert_eq!(
            workflow
                .read_data_node(
                    2,
                    Command::StringGet {
                        key: "k".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"promoted".to_vec())
            }
        );
    }

    #[test]
    fn e2e_reads_pin_primary_by_default() {
        let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
        workflow.set_data_node_alive(2, false).unwrap();
        workflow
            .proxy()
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"primary".to_vec(),
                },
            })
            .unwrap();

        assert_eq!(
            workflow
                .client()
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string()
                    },
                })
                .unwrap()
                .response,
            CommandResponse::Bytes {
                value: Some(b"primary".to_vec())
            }
        );
    }

    #[test]
    fn e2e_can_read_from_secondary_replica_when_not_pinned() {
        let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
        workflow
            .proxy()
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"replica".to_vec(),
                },
            })
            .unwrap();
        let client = workflow.client_with_options(TemporalStoreClientOptions {
            pin_primary: false,
            replica_read_policy: ReplicaReadPolicy::Replica(2),
        });

        assert_eq!(
            client
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string()
                    },
                })
                .unwrap()
                .response,
            CommandResponse::Bytes {
                value: Some(b"replica".to_vec())
            }
        );
    }

    #[test]
    fn e2e_async_storage_queues_and_flushes_without_blocking_response() {
        let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
        workflow
            .proxy()
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            })
            .unwrap();

        assert_eq!(workflow.async_storage().queued_len(), 1);
        assert_eq!(workflow.async_storage().flushed_len(), 0);
        assert_eq!(workflow.async_storage().flush_all(), 1);
        assert_eq!(workflow.async_storage().queued_len(), 0);
        assert_eq!(workflow.async_storage().flushed_len(), 1);
    }

    #[test]
    fn e2e_kill_switches_block_proxy_writes_reads_replication_and_scale() {
        let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
        let mut switches = KillSwitches::default();
        switches.writes_enabled = false;
        workflow.set_kill_switches(switches);
        assert_eq!(
            workflow
                .proxy()
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .unwrap_err(),
            WorkflowError::KillSwitch("writes")
        );

        let mut switches = KillSwitches::default();
        switches.reads_enabled = false;
        workflow.set_kill_switches(switches);
        assert_eq!(
            workflow
                .proxy()
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string(),
                    },
                })
                .unwrap_err(),
            WorkflowError::KillSwitch("reads")
        );

        let mut switches = KillSwitches::default();
        switches.replication_enabled = false;
        workflow.set_kill_switches(switches);
        assert_eq!(
            workflow
                .proxy()
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .unwrap_err(),
            WorkflowError::KillSwitch("replication")
        );

        let mut switches = KillSwitches::default();
        switches.scale_changes_enabled = false;
        workflow.set_kill_switches(switches);
        assert_eq!(
            workflow.scale_up(4).unwrap_err(),
            WorkflowError::KillSwitch("scale_changes")
        );
    }

    #[test]
    fn e2e_scale_up_and_scale_down_keep_workflow_available() {
        let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
        workflow.scale_up(4).unwrap();
        workflow.scale_down(3).unwrap();
        workflow
            .proxy()
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"scaled".to_vec(),
                },
            })
            .unwrap();
        assert_eq!(
            workflow
                .read_data_node(
                    4,
                    Command::StringGet {
                        key: "k".to_string()
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"scaled".to_vec())
            }
        );
    }
}
