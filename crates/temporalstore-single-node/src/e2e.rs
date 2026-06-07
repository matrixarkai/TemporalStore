use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::meta::ShardLocation;
use crate::raft::{MetaCommand, MetaRaftCluster, RaftCluster, RaftError, RaftNodeId};
use crate::types::{Command, CommandResponse, ExecuteRequest, ExecuteResponse, ShardId, Status};

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
}

impl EndToEndWorkflow {
    pub fn new(shard_id: ShardId, data_nodes: impl IntoIterator<Item = RaftNodeId>) -> Self {
        let data = RaftCluster::new_single_shard(shard_id, data_nodes);
        let meta = MetaRaftCluster::new([100, 101, 102]);
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
            shard_id,
            server_addr: "raft://shard-leader".to_string(),
        }))
        .expect("initial shard route must replicate");
        Self {
            shard_id,
            meta,
            data,
            switches: Arc::new(RwLock::new(KillSwitches::default())),
            async_storage: AsyncStorageJournal::default(),
        }
    }

    pub fn proxy(&self) -> WorkflowProxy {
        WorkflowProxy {
            client: self.client(),
            switches: Arc::clone(&self.switches),
        }
    }

    pub fn client(&self) -> RoutingClient {
        RoutingClient {
            shard_id: self.shard_id,
            meta: self.meta.clone(),
            data: self.data.clone(),
            switches: Arc::clone(&self.switches),
            async_storage: self.async_storage.clone(),
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
            .get_shard_location(100, request.shard_id)?
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
            if switches.async_storage_enabled {
                self.async_storage.enqueue(request.clone());
            }
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
        let response = self.data.read_local(1, request.command)?;
        Ok(ExecuteResponse {
            status: Status::ok(),
            response,
        })
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
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::IpsAdd { .. }
            | Command::RiskIncrement { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

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
