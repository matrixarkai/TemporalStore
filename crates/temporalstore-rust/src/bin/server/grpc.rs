// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::io;
use std::net::{SocketAddr, TcpListener};

use temporalstore_rust::sdk::{
    v1::temporal_store_service_server::TemporalStoreServiceServer, TemporalStoreSdkExecutor,
    TemporalStoreTonicAdapter,
};
use temporalstore_rust::types::BatchExecuteResponse;
use tonic::transport::{server::TcpIncoming, Server};

use super::{
    is_raft_read_command, BatchExecuteRequest, Command, CommandResponse, DataNodeRuntime,
    DataRaftReadMode, ExecuteRequest, ExecuteResponse, ServerRaftState, ShardId, Status,
};

const MAX_MESSAGE_BYTES: usize = 40 * 1024 * 1024;

struct DataNodeExecutor {
    runtime: DataNodeRuntime,
    raft: Option<ServerRaftState>,
}

impl DataNodeExecutor {
    fn validate_raft_request(
        &self,
        raft: &ServerRaftState,
        shard_id: ShardId,
        commands: &[Command],
    ) -> Result<(), Status> {
        if shard_id != raft.raft_shard_id {
            return Err(Status::error(
                "shard_mismatch",
                format!("this Raft group serves shard {}", raft.raft_shard_id),
            ));
        }
        self.runtime
            .validate_foreground_write_allowed(shard_id, commands)
    }
}

fn sdk_read_target(
    state: &ServerRaftState,
) -> Result<temporalstore_rust::RaftCluster, temporalstore_rust::RaftError> {
    let cluster = state.runtime.cluster();
    let node_id = state.local_node_id;
    let primary_only = matches!(
        state.read_policy.mode,
        DataRaftReadMode::Leader | DataRaftReadMode::Linearizable
    );
    if primary_only && !cluster.is_local_leader(node_id) {
        return Err(temporalstore_rust::RaftError::NotLeader { node_id });
    }
    let read = cluster.check_data_raft_read_policy(node_id, state.read_policy)?;
    cluster.wait_for_applied_index(
        node_id,
        read.read_index,
        state.read_policy.read_index_timeout_ms,
    )?;
    if primary_only && !cluster.is_local_leader(node_id) {
        return Err(temporalstore_rust::RaftError::NotLeader { node_id });
    }
    Ok(cluster)
}

fn sdk_read_error(error: temporalstore_rust::RaftError) -> Status {
    let code = if matches!(&error, temporalstore_rust::RaftError::NotLeader { .. }) {
        "raft_not_leader"
    } else {
        "raft_error"
    };
    Status::error(code, error.to_string())
}

pub(super) fn execute_raft_sdk_command(
    raft: &ServerRaftState,
    request: ExecuteRequest,
) -> ExecuteResponse {
    if is_raft_read_command(&request.command) {
        return match sdk_read_target(raft)
            .and_then(|cluster| cluster.read_from_replica_with_status(raft.local_node_id, request.command))
        {
            Ok(response) => response,
            Err(error) => ExecuteResponse {
                status: sdk_read_error(error),
                response: CommandResponse::Empty,
            },
        };
    }
    match raft.runtime.propose_with_admission(request.command) {
        Ok(response) => ExecuteResponse {
            status: Status::ok(),
            response,
        },
        Err(error) => {
            let code = if matches!(
                &error,
                temporalstore_rust::RaftError::ProposalNotStarted { .. }
            ) {
                "raft_write_not_started"
            } else {
                "raft_error"
            };
            ExecuteResponse {
                status: Status::error(code, error.to_string()),
                response: CommandResponse::Empty,
            }
        }
    }
}

impl TemporalStoreSdkExecutor for DataNodeExecutor {
    fn execute_checked_sdk(&self, request: ExecuteRequest, load_version: u64) -> ExecuteResponse {
        self.runtime
            .with_load_version(request.shard_id, load_version, || self.execute_sdk(request))
            .unwrap_or_else(|status| ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            })
    }

    fn batch_execute_checked_sdk(
        &self,
        request: BatchExecuteRequest,
        load_version: u64,
    ) -> BatchExecuteResponse {
        self.runtime
            .with_load_version(request.shard_id, load_version, || {
                self.batch_execute_sdk(request)
            })
            .unwrap_or_else(|status| BatchExecuteResponse {
                status,
                responses: Vec::new(),
            })
    }

    fn execute_sdk(&self, request: ExecuteRequest) -> ExecuteResponse {
        let Some(raft) = &self.raft else {
            return self.runtime.execute(request);
        };
        match self.validate_raft_request(
            raft,
            request.shard_id,
            std::slice::from_ref(&request.command),
        ) {
            Ok(()) => execute_raft_sdk_command(raft, request),
            Err(status) => ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            },
        }
    }

    fn batch_execute_sdk(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        let Some(raft) = &self.raft else {
            return self.runtime.batch_execute(request);
        };
        if let Err(status) = self.validate_raft_request(raft, request.shard_id, &request.commands) {
            return BatchExecuteResponse {
                status,
                responses: Vec::new(),
            };
        }
        if request.commands.len() > 1 && request.commands.iter().all(is_raft_read_command) {
            let count = request.commands.len();
            let result = sdk_read_target(raft).and_then(|cluster| {
                cluster.read_batch_from_replica_with_status(raft.local_node_id, request.commands)
            });
            return match result {
                Ok(responses) => BatchExecuteResponse {
                    status: Status::ok(),
                    responses,
                },
                Err(error) => {
                    let status = sdk_read_error(error);
                    BatchExecuteResponse {
                        responses: vec![
                            ExecuteResponse {
                                status: status.clone(),
                                response: CommandResponse::Empty,
                            };
                            count
                        ],
                        status,
                    }
                }
            };
        }

        let responses: Vec<_> = request
            .commands
            .into_iter()
            .map(|command| {
                execute_raft_sdk_command(
                    raft,
                    ExecuteRequest {
                        shard_id: request.shard_id,
                        command,
                    },
                )
            })
            .collect();
        let status = responses
            .iter()
            .find(|response| !response.status.ok)
            .map_or_else(Status::ok, |response| response.status.clone());
        BatchExecuteResponse { status, responses }
    }
}

pub(super) fn start(
    addr: &str,
    runtime: DataNodeRuntime,
    raft: Option<ServerRaftState>,
) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let executor = DataNodeExecutor { runtime, raft };
    let transport = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    std::thread::Builder::new()
        .name("ts-grpc".to_string())
        .spawn(move || {
            if let Err(error) = transport.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener)?;
                let incoming = TcpIncoming::from_listener(listener, true, None)?;
                let service =
                    TemporalStoreServiceServer::new(TemporalStoreTonicAdapter::new(executor))
                        .max_decoding_message_size(MAX_MESSAGE_BYTES)
                        .max_encoding_message_size(MAX_MESSAGE_BYTES);
                tracing::info!(%address, "temporalstore gRPC listener ready");
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming(incoming)
                    .await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }) {
                tracing::error!(%address, %error, "temporalstore gRPC listener failed");
            }
        })?;
    Ok(address)
}

#[cfg(test)]
#[path = "typed_read_status_tests.rs"]
mod typed_read_status_tests;
