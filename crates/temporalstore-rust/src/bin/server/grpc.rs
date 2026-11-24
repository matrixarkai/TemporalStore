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
    execute_via_server_raft, BatchExecuteRequest, Command, CommandResponse, DataNodeRuntime,
    ExecuteRequest, ExecuteResponse, ServerRaftState, ShardId, Status,
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
            Ok(()) => execute_via_server_raft(raft, request),
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
        let responses: Vec<_> = request
            .commands
            .into_iter()
            .map(|command| {
                execute_via_server_raft(
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
