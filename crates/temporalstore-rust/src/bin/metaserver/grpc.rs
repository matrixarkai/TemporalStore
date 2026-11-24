// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::MetaBackend;
use prost::Message;
use std::io;
use std::net::TcpListener;
use temporalstore_rust::meta::{GetTableTopologyRequest, TableTopologyResponse};
use temporalstore_rust::sdk::v1;
use tonic::transport::{server::TcpIncoming, Server};
use tonic::{Request, Response, Status};

struct MetadataRpc {
    backend: MetaBackend,
    token: Option<String>,
}

#[tonic::async_trait]
impl v1::metadata_service_server::MetadataService for MetadataRpc {
    async fn get_table_topology(
        &self,
        request: Request<v1::MetadataTableTopologyRequest>,
    ) -> Result<Response<v1::MetadataTableTopologyResponse>, Status> {
        if let Some(required) = self.token.as_deref() {
            let provided = request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "));
            if provided != Some(required) {
                return Err(Status::unauthenticated(
                    "missing or invalid metaserver credential",
                ));
            }
        }
        let request = request.into_inner();
        if request.namespace_name.is_empty() || request.table_name.is_empty() {
            return Err(Status::invalid_argument("namespace and table are required"));
        }
        let query = GetTableTopologyRequest {
            namespace: request.namespace_name,
            table_name: request.table_name,
            client_location: request.client_location,
            old_topology_version: request.old_topology_version,
        };
        let backend = self.backend.clone();
        let response = tokio::task::spawn_blocking(move || {
            let topology = match &backend {
                MetaBackend::Single(meta) => meta.get_table_topology(query),
                MetaBackend::Raft(runtime) => runtime.cluster().get_table_topology(query),
            };
            let response = topology_response(topology);
            backend.record_topology_bytes(response.encoded_len());
            response
        })
        .await
        .map_err(|_| Status::internal("topology lookup failed"))?;
        Ok(Response::new(response))
    }
}

fn topology_response(topology: TableTopologyResponse) -> v1::MetadataTableTopologyResponse {
    let mut response = v1::MetadataTableTopologyResponse {
        status: Some(v1::Status {
            ok: topology.status.ok,
            code: topology.status.code,
            message: topology.status.message,
        }),
        unchanged: topology.unchanged,
        ..Default::default()
    };
    if let Some(table) = topology.table {
        response.table_id = table.table_id;
        response.topology_version = table.topology_version;
        response.first_shard_id = table.first_shard_id;
        response.shard_count = table.shard_count;
    }
    response.shards = topology
        .shards
        .into_iter()
        .map(|route| {
            let uses_raft_route = route.raft_route.is_some();
            let (primary_addr, primary_grpc, load_version) = match route.raft_route {
                Some(elected) => {
                    let (address, grpc) = elected
                        .endpoint
                        .map(|ep| (Some(ep.server_addr), ep.grpc_addr))
                        .unwrap_or_default();
                    (address, grpc, elected.load_version)
                }
                None => (
                    route.primary,
                    route
                        .primary_endpoint
                        .and_then(|endpoint| endpoint.grpc_addr),
                    route.load_version,
                ),
            };
            v1::MetadataShardRoute {
                shard_id: route.shard_id,
                primary_addr,
                primary_grpc,
                replica_addrs: route.replicas,
                replica_grpc: route
                    .replica_endpoints
                    .into_iter()
                    .filter_map(|endpoint| endpoint.grpc_addr)
                    .collect(),
                load_version,
                uses_raft_route,
            }
        })
        .collect();
    response
}

pub(super) fn start(addr: &str, backend: MetaBackend, token: Option<String>) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let address = listener.local_addr()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    std::thread::Builder::new()
        .name("ts-meta-grpc".to_owned())
        .spawn(move || {
            if let Err(error) = runtime.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener)?;
                let incoming = TcpIncoming::from_listener(listener, true, None)?;
                let service =
                    v1::metadata_service_server::MetadataServiceServer::new(MetadataRpc {
                        backend,
                        token,
                    })
                    .max_encoding_message_size(40 * 1024 * 1024);
                tracing::info!(%address, "temporalstore metaserver gRPC listening");
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming(incoming)
                    .await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }) {
                tracing::error!(%address, %error, "metaserver gRPC listener failed");
            }
        })?;
    Ok(())
}
