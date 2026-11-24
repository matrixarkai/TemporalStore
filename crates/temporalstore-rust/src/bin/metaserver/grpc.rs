// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::MetaBackend;
use prost::Message;
use std::io;
use std::net::TcpListener;
use temporalstore_rust::meta::{
    GetTableTopologyRequest, ListShardsRequest, ProxyHeartbeatRequest, RegisterProxyRequest,
    ServerEndpoint, TableTopologyResponse, TopologyVersionRequest,
};
use temporalstore_rust::metaserver_files_proto::{
    table_meta_info_to_proto, topology_change_event_to_proto,
};
use temporalstore_rust::sdk::v1;
use tonic::transport::{server::TcpIncoming, Server};
use tonic::{Request, Response, Status};

#[derive(Clone)]
struct MetadataRpc {
    backend: MetaBackend,
    token: Option<String>,
}

impl MetadataRpc {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
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
        Ok(())
    }
}

#[tonic::async_trait]
impl v1::metadata_service_server::MetadataService for MetadataRpc {
    async fn get_table_topology(
        &self,
        request: Request<v1::MetadataTableTopologyRequest>,
    ) -> Result<Response<v1::MetadataTableTopologyResponse>, Status> {
        self.authorize(&request)?;
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

    async fn get_topology_version(
        &self,
        request: Request<v1::MetadataVersionRequest>,
    ) -> Result<Response<v1::MetadataVersionResponse>, Status> {
        self.authorize(&request)?;
        let query = TopologyVersionRequest {
            old_topology_version: request.into_inner().old_topology_version,
        };
        let backend = self.backend.clone();
        let report = tokio::task::spawn_blocking(move || match &backend {
            MetaBackend::Single(meta) => meta.topology_version_report(query),
            MetaBackend::Raft(runtime) => runtime.cluster().topology_version_report(query),
        })
        .await
        .map_err(|_| Status::internal("topology version lookup failed"))?;
        Ok(Response::new(v1::MetadataVersionResponse {
            status: Some(wire_status(report.status)),
            current_topology_version: report.current_topology_version,
            old_topology_version: report.old_topology_version,
            unchanged: report.unchanged,
            server_count: report.server_count as u64,
            proxy_count: report.proxy_count as u64,
            table_count: report.table_count as u64,
            shard_route_count: report.shard_route_count as u64,
            normal_servers: report.normal_servers as u64,
            frozen_servers: report.frozen_servers as u64,
            dropped_servers: report.dropped_servers as u64,
            normal_proxies: report.normal_proxies as u64,
            frozen_proxies: report.frozen_proxies as u64,
            dropped_proxies: report.dropped_proxies as u64,
            normal_tables: report.normal_tables as u64,
            frozen_tables: report.frozen_tables as u64,
            dropped_tables: report.dropped_tables as u64,
            changed_tables: report
                .changed_tables
                .iter()
                .map(table_meta_info_to_proto)
                .collect(),
            events: report
                .events
                .iter()
                .map(topology_change_event_to_proto)
                .collect(),
            event_history_truncated: report.event_history_truncated,
        }))
    }
}

#[tonic::async_trait]
impl v1::proxy_metadata_service_server::ProxyMetadataService for MetadataRpc {
    async fn register_proxy(
        &self,
        request: Request<v1::MetadataRegisterProxyRequest>,
    ) -> Result<Response<v1::Status>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let query = RegisterProxyRequest {
            proxy_addr: request.proxy_addr,
            namespace: request.namespace_name,
            location: request.location,
            config_version: request.config_version,
            binary_version: request.binary_version,
            registered_at_ms: 0,
        };
        let backend = self.backend.clone();
        let reply = tokio::task::spawn_blocking(move || match &backend {
            MetaBackend::Single(meta) => meta.register_proxy(query),
            MetaBackend::Raft(runtime) => runtime.cluster().register_proxy(query),
        })
        .await
        .map_err(|_| Status::internal("proxy registration failed"))?;
        Ok(Response::new(wire_status(reply.status)))
    }

    async fn heartbeat(
        &self,
        request: Request<v1::MetadataProxyHeartbeatRequest>,
    ) -> Result<Response<v1::MetadataProxyHeartbeatResponse>, Status> {
        self.authorize(&request)?;
        let request = request.into_inner();
        let query = ProxyHeartbeatRequest {
            proxy_addr: request.proxy_addr,
            boot_time_ms: request.boot_time_ms,
            namespace: request.namespace_name,
            config_version: request.config_version,
            binary_version: request.binary_version,
        };
        let backend = self.backend.clone();
        let reply = tokio::task::spawn_blocking(move || match &backend {
            MetaBackend::Single(meta) => meta.proxy_heartbeat(query),
            MetaBackend::Raft(runtime) => runtime.cluster().proxy_heartbeat(query),
        })
        .await
        .map_err(|_| Status::internal("proxy heartbeat failed"))?;
        Ok(Response::new(v1::MetadataProxyHeartbeatResponse {
            status: Some(wire_status(reply.status)),
            config_changed: reply.config_changed,
            namespace_name: reply.namespace,
            config_version: reply.config_version,
            serving_mode: reply.serving_mode,
            drop_percent: reply.drop_percent.map(u32::from),
        }))
    }

    async fn get_leader(
        &self,
        request: Request<v1::MetadataLeaderRequest>,
    ) -> Result<Response<v1::MetadataLeaderResponse>, Status> {
        self.authorize(&request)?;
        let backend = self.backend.clone();
        let reply = tokio::task::spawn_blocking(move || backend.raft_leader())
            .await
            .map_err(|_| Status::internal("metaserver leader lookup failed"))?;
        Ok(Response::new(v1::MetadataLeaderResponse {
            status: Some(wire_status(reply.status)),
            leader_id: reply.leader_id,
            addr: reply.addr,
            is_local: reply.is_local,
        }))
    }

    async fn list_shard_ids(
        &self,
        request: Request<v1::MetadataListShardIdsRequest>,
    ) -> Result<Response<v1::MetadataListShardIdsResponse>, Status> {
        self.authorize(&request)?;
        let backend = self.backend.clone();
        let reply = tokio::task::spawn_blocking(move || match &backend {
            MetaBackend::Single(meta) => meta.list_shards(ListShardsRequest::default()),
            MetaBackend::Raft(runtime) => {
                runtime.cluster().list_shards(ListShardsRequest::default())
            }
        })
        .await
        .map_err(|_| Status::internal("metaserver shard listing failed"))?;
        Ok(Response::new(v1::MetadataListShardIdsResponse {
            status: Some(wire_status(reply.status)),
            shard_ids: reply
                .shards
                .into_iter()
                .map(|shard| shard.shard_id)
                .collect(),
            next_after_shard_id: reply.next_after_shard_id,
        }))
    }
}

fn wire_status(status: temporalstore_rust::types::Status) -> v1::Status {
    v1::Status {
        ok: status.ok,
        code: status.code,
        message: status.message,
    }
}

fn wire_endpoint(endpoint: ServerEndpoint) -> v1::MetadataServerEndpoint {
    v1::MetadataServerEndpoint {
        server_addr: endpoint.server_addr,
        grpc_addr: endpoint.grpc_addr,
        location: endpoint.location,
    }
}

fn topology_response(topology: TableTopologyResponse) -> v1::MetadataTableTopologyResponse {
    let mut response = v1::MetadataTableTopologyResponse {
        status: Some(wire_status(topology.status)),
        unchanged: topology.unchanged,
        ..Default::default()
    };
    if let Some(table) = topology.table {
        response.table_id = table.table_id;
        response.topology_version = table.topology_version;
        response.first_shard_id = table.first_shard_id;
        response.shard_count = table.shard_count;
        response.table = Some(table_meta_info_to_proto(&table));
    }
    response.shards = topology
        .shards
        .into_iter()
        .map(|route| {
            let elected = route.raft_route.as_ref();
            let (primary_addr, primary_grpc, load_version) = match elected {
                Some(elected) => (
                    elected.endpoint.as_ref().map(|ep| ep.server_addr.clone()),
                    elected
                        .endpoint
                        .as_ref()
                        .and_then(|ep| ep.grpc_addr.clone()),
                    elected.load_version,
                ),
                None => (
                    route.primary.clone(),
                    route
                        .primary_endpoint
                        .as_ref()
                        .and_then(|ep| ep.grpc_addr.clone()),
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
                    .iter()
                    .filter_map(|ep| ep.grpc_addr.clone())
                    .collect(),
                load_version,
                uses_raft_route: elected.is_some(),
                start_bucket: route.start_bucket,
                end_bucket: route.end_bucket,
                placement_primary: route.primary,
                placement_endpoint: route.primary_endpoint.map(wire_endpoint),
                replica_endpoints: route
                    .replica_endpoints
                    .into_iter()
                    .map(wire_endpoint)
                    .collect(),
                placement_load_version: route.load_version,
                elected_endpoint: route
                    .raft_route
                    .and_then(|elected| elected.endpoint)
                    .map(wire_endpoint),
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
                let rpc = MetadataRpc { backend, token };
                let topology = v1::metadata_service_server::MetadataServiceServer::new(rpc.clone())
                    .max_encoding_message_size(40 * 1024 * 1024);
                let proxy_control =
                    v1::proxy_metadata_service_server::ProxyMetadataServiceServer::new(rpc)
                        .max_encoding_message_size(40 * 1024 * 1024);
                tracing::info!(%address, "temporalstore metaserver gRPC listening");
                Server::builder()
                    .add_service(topology)
                    .add_service(proxy_control)
                    .serve_with_incoming(incoming)
                    .await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }) {
                tracing::error!(%address, %error, "metaserver gRPC listener failed");
            }
        })?;
    Ok(())
}
