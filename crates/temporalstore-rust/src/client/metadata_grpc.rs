// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::meta::{
    AckResponse, DataRaftRoute, GetTableTopologyRequest, MetaRaftLeaderResponse,
    ProxyHeartbeatRequest, ProxyHeartbeatResponse, RegisterProxyRequest, RegisterServerRequest,
    RegisterShardRequest, ServerHeartbeatRequest, ServerHeartbeatResponse, ShardCacheBudget,
    TableShard, TableTopologyResponse, TopologyVersionReport,
};
use crate::metaserver_files_proto::{
    numa_node_to_proto, server_runtime_load_to_proto, server_shard_serving_state_to_proto,
    shard_load_to_proto, shard_stat_load_to_proto, table_meta_info_from_proto,
    topology_change_event_from_proto,
};
use crate::sdk::v1;
use std::future::Future;
use std::sync::{mpsc, OnceLock};
use tokio::runtime::{Builder, Runtime};
use tonic::transport::{Channel, Endpoint};

type MetadataClient = v1::metadata_service_client::MetadataServiceClient<Channel>;
type DatanodeMetadataClient =
    v1::datanode_metadata_service_client::DatanodeMetadataServiceClient<Channel>;
type ProxyMetadataClient = v1::proxy_metadata_service_client::ProxyMetadataServiceClient<Channel>;

pub(super) fn runtime() -> Result<&'static Runtime, ClientError> {
    static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("ts-sdk-grpc")
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| ClientError::Status(error.clone()))
}

fn decode_status(status: Option<v1::Status>) -> Result<Status, ClientError> {
    let status =
        status.ok_or_else(|| ClientError::Status("missing metaserver status".to_owned()))?;
    Ok(Status {
        ok: status.ok,
        code: status.code,
        message: status.message,
    })
}

fn decode_endpoint(endpoint: v1::MetadataServerEndpoint) -> ServerEndpoint {
    ServerEndpoint {
        server_addr: endpoint.server_addr,
        grpc_addr: endpoint.grpc_addr,
        location: endpoint.location,
    }
}

fn decode_topology(
    response: v1::MetadataTableTopologyResponse,
) -> Result<TableTopologyResponse, ClientError> {
    let status = decode_status(response.status)?;
    let table = response
        .table
        .map(table_meta_info_from_proto)
        .transpose()
        .map_err(ClientError::Status)?;
    let shards = response
        .shards
        .into_iter()
        .map(|shard| TableShard {
            shard_id: shard.shard_id,
            start_bucket: shard.start_bucket,
            end_bucket: shard.end_bucket,
            primary: shard.placement_primary,
            replicas: shard.replica_addrs,
            primary_endpoint: shard.placement_endpoint.map(decode_endpoint),
            replica_endpoints: shard
                .replica_endpoints
                .into_iter()
                .map(decode_endpoint)
                .collect(),
            load_version: shard.placement_load_version,
            raft_route: shard.uses_raft_route.then(|| DataRaftRoute {
                endpoint: shard.elected_endpoint.map(decode_endpoint),
                load_version: shard.load_version,
            }),
        })
        .collect();
    Ok(TableTopologyResponse {
        status,
        table,
        shards,
        unchanged: response.unchanged,
    })
}

impl TemporalStoreClient {
    fn metadata_rpc<T, F>(
        &self,
        target: &str,
        timeout_ms: u64,
        call: impl FnOnce(Channel, Option<String>) -> F + Send + 'static,
    ) -> Result<T, ClientError>
    where
        T: Send + 'static,
        F: Future<Output = Result<tonic::Response<T>, tonic::Status>> + Send + 'static,
    {
        let runtime = runtime()?;
        let channel = {
            let mut cached = self
                .inner
                .meta_grpc_channel
                .lock()
                .expect("metadata channel lock poisoned");
            match cached.as_ref() {
                Some((cached_target, channel)) if cached_target == target => channel.clone(),
                _ => {
                    let address = target.strip_prefix("grpc://").unwrap_or(target);
                    let endpoint = Endpoint::from_shared(format!("http://{address}"))
                        .map_err(|error| ClientError::InvalidRequest(error.to_string()))?
                        .connect_timeout(Duration::from_millis(
                            self.inner.options.connect_timeout_ms,
                        ));
                    let _guard = runtime.enter();
                    let channel = endpoint.connect_lazy();
                    *cached = Some((target.to_owned(), channel.clone()));
                    channel
                }
            }
        };
        let token = crate::meta::admin_auth_token();
        let (send, receive) = mpsc::sync_channel(1);
        runtime.spawn(async move {
            let result =
                tokio::time::timeout(Duration::from_millis(timeout_ms), call(channel, token))
                    .await
                    .unwrap_or_else(|_| {
                        Err(tonic::Status::deadline_exceeded(
                            "metaserver request timed out",
                        ))
                    });
            let _ = send.send(result);
        });
        receive
            .recv()
            .map_err(|error| ClientError::Status(error.to_string()))?
            .map(tonic::Response::into_inner)
            .map_err(|error| ClientError::Status(error.to_string()))
    }

    pub(super) fn get_table_topology_grpc(
        &self,
        target: &str,
        req: GetTableTopologyRequest,
    ) -> Result<TableTopologyResponse, ClientError> {
        let timeout_ms = self.inner.options.meta_sync_http_options().io_timeout_ms;
        let response = self.metadata_rpc(target, timeout_ms, move |channel, token| async move {
            let mut client =
                MetadataClient::new(channel).max_decoding_message_size(40 * 1024 * 1024);
            let request = authenticated(
                v1::MetadataTableTopologyRequest {
                    namespace_name: req.namespace,
                    table_name: req.table_name,
                    client_location: req.client_location,
                    old_topology_version: req.old_topology_version,
                },
                token,
            )?;
            client.get_table_topology(request).await
        })?;
        decode_topology(response)
    }

    pub(super) fn get_topology_version_grpc(
        &self,
        target: &str,
        old_topology_version: u64,
    ) -> Result<TopologyVersionReport, ClientError> {
        let timeout_ms = self.inner.options.meta_sync_http_options().io_timeout_ms;
        let response = self.metadata_rpc(target, timeout_ms, move |channel, token| async move {
            let mut client =
                MetadataClient::new(channel).max_decoding_message_size(40 * 1024 * 1024);
            let request = authenticated(
                v1::MetadataVersionRequest {
                    old_topology_version,
                },
                token,
            )?;
            client.get_topology_version(request).await
        })?;
        Ok(TopologyVersionReport {
            status: decode_status(response.status)?,
            current_topology_version: response.current_topology_version,
            old_topology_version: response.old_topology_version,
            unchanged: response.unchanged,
            server_count: response.server_count as usize,
            proxy_count: response.proxy_count as usize,
            table_count: response.table_count as usize,
            shard_route_count: response.shard_route_count as usize,
            normal_servers: response.normal_servers as usize,
            frozen_servers: response.frozen_servers as usize,
            dropped_servers: response.dropped_servers as usize,
            normal_proxies: response.normal_proxies as usize,
            frozen_proxies: response.frozen_proxies as usize,
            dropped_proxies: response.dropped_proxies as usize,
            normal_tables: response.normal_tables as usize,
            frozen_tables: response.frozen_tables as usize,
            dropped_tables: response.dropped_tables as usize,
            changed_tables: response
                .changed_tables
                .into_iter()
                .map(table_meta_info_from_proto)
                .collect::<Result<_, _>>()
                .map_err(ClientError::Status)?,
            events: response
                .events
                .into_iter()
                .map(topology_change_event_from_proto)
                .collect::<Result<_, _>>()
                .map_err(ClientError::Status)?,
            event_history_truncated: response.event_history_truncated,
        })
    }

    /// Register a distributed datanode on its configured metaserver gRPC endpoint.
    pub fn register_datanode_grpc(
        &self,
        target: &str,
        query: &RegisterServerRequest,
    ) -> Result<Status, ClientError> {
        let request = v1::MetadataRegisterServerRequest {
            server_addr: query.server_addr.clone(),
            grpc_addr: query.grpc_addr.clone(),
            node_id: query.node_id,
            location: query.location.clone(),
            binary_version: query.binary_version.clone(),
            numa_nodes: query.numa_nodes.iter().map(numa_node_to_proto).collect(),
        };
        let reply = self.metadata_rpc(
            target,
            self.inner.options.io_timeout_ms,
            move |channel, token| async move {
                DatanodeMetadataClient::new(channel)
                    .register_server(authenticated(request, token)?)
                    .await
            },
        )?;
        decode_status(Some(reply))
    }

    /// Publish the initial placement of a shard hosted by this datanode.
    pub fn register_datanode_shard_grpc(
        &self,
        target: &str,
        query: &RegisterShardRequest,
    ) -> Result<Status, ClientError> {
        let request = v1::MetadataRegisterShardRequest {
            shard_id: query.shard_id,
            server_addr: query.server_addr.clone(),
        };
        let reply = self.metadata_rpc(
            target,
            self.inner.options.io_timeout_ms,
            move |channel, token| async move {
                DatanodeMetadataClient::new(channel)
                    .register_shard(authenticated(request, token)?)
                    .await
            },
        )?;
        decode_status(Some(reply))
    }

    /// Report load and serving state and receive the current cache budgets.
    pub fn datanode_heartbeat_grpc(
        &self,
        target: &str,
        query: &ServerHeartbeatRequest,
    ) -> Result<ServerHeartbeatResponse, ClientError> {
        let request = v1::MetadataServerHeartbeatRequest {
            server_addr: query.server_addr.clone(),
            boot_time_ms: query.boot_time_ms,
            binary_version: query.binary_version.clone(),
            shard_loads: query.shard_loads.iter().map(shard_load_to_proto).collect(),
            shard_stat_loads: query
                .shard_stat_loads
                .iter()
                .map(shard_stat_load_to_proto)
                .collect(),
            runtime_load: Some(server_runtime_load_to_proto(&query.runtime_load)),
            shard_states: query
                .shard_states
                .iter()
                .map(server_shard_serving_state_to_proto)
                .collect(),
        };
        let reply = self.metadata_rpc(
            target,
            self.inner.options.io_timeout_ms,
            move |channel, token| async move {
                DatanodeMetadataClient::new(channel)
                    .max_encoding_message_size(40 * 1024 * 1024)
                    .max_decoding_message_size(40 * 1024 * 1024)
                    .heartbeat(authenticated(request, token)?)
                    .await
            },
        )?;
        Ok(ServerHeartbeatResponse {
            status: decode_status(reply.status)?,
            forbid_auto_register: reply.forbid_auto_register,
            topology_version: reply.topology_version,
            server_state: reply.server_state,
            cache_budgets: reply
                .cache_budgets
                .into_iter()
                .map(|budget| ShardCacheBudget {
                    shard_id: budget.shard_id,
                    table_id: budget.table_id,
                    load_version: budget.load_version,
                    raft_term: budget.raft_term,
                    bytes: budget.bytes,
                })
                .collect(),
        })
    }

    pub(crate) fn register_proxy_grpc(
        &self,
        target: &str,
        timeout_ms: u64,
        query: RegisterProxyRequest,
    ) -> Result<AckResponse, ClientError> {
        let status = self.metadata_rpc(target, timeout_ms, move |channel, token| async move {
            ProxyMetadataClient::new(channel)
                .register_proxy(authenticated(
                    v1::MetadataRegisterProxyRequest {
                        proxy_addr: query.proxy_addr,
                        namespace_name: query.namespace,
                        location: query.location,
                        config_version: query.config_version,
                        binary_version: query.binary_version,
                    },
                    token,
                )?)
                .await
        })?;
        Ok(AckResponse {
            status: decode_status(Some(status))?,
        })
    }

    pub(crate) fn proxy_heartbeat_grpc(
        &self,
        target: &str,
        timeout_ms: u64,
        query: ProxyHeartbeatRequest,
    ) -> Result<ProxyHeartbeatResponse, ClientError> {
        let reply = self.metadata_rpc(target, timeout_ms, move |channel, token| async move {
            ProxyMetadataClient::new(channel)
                .heartbeat(authenticated(
                    v1::MetadataProxyHeartbeatRequest {
                        proxy_addr: query.proxy_addr,
                        boot_time_ms: query.boot_time_ms,
                        namespace_name: query.namespace,
                        config_version: query.config_version,
                        binary_version: query.binary_version,
                    },
                    token,
                )?)
                .await
        })?;
        let drop_percent = reply
            .drop_percent
            .map(u8::try_from)
            .transpose()
            .map_err(|_| ClientError::Status("invalid metaserver drop percent".to_string()))?;
        Ok(ProxyHeartbeatResponse {
            status: decode_status(reply.status)?,
            config_changed: reply.config_changed,
            namespace: reply.namespace_name,
            config_version: reply.config_version,
            serving_mode: reply.serving_mode,
            drop_percent,
        })
    }

    pub(crate) fn metadata_leader_grpc(
        &self,
        target: &str,
        timeout_ms: u64,
    ) -> Result<MetaRaftLeaderResponse, ClientError> {
        let reply = self.metadata_rpc(target, timeout_ms, move |channel, token| async move {
            ProxyMetadataClient::new(channel)
                .get_leader(authenticated(v1::MetadataLeaderRequest {}, token)?)
                .await
        })?;
        Ok(MetaRaftLeaderResponse {
            status: decode_status(reply.status)?,
            leader_id: reply.leader_id,
            addr: reply.addr,
            is_local: reply.is_local,
        })
    }

    pub(crate) fn metadata_shard_ids_grpc(
        &self,
        target: &str,
        timeout_ms: u64,
    ) -> Result<(Vec<u64>, bool), ClientError> {
        let reply = self.metadata_rpc(target, timeout_ms, move |channel, token| async move {
            ProxyMetadataClient::new(channel)
                .max_decoding_message_size(40 * 1024 * 1024)
                .list_shard_ids(authenticated(v1::MetadataListShardIdsRequest {}, token)?)
                .await
        })?;
        let status = decode_status(reply.status)?;
        if !status.ok {
            return Err(ClientError::Status(status.message));
        }
        Ok((reply.shard_ids, reply.next_after_shard_id.is_none()))
    }
}

fn authenticated<T>(message: T, token: Option<String>) -> Result<tonic::Request<T>, tonic::Status> {
    let mut request = tonic::Request::new(message);
    if let Some(token) = token {
        let mut header: tonic::metadata::MetadataValue<_> = format!("Bearer {token}")
            .parse()
            .map_err(|_| tonic::Status::invalid_argument("invalid metaserver credential"))?;
        header.set_sensitive(true);
        request.metadata_mut().insert("authorization", header);
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{MetaEntityState, TableMetaInfo};
    use crate::metaserver_files_proto::table_meta_info_to_proto;
    use tonic::transport::{server::TcpIncoming, Server};

    struct Topology;

    #[tonic::async_trait]
    impl v1::metadata_service_server::MetadataService for Topology {
        async fn get_table_topology(
            &self,
            request: tonic::Request<v1::MetadataTableTopologyRequest>,
        ) -> Result<tonic::Response<v1::MetadataTableTopologyResponse>, tonic::Status> {
            let req = request.into_inner();
            assert_eq!(
                (req.namespace_name.as_str(), req.table_name.as_str()),
                ("kvcache", "model")
            );
            let table = TableMetaInfo {
                table_id: 13,
                namespace: req.namespace_name,
                table_name: req.table_name,
                state: MetaEntityState::Normal,
                topology_version: 42,
                first_shard_id: 75,
                shard_count: 1,
                replica_count: 3,
                partition_version: 0,
                serving_options: Default::default(),
            };
            Ok(tonic::Response::new(v1::MetadataTableTopologyResponse {
                status: Some(v1::Status {
                    ok: true,
                    ..Default::default()
                }),
                table: Some(table_meta_info_to_proto(&table)),
                shards: vec![v1::MetadataShardRoute {
                    shard_id: 75,
                    placement_primary: Some("127.0.0.1:17002".to_string()),
                    placement_load_version: 4,
                    elected_endpoint: Some(v1::MetadataServerEndpoint {
                        server_addr: "127.0.0.1:17003".to_string(),
                        grpc_addr: Some("127.0.0.1:17203".to_string()),
                        ..Default::default()
                    }),
                    load_version: 8,
                    uses_raft_route: true,
                    ..Default::default()
                }],
                ..Default::default()
            }))
        }

        async fn get_topology_version(
            &self,
            request: tonic::Request<v1::MetadataVersionRequest>,
        ) -> Result<tonic::Response<v1::MetadataVersionResponse>, tonic::Status> {
            Ok(tonic::Response::new(v1::MetadataVersionResponse {
                status: Some(v1::Status {
                    ok: true,
                    ..Default::default()
                }),
                current_topology_version: 43,
                old_topology_version: request.into_inner().old_topology_version,
                ..Default::default()
            }))
        }
    }

    #[tokio::test]
    async fn rust_sdk_discovers_elected_grpc_route_and_topology_version_over_grpc() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = listener.local_addr().unwrap().to_string();
        let worker = tokio::spawn(
            Server::builder()
                .add_service(v1::metadata_service_server::MetadataServiceServer::new(
                    Topology,
                ))
                .serve_with_incoming(TcpIncoming::from_listener(listener, true, None).unwrap()),
        );
        tokio::task::spawn_blocking(move || {
            let client = TemporalStoreClient::with_options(ClientOptions {
                connect_timeout_ms: 3_000,
                io_timeout_ms: 3_000,
                meta_sync_deadline_ms: 3_000,
                ..ClientOptions::direct(target)
            });
            let table = client.open_table_from_meta("kvcache", "model").unwrap();
            assert_eq!(table.options().table_id, 13);
            let route = table.resolve_grpc_primary("chunk").unwrap();
            assert_eq!(
                (route.shard_id, route.address.as_str(), route.load_version),
                (75, "127.0.0.1:17203", 8)
            );
            let version = client.fetch_meta_topology_version(42).unwrap();
            assert_eq!(
                (
                    version.old_topology_version,
                    version.current_topology_version
                ),
                (42, 43)
            );
            assert!(client.inner.meta_grpc_channel.lock().unwrap().is_some());
        })
        .await
        .unwrap();
        worker.abort();
    }
}
