// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::client::{
    protobuf_command_key, ClientError, ProtobufBatchExecuteReply, ProtobufExecuteReply,
    TemporalStoreTable,
};
use crate::sdk::v1;
use bytes::Bytes;
use std::io;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{server::TcpIncoming, Server};
use tonic::{Request, Response, Status as RpcStatus, Streaming};

const MESSAGE_LIMIT: usize = 40 * 1024 * 1024;

include!(concat!(
    env!("OUT_DIR"),
    "/temporalstore.v1.ProxyService.rs"
));

// The public message fields are embedded protobufs. Prost decodes these
// fields as byte slices so the relay can inspect keys and forward the payload.
#[derive(Clone, PartialEq, prost::Message)]
pub struct RelayRequest {
    #[prost(oneof = "RelayOperation", tags = "1, 2, 3")]
    operation: Option<RelayOperation>,
    #[prost(string, tag = "4")]
    namespace: String,
    #[prost(string, tag = "5")]
    table_name: String,
}

#[derive(Clone, PartialEq, prost::Oneof)]
enum RelayOperation {
    #[prost(message, tag = "1")]
    Execute(RelayExecute),
    #[prost(message, tag = "2")]
    Batch(RelayBatch),
    #[prost(message, tag = "3")]
    Sync(v1::SyncTopologyRequest),
}

#[derive(Clone, PartialEq, prost::Message)]
struct RelayExecute {
    #[prost(uint64, tag = "2")]
    trace_id: u64,
    #[prost(bytes = "bytes", optional, tag = "3")]
    command: Option<Bytes>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RelayBatch {
    #[prost(uint64, tag = "2")]
    trace_id: u64,
    #[prost(bytes = "bytes", repeated, tag = "3")]
    commands: Vec<Bytes>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct RelayEvent {
    #[prost(message, optional, tag = "1")]
    status: Option<v1::Status>,
    #[prost(uint64, tag = "2")]
    topology_version: u64,
    #[prost(message, optional, tag = "3")]
    execute: Option<ProtobufExecuteReply>,
    #[prost(message, optional, tag = "4")]
    batch: Option<ProtobufBatchExecuteReply>,
    #[prost(string, repeated, tag = "5")]
    route_refreshes: Vec<String>,
}

type ReplyStream<T> = ReceiverStream<Result<T, RpcStatus>>;

fn wire_status(status: Status) -> v1::Status {
    v1::Status {
        ok: status.ok,
        code: status.code,
        message: status.message,
    }
}

fn client_status(error: ClientError) -> Status {
    Status::error(proxy_client_error_code(&error), error.to_string())
}

async fn blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| Status::error("server_error", error.to_string()))?
}

fn relay_stream<A, B, F>(
    mut requests: Streaming<A>,
    mut handle: impl FnMut(A) -> F + Send + 'static,
) -> ReplyStream<B>
where
    A: Send + 'static,
    B: Send + 'static,
    F: std::future::Future<Output = B> + Send,
{
    let (send, receive) = mpsc::channel(1);
    tokio::spawn(async move {
        loop {
            // Reserve reply capacity before reading the next frame.
            let Ok(release) = send.reserve().await else {
                break;
            };
            let request = tokio::select! {
                _ = send.closed() => break,
                request = requests.message() => request,
            };
            match request {
                Ok(Some(request)) => {
                    let reply = tokio::select! {
                        _ = send.closed() => break,
                        reply = handle(request) => reply,
                    };
                    release.send(Ok(reply));
                }
                Ok(None) => break,
                Err(error) => {
                    release.send(Err(error));
                    break;
                }
            }
        }
    });
    ReceiverStream::new(receive)
}

impl ProxyService {
    /// Start the protobuf proxy on an explicitly configured address.
    pub fn start_grpc(&self, addr: &str) -> io::Result<SocketAddr> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let transport = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let proxy = self.clone();
        thread::Builder::new()
            .name("ts-proxy-grpc".to_string())
            .spawn(move || {
                if let Err(error) = transport.block_on(async move {
                    let listener = tokio::net::TcpListener::from_std(listener)?;
                    let incoming = TcpIncoming::from_listener(listener, true, None)?;
                    Server::builder()
                        .add_service(relay_service(proxy))
                        .serve_with_incoming(incoming)
                        .await?;
                    Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
                }) {
                    tracing::error!(%address, %error, "temporalstore proxy gRPC listener failed");
                }
            })?;
        Ok(address)
    }

    fn admit_relay<'a>(
        &'a self,
        namespace: &str,
        keys: &[(&str, bool)],
    ) -> Result<ProxyInflightGuard<'a>, Status> {
        if namespace.is_empty() {
            return Err(Status::error("invalid_request", "namespace is required"));
        }
        let is_write = keys.iter().any(|(_, read)| !read);
        let (rejection, max_requests, max_writes) = self.with_options(|options| {
            let rejection = proxy_account_rejection(options, namespace)
                .map(|status| (status, ProxyRejectionKind::Account))
                .or_else(|| {
                    proxy_serving_rejection(options, is_write)
                        .map(|status| (status, ProxyRejectionKind::Policy))
                })
                .or_else(|| {
                    keys.iter()
                        .find_map(|(key, _)| proxy_drop_rejection(options, key))
                        .map(|status| (status, ProxyRejectionKind::Policy))
                });
            (
                rejection,
                options.max_inflight_requests,
                options.max_inflight_write_requests,
            )
        });
        if let Some((status, kind)) = rejection {
            return Err(self.reject(status, kind));
        }
        self.inner
            .inflight
            .try_acquire(is_write, max_requests, max_writes)
            .map_err(|error| self.reject(error.status(), ProxyRejectionKind::Inflight))
    }

    async fn relay_table(
        &self,
        namespace: String,
        table_name: String,
    ) -> Result<TemporalStoreTable, Status> {
        if table_name.is_empty() || table_name.contains('/') || namespace.contains('/') {
            return Err(Status::error(
                "invalid_request",
                "invalid namespace or table",
            ));
        }
        let names = self.with_client(|client| client.cached_table_or_names(namespace, table_name));
        match names {
            Ok(table) => Ok(table),
            Err((namespace, table_name)) => {
                let proxy = self.clone();
                blocking(move || {
                    proxy
                        .table_for_request(namespace, table_name)
                        .map_err(client_status)
                })
                .await
            }
        }
    }

    async fn relay_execute(&self, request: RelayRequest) -> RelayEvent {
        let result = match request.operation {
            Some(RelayOperation::Execute(execute)) => {
                self.relay_single(request.namespace, request.table_name, execute)
                    .await
            }
            Some(RelayOperation::Batch(batch)) => {
                self.relay_batch(request.namespace, request.table_name, batch)
                    .await
            }
            Some(RelayOperation::Sync(sync)) => {
                self.relay_sync(request.namespace, request.table_name, sync)
                    .await
            }
            None => Err(Status::error(
                "invalid_request",
                "proxy operation is required",
            )),
        };
        result.unwrap_or_else(|status| RelayEvent {
            status: Some(wire_status(status)),
            ..Default::default()
        })
    }

    async fn relay_single(
        &self,
        namespace: String,
        table_name: String,
        request: RelayExecute,
    ) -> Result<RelayEvent, Status> {
        self.inner.execute_requests.fetch_add(1, Ordering::Relaxed);
        let command = request
            .command
            .ok_or_else(|| Status::error("invalid_request", "command is required"))?;
        let routing = relay_routing(&command)?;
        let _admitted = self.admit_relay(&namespace, &[routing])?;
        let table = self.relay_table(namespace, table_name).await?;
        let response = table
            .execute_routed_protobuf(&command, routing, request.trace_id)
            .await
            .map_err(client_status)?;
        Ok(RelayEvent {
            status: response.status.clone(),
            topology_version: response.topology_version,
            execute: Some(response),
            ..Default::default()
        })
    }

    async fn relay_batch(
        &self,
        namespace: String,
        table_name: String,
        request: RelayBatch,
    ) -> Result<RelayEvent, Status> {
        self.inner
            .batch_execute_requests
            .fetch_add(1, Ordering::Relaxed);
        if request.commands.is_empty() {
            return Err(Status::error(
                "invalid_request",
                "batch must contain commands",
            ));
        }
        let routing = request
            .commands
            .iter()
            .map(relay_routing)
            .collect::<Result<Vec<_>, _>>()?;
        let _admitted = self.admit_relay(&namespace, &routing)?;
        let table = self.relay_table(namespace, table_name).await?;
        let reply = table
            .execute_routed_protobuf_batch(
                &request.commands,
                &routing,
                request.trace_id,
                MESSAGE_LIMIT,
                true,
            )
            .await
            .map_err(client_status)?;
        Ok(RelayEvent {
            status: reply.status.clone(),
            topology_version: reply.topology_version,
            batch: Some(reply),
            ..Default::default()
        })
    }

    async fn relay_sync(
        &self,
        namespace: String,
        table_name: String,
        request: v1::SyncTopologyRequest,
    ) -> Result<RelayEvent, Status> {
        let mut tables = request.table_keys;
        if tables.is_empty() && !namespace.is_empty() && !table_name.is_empty() {
            tables.push(format!("{namespace}/{table_name}"));
        }
        if tables.is_empty() {
            return Err(Status::error("invalid_request", "table is required"));
        }
        let proxy = self.clone();
        let sync = blocking(move || {
            let _admitted = proxy.admit_topology_refresh()?;
            // Validate the complete scope before making control-plane requests.
            for table in &tables {
                let (ns, _) = split_table_key(table)?;
                if let Some(status) =
                    proxy.with_options(|options| proxy_account_rejection(options, ns))
                {
                    return Err(status);
                }
            }
            let topology = proxy.fetch_meta_topology_version(request.min_topology_version)?;
            let client = proxy.client();
            for table in &tables {
                let (ns, name) = split_table_key(table)?;
                client
                    .sync_table_topology(ns, name)
                    .map_err(client_status)?;
            }
            Ok(RelayEvent {
                status: Some(wire_status(topology.status)),
                topology_version: topology.current_topology_version,
                route_refreshes: tables,
                ..Default::default()
            })
        });
        if request.deadline_ms == 0 {
            sync.await
        } else {
            tokio::time::timeout(Duration::from_millis(request.deadline_ms), sync)
                .await
                .map_err(|_| Status::error("deadline_exceeded", "topology refresh timed out"))?
        }
    }

    async fn relay_callback(&self, request: v1::ProxyRouteCallback) -> v1::ProxyRouteAck {
        let table_key = request.table_key.clone();
        let shard_id = request.shard_id;
        let result = self
            .relay_sync(
                String::new(),
                String::new(),
                v1::SyncTopologyRequest {
                    table_keys: vec![request.table_key],
                    min_topology_version: request.topology_version,
                    deadline_ms: 0,
                },
            )
            .await;
        let (status, topology_version) = match result {
            Ok(event) => (event.status, event.topology_version),
            Err(status) => (Some(wire_status(status)), 0),
        };
        v1::ProxyRouteAck {
            status,
            table_key,
            shard_id,
            topology_version,
        }
    }
}

fn relay_routing(command: &Bytes) -> Result<(&str, bool), Status> {
    protobuf_command_key(command)
        .map_err(|error| Status::error("invalid_request", error.to_string()))
}

fn split_table_key(table: &str) -> Result<(&str, &str), Status> {
    table
        .split_once('/')
        .filter(|(namespace, table)| {
            !namespace.is_empty() && !table.is_empty() && !table.contains('/')
        })
        .ok_or_else(|| Status::error("invalid_request", "expected namespace/table"))
}

const PREFLIGHT_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct GrpcRelay {
    proxy: ProxyService,
    preflight: Arc<tokio::sync::Mutex<Option<(std::time::Instant, Arc<ProxyPreflightReport>)>>>,
}

impl GrpcRelay {
    async fn preflight(&self) -> Result<Arc<ProxyPreflightReport>, Status> {
        let mut cached = self.preflight.lock().await;
        if let Some((observed, report)) = cached.as_ref() {
            if observed.elapsed() < PREFLIGHT_INTERVAL {
                return Ok(report.clone());
            }
        }
        let proxy = self.proxy.clone();
        let report = Arc::new(blocking(move || Ok(proxy.preflight_report())).await?);
        *cached = Some((std::time::Instant::now(), report.clone()));
        Ok(report)
    }
}

fn relay_service(proxy: ProxyService) -> proxy_service_server::ProxyServiceServer<GrpcRelay> {
    proxy_service_server::ProxyServiceServer::new(GrpcRelay {
        proxy,
        preflight: Default::default(),
    })
    .max_decoding_message_size(MESSAGE_LIMIT)
    .max_encoding_message_size(MESSAGE_LIMIT)
}

#[tonic::async_trait]
impl proxy_service_server::ProxyService for GrpcRelay {
    type ProxyExecuteStreamStream = ReplyStream<RelayEvent>;
    type RouteCallbacksStream = ReplyStream<v1::ProxyRouteAck>;
    type WatchProxyPreflightStream = ReplyStream<v1::ProxyPreflightEvent>;

    async fn proxy_execute_stream(
        &self,
        request: Request<Streaming<RelayRequest>>,
    ) -> Result<Response<Self::ProxyExecuteStreamStream>, RpcStatus> {
        let proxy = self.proxy.clone();
        Ok(Response::new(relay_stream(
            request.into_inner(),
            move |request| {
                let proxy = proxy.clone();
                async move { proxy.relay_execute(request).await }
            },
        )))
    }

    async fn route_callbacks(
        &self,
        request: Request<Streaming<v1::ProxyRouteCallback>>,
    ) -> Result<Response<Self::RouteCallbacksStream>, RpcStatus> {
        let proxy = self.proxy.clone();
        Ok(Response::new(relay_stream(
            request.into_inner(),
            move |request| {
                let proxy = proxy.clone();
                async move { proxy.relay_callback(request).await }
            },
        )))
    }

    async fn watch_proxy_preflight(
        &self,
        request: Request<v1::ProxyPreflightWatchRequest>,
    ) -> Result<Response<Self::WatchProxyPreflightStream>, RpcStatus> {
        let request = request.into_inner();
        let relay = self.clone();
        let (send, receive) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut last = None;
            let mut ticks = tokio::time::interval(PREFLIGHT_INTERVAL);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = send.closed() => break,
                    _ = ticks.tick() => {}
                }
                let report = match relay.preflight().await {
                    Ok(report) => report,
                    Err(status) => {
                        let _ = send.send(Err(RpcStatus::unavailable(status.message))).await;
                        break;
                    }
                };
                let topology_version = report.authoritative_topology_version;
                let degraded = !report.degraded_reasons.is_empty();
                let mut warnings = report.degraded_reasons.clone();
                if request.include_routes && topology_version < request.min_topology_version {
                    warnings.push("requested topology is not available".to_string());
                }
                if request.include_policy && !report.policy.serving_writes {
                    warnings.push("proxy is refusing writes".to_string());
                }
                let event = v1::ProxyPreflightEvent {
                    status: Some(wire_status(report.status.clone())),
                    topology_version,
                    degraded,
                    warnings,
                };
                if last.as_ref() != Some(&event) {
                    last = Some(event.clone());
                    if send.send(Ok(event)).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receive)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{
        MetaEntityState, ServerEndpoint, TableMetaInfo, TableShard, TableTopologyResponse,
    };
    use crate::sdk::{TemporalStoreSdkExecutor, TemporalStoreTonicAdapter};

    struct Echo(Arc<std::sync::atomic::AtomicUsize>);

    impl TemporalStoreSdkExecutor for Echo {
        fn execute_sdk(&self, _request: ExecuteRequest) -> ExecuteResponse {
            panic!("expected generation-fenced execution");
        }

        fn execute_checked_sdk(
            &self,
            request: ExecuteRequest,
            load_version: u64,
        ) -> ExecuteResponse {
            assert_eq!((request.shard_id, load_version), (75, 17));
            let Command::StringSet { key, value } = request.command else {
                panic!("expected binary StringSet");
            };
            assert_eq!(key, "chunk");
            ExecuteResponse {
                status: Status::ok(),
                response: crate::types::CommandResponse::Bytes { value: Some(value) },
            }
        }
        fn batch_execute_checked_sdk(
            &self,
            request: BatchExecuteRequest,
            load_version: u64,
        ) -> BatchExecuteResponse {
            assert_eq!(request.commands.len(), 2);
            self.0.fetch_add(1, Ordering::Relaxed);
            BatchExecuteResponse {
                status: Status::ok(),
                responses: request
                    .commands
                    .into_iter()
                    .map(|command| {
                        self.execute_checked_sdk(
                            ExecuteRequest {
                                shard_id: request.shard_id,
                                command,
                            },
                            load_version,
                        )
                    })
                    .collect(),
            }
        }
    }

    #[tokio::test]
    async fn typed_proxy_stream_routes_binary_payloads_with_account_admission() {
        let primary_available = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let meta_primary_available = primary_available.clone();
        let batch_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let datanode = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let datanode_addr = datanode.local_addr().unwrap().to_string();
        let datanode = tokio::spawn(
            Server::builder()
                .add_service(
                    v1::temporal_store_service_server::TemporalStoreServiceServer::new(
                        TemporalStoreTonicAdapter::new(Echo(batch_calls.clone())),
                    ),
                )
                .serve_with_incoming(TcpIncoming::from_listener(datanode, true, None).unwrap()),
        );
        let meta = TcpListener::bind("127.0.0.1:0").unwrap();
        let meta_addr = meta.local_addr().unwrap().to_string();
        let endpoint = ServerEndpoint {
            server_addr: datanode_addr.clone(),
            grpc_addr: Some(datanode_addr),
            location: String::new(),
        };
        let version = serde_json::json!({
            "status": Status::ok(), "current_topology_version": 3, "old_topology_version": 0,
            "unchanged": false, "server_count": 1, "proxy_count": 0, "table_count": 1,
            "shard_route_count": 1, "normal_servers": 1, "frozen_servers": 0, "dropped_servers": 0,
            "normal_proxies": 0, "frozen_proxies": 0, "dropped_proxies": 0,
            "normal_tables": 1, "frozen_tables": 0, "dropped_tables": 0, "changed_tables": [],
        });
        thread::spawn(move || {
            crate::http::serve_listener(meta, move |request| match request.path.as_str() {
                "/tables/topology" => crate::http::json_response(
                    200,
                    &TableTopologyResponse {
                        status: Status::ok(),
                        table: Some(TableMetaInfo {
                            table_id: 1,
                            namespace: "kvcache".to_string(),
                            table_name: "model".to_string(),
                            state: MetaEntityState::Normal,
                            topology_version: 3,
                            first_shard_id: 75,
                            shard_count: 1,
                            replica_count: 1,
                            partition_version: 0,
                            serving_options: Default::default(),
                        }),
                        shards: vec![TableShard {
                            shard_id: 75,
                            start_bucket: 0,
                            end_bucket: 1,
                            primary: None,
                            replicas: Vec::new(),
                            primary_endpoint: meta_primary_available
                                .load(Ordering::Relaxed)
                                .then(|| endpoint.clone()),
                            replica_endpoints: Vec::new(),
                            load_version: 17,
                            raft_route: None,
                        }],
                        unchanged: false,
                    },
                ),
                "/meta/topology_version" => crate::http::json_response(200, &version),
                _ => crate::http::json_response(404, &Status::error("not_found", request.path)),
            })
            .unwrap();
        });
        let proxy = ProxyService::new(ProxyOptions {
            meta_addr,
            ingestion_account: "kvcache".to_string(),
            enforce_ingestion_account: true,
            io_timeout_ms: 3_000,
            connect_timeout_ms: 3_000,
            ..Default::default()
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let listener = tokio::spawn(
            Server::builder()
                .add_service(relay_service(proxy))
                .serve_with_incoming(TcpIncoming::from_listener(listener, true, None).unwrap()),
        );
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .timeout(Duration::from_secs(5));
        let mut client = v1::proxy_service_client::ProxyServiceClient::new(endpoint.connect_lazy());
        let first = b"\x00\xff\xa1\x00";
        let second = b"\x80\x00\x9f";
        let command = |value: &[u8]| v1::Command {
            kind: Some(v1::command::Kind::StringSet(v1::StringSet {
                key: "chunk".to_string(),
                value: value.to_vec(),
                ttl_ms: 0,
            })),
        };
        let single = |namespace: &str| v1::ProxyStreamRequest {
            namespace_name: namespace.to_string(),
            table_name: "model".to_string(),
            kind: Some(v1::proxy_stream_request::Kind::Execute(
                v1::ExecuteRequest {
                    shard_id: 999,
                    trace_id: 17,
                    command: Some(command(first)),
                    load_version: Some(999),
                },
            )),
        };
        let batch = v1::ProxyStreamRequest {
            namespace_name: "kvcache".to_string(),
            table_name: "model".to_string(),
            kind: Some(v1::proxy_stream_request::Kind::BatchExecute(
                v1::BatchExecuteRequest {
                    commands: vec![command(first), command(second)],
                    ..Default::default()
                },
            )),
        };
        let frames = tonic::codegen::tokio_stream::iter([
            single("denied"),
            single("kvcache"),
            batch.clone(),
        ]);
        let mut stream = client
            .proxy_execute_stream(frames)
            .await
            .unwrap()
            .into_inner();
        let event = stream.message().await.unwrap().unwrap();
        assert_eq!(event.status.unwrap().code, "proxy_account_denied");
        let event = stream.message().await.unwrap().unwrap();
        assert!(event.status.unwrap().ok);
        assert_eq!(event.execute.unwrap().response.unwrap().value, first);
        let event = stream.message().await.unwrap().unwrap();
        assert!(event.status.unwrap().ok);
        let responses = event.batch_execute.unwrap().responses;
        assert_eq!(responses[0].value, first);
        assert_eq!(responses[1].value, second);
        assert_eq!(batch_calls.load(Ordering::Relaxed), 1);
        assert!(stream.message().await.unwrap().is_none());

        let callback = v1::ProxyRouteCallback {
            table_key: "kvcache/model".to_string(),
            shard_id: 75,
            topology_version: 2,
            ..Default::default()
        };
        let replies = tonic::codegen::tokio_stream::iter([callback.clone()]);
        let mut callbacks = client.route_callbacks(replies).await.unwrap().into_inner();
        let response = callbacks.message().await.unwrap().unwrap();
        assert!(response.status.unwrap().ok);
        assert_eq!((response.topology_version, response.shard_id), (3, 75));
        let mut preflight = client
            .watch_proxy_preflight(v1::ProxyPreflightWatchRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert!(preflight.message().await.unwrap().unwrap().status.is_some());
        primary_available.store(false, Ordering::Relaxed);
        let replies = tonic::codegen::tokio_stream::iter([callback]);
        let mut callbacks = client.route_callbacks(replies).await.unwrap().into_inner();
        assert!(
            callbacks
                .message()
                .await
                .unwrap()
                .unwrap()
                .status
                .unwrap()
                .ok
        );
        let frames = tonic::codegen::tokio_stream::iter([single("kvcache"), batch]);
        let mut unavailable = client
            .proxy_execute_stream(frames)
            .await
            .unwrap()
            .into_inner();
        for _ in 0..2 {
            let status = unavailable
                .message()
                .await
                .unwrap()
                .unwrap()
                .status
                .unwrap();
            assert_eq!(status.code, "raft_write_not_started");
        }
        assert_eq!(batch_calls.load(Ordering::Relaxed), 1);
        listener.abort();
        datanode.abort();
    }
}
