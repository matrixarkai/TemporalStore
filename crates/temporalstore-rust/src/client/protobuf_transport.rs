// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::sdk::v1;
use bytes::Bytes;
use prost::encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType};
use prost::Message;
use tonic::client::Grpc;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::transport::{Channel, Endpoint};

const MESSAGE_LIMIT: usize = 40 * 1024 * 1024;

// Embedded protobuf messages and byte strings use the same wire type. Keep
// the command serialized so relays can forward the original nested message.
#[derive(Clone, PartialEq, prost::Message)]
struct EncodedExecuteRequest {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    trace_id: u64,
    #[prost(bytes = "bytes", tag = "3")]
    command: Bytes,
    #[prost(uint64, optional, tag = "4")]
    load_version: Option<u64>,
}

/// The datanode's gRPC reply with its command response still serialized.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ProtobufExecuteReply {
    #[prost(message, optional, tag = "1")]
    pub status: Option<crate::sdk::v1::Status>,
    #[prost(bytes = "bytes", optional, tag = "2")]
    pub response: Option<Bytes>,
    #[prost(uint64, tag = "3")]
    pub topology_version: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct EncodedBatchExecuteRequest {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    trace_id: u64,
    #[prost(bytes = "bytes", repeated, tag = "3")]
    commands: Vec<Bytes>,
    #[prost(uint64, optional, tag = "4")]
    load_version: Option<u64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct ProtobufBatchExecuteReply {
    #[prost(message, optional, tag = "1")]
    pub status: Option<crate::sdk::v1::Status>,
    #[prost(bytes = "bytes", repeated, tag = "2")]
    pub responses: Vec<Bytes>,
    #[prost(uint64, tag = "3")]
    pub topology_version: u64,
}

struct ProtobufShardBatch {
    shard_id: u64,
    route: Option<GrpcPrimaryRoute>,
    positions: Vec<usize>,
    read_only: bool,
}

fn length_delimited<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], prost::DecodeError> {
    let size = usize::try_from(decode_varint(input)?)
        .map_err(|_| prost::DecodeError::new("field length overflows usize"))?;
    let (value, rest) = input
        .split_at_checked(size)
        .ok_or_else(|| prost::DecodeError::new("truncated field"))?;
    *input = rest;
    Ok(value)
}

pub(crate) fn protobuf_command_key(mut input: &[u8]) -> Result<(&str, bool), prost::DecodeError> {
    let (kind, wire) = decode_key(&mut input)?;
    // Keyed string, hash, common, list, set, sorted-set and feature
    // commands carry the key at field 1 of the selected message.
    let allowed = matches!(kind, 1..=13 | 20..=30 | 38..=42 | 44 | 63..=68 | 70..=74 | 76..=88);
    if !allowed || wire != WireType::LengthDelimited {
        return Err(prost::DecodeError::new(
            "expected a key-routed protobuf command",
        ));
    }
    let mut body = length_delimited(&mut input)?;
    if !input.is_empty() {
        return Err(prost::DecodeError::new(
            "expected exactly one command variant",
        ));
    }
    let mut key = "";
    while !body.is_empty() {
        let (tag, wire) = decode_key(&mut body)?;
        if tag == 1 {
            if wire != WireType::LengthDelimited {
                return Err(prost::DecodeError::new("invalid key field"));
            }
            key = std::str::from_utf8(length_delimited(&mut body)?)
                .map_err(|_| prost::DecodeError::new("key is not UTF-8"))?;
        } else {
            skip_field(wire, tag, &mut body, DecodeContext::default())?;
        }
    }
    let read = matches!(kind, 2 | 5 | 7 | 9 | 11 | 13 | 21 | 64 | 68 | 82..=88);
    Ok((key, read))
}

fn protobuf_status(code: &str, message: impl Into<String>) -> v1::Status {
    v1::Status {
        ok: code == "ok",
        code: code.to_owned(),
        message: message.into(),
    }
}

fn incomplete_batch(read_only: bool) -> v1::Status {
    let code = if read_only {
        "invalid_batch"
    } else {
        "write_outcome_unknown"
    };
    protobuf_status(code, "datanode returned an incomplete batch")
}

fn before_protobuf_dispatch(error: ClientError, read_only: bool) -> ClientError {
    if read_only || matches!(error, ClientError::InvalidRequest(_)) {
        return error;
    }
    ClientError::WriteNotStarted(error.to_string())
}

impl TemporalStoreClient {
    fn grpc_channel(&self, address: &str) -> Result<Channel, ClientError> {
        if let Some(channel) = self
            .inner
            .grpc_channels
            .read()
            .expect("gRPC channel lock poisoned")
            .get(address)
            .cloned()
        {
            return Ok(channel);
        }
        let endpoint = Endpoint::from_shared(format!("http://{address}"))
            .map_err(|error| ClientError::InvalidRequest(error.to_string()))?
            .connect_timeout(Duration::from_millis(self.inner.options.connect_timeout_ms))
            .timeout(Duration::from_millis(self.inner.options.io_timeout_ms));
        let runtime = super::metadata_grpc::runtime()?;
        let _guard = runtime.enter();
        let mut channels = self
            .inner
            .grpc_channels
            .write()
            .expect("gRPC channel lock poisoned");
        Ok(channels
            .entry(address.to_string())
            .or_insert_with(|| endpoint.connect_lazy())
            .clone())
    }
}

impl TemporalStoreTable {
    fn invalidate_grpc_route(&self, route: &GrpcPrimaryRoute) {
        let mut routes = self
            .client
            .inner
            .grpc_routes
            .write()
            .expect("gRPC route cache lock poisoned");
        if let Some(table) = routes.get_mut(&self.combined_name) {
            if table.shards.get(&route.shard_id) == Some(route) {
                table.refreshed_at = None;
            }
        }
    }

    /// Execute one serialized, key-routed command over the shard's gRPC channel.
    /// Data-plane RPC failures are returned to the caller without replay.
    pub async fn execute_protobuf(
        &self,
        command: Bytes,
    ) -> Result<ProtobufExecuteReply, ClientError> {
        let routing = protobuf_command_key(&command)
            .map_err(|error| ClientError::InvalidRequest(error.to_string()))?;
        self.execute_routed_protobuf(&command, routing, 0).await
    }

    /// Execute a typed command against the table's elected gRPC primary.
    pub async fn execute_grpc(
        &self,
        command: v1::Command,
    ) -> Result<v1::ExecuteResponse, ClientError> {
        let reply = self
            .execute_protobuf(Bytes::from(command.encode_to_vec()))
            .await?;
        Ok(v1::ExecuteResponse {
            status: reply.status,
            response: reply
                .response
                .map(v1::CommandResponse::decode)
                .transpose()
                .map_err(|error| ClientError::Status(error.to_string()))?,
            topology_version: reply.topology_version,
        })
    }

    /// Batch typed commands by primary and return the results in input order.
    /// Each shard batch is dispatched once, with a generation fence when available.
    pub async fn batch_execute_grpc(
        &self,
        commands: impl IntoIterator<Item = v1::Command>,
    ) -> Result<v1::BatchExecuteResponse, ClientError> {
        let commands: Vec<_> = commands
            .into_iter()
            .map(|command| Bytes::from(command.encode_to_vec()))
            .collect();
        let routing = commands
            .iter()
            .map(|command| {
                protobuf_command_key(command)
                    .map_err(|error| ClientError::InvalidRequest(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let reply = self
            .execute_routed_protobuf_batch(&commands, &routing, 0, usize::MAX, false)
            .await?;
        let responses = reply
            .responses
            .into_iter()
            .map(v1::CommandResponse::decode)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ClientError::Status(error.to_string()))?;
        Ok(v1::BatchExecuteResponse {
            status: reply.status,
            responses,
            topology_version: reply.topology_version,
        })
    }

    pub(crate) async fn execute_routed_protobuf(
        &self,
        command: &Bytes,
        (key, read): (&str, bool),
        trace_id: u64,
    ) -> Result<ProtobufExecuteReply, ClientError> {
        let route = match self.cached_grpc_primary(key, true) {
            Some(cached) => cached,
            None => {
                let table = self.clone();
                let key = key.to_owned();
                tokio::task::spawn_blocking(move || table.resolve_grpc_primary(&key))
                    .await
                    .map_err(|error| ClientError::Status(error.to_string()))
                    .and_then(|result| result)
            }
        }
        .map_err(|error| before_protobuf_dispatch(error, read))?;
        let checked = route.load_version != 0;
        let path = if checked {
            "/temporalstore.v1.TemporalStoreService/ExecuteChecked"
        } else {
            "/temporalstore.v1.TemporalStoreService/Execute"
        };
        let request = EncodedExecuteRequest {
            shard_id: route.shard_id,
            trace_id,
            command: command.clone(),
            load_version: checked.then_some(route.load_version),
        };
        self.send_protobuf(
            &route,
            request,
            path,
            read,
            |reply: &ProtobufExecuteReply| reply.status.as_ref(),
        )
        .await
    }

    pub(crate) async fn execute_routed_protobuf_batch(
        &self,
        commands: &[Bytes],
        routing: &[(&str, bool)],
        trace_id: u64,
        response_limit: usize,
        preserve_unrouted_reads: bool,
    ) -> Result<ProtobufBatchExecuteReply, ClientError> {
        if commands.is_empty() {
            return Ok(ProtobufBatchExecuteReply {
                status: Some(protobuf_status("ok", "")),
                ..Default::default()
            });
        }
        let groups = self
            .route_protobuf_batch(routing, preserve_unrouted_reads)
            .await?;
        let mut reply = ProtobufBatchExecuteReply {
            status: Some(protobuf_status("ok", "")),
            responses: vec![Bytes::new(); commands.len()],
            ..Default::default()
        };
        let mut response_bytes = 0usize;
        for group in groups {
            let result = if let Some(route) = &group.route {
                self.execute_protobuf_shard_batch(route, &group, commands, trace_id)
                    .await
            } else {
                Ok(ProtobufBatchExecuteReply {
                    status: Some(protobuf_status(
                        "no_grpc_primary",
                        &format!("shard {} has no gRPC primary", group.shard_id),
                    )),
                    ..Default::default()
                })
            };
            let (status, responses) = match result {
                Ok(result) => {
                    reply.topology_version = reply.topology_version.max(result.topology_version);
                    let status = result
                        .status
                        .unwrap_or_else(|| incomplete_batch(group.read_only));
                    let status = if result.responses.len() == group.positions.len()
                        || (result.responses.is_empty() && !status.ok)
                    {
                        status
                    } else {
                        incomplete_batch(group.read_only)
                    };
                    (status, result.responses)
                }
                Err(error) => (
                    protobuf_status(crate::sdk::client_error_code(&error), error.to_string()),
                    Vec::new(),
                ),
            };
            if !status.ok
                && (status.code == "write_outcome_unknown"
                    || reply
                        .status
                        .as_ref()
                        .is_some_and(|status| status.ok || status.code == "no_grpc_primary"))
            {
                reply.status = Some(status.clone());
            }
            let mut responses = responses.into_iter();
            for index in group.positions {
                let response = responses.next().unwrap_or_else(|| {
                    Bytes::from(
                        v1::CommandResponse {
                            status: Some(status.clone()),
                            ..Default::default()
                        }
                        .encode_to_vec(),
                    )
                });
                response_bytes += response.len() + 8;
                if response_bytes > response_limit.saturating_sub(1024) {
                    reply.status = Some(protobuf_status(
                        "resource_exhausted",
                        "batch reply exceeds message limit after executing commands",
                    ));
                    reply.responses.clear();
                    return Ok(reply);
                }
                reply.responses[index] = response;
            }
        }
        Ok(reply)
    }

    async fn route_protobuf_batch(
        &self,
        keys: &[(&str, bool)],
        preserve_unrouted_reads: bool,
    ) -> Result<Vec<ProtobufShardBatch>, ClientError> {
        let read_only = keys.iter().all(|(_, read)| *read);
        let preserve_unrouted_reads = preserve_unrouted_reads && read_only;
        let groups = match self.cached_protobuf_batches(keys, true, preserve_unrouted_reads) {
            Some(cached) => cached,
            None => {
                self.refresh_protobuf_batches(keys, preserve_unrouted_reads)
                    .await
            }
        };
        groups.map_err(|error| before_protobuf_dispatch(error, read_only))
    }

    async fn refresh_protobuf_batches(
        &self,
        keys: &[(&str, bool)],
        preserve_unrouted_reads: bool,
    ) -> Result<Vec<ProtobufShardBatch>, ClientError> {
        let table = self.clone();
        tokio::task::spawn_blocking(move || {
            table
                .client
                .sync_table_topology(&table.namespace, &table.table_name)
        })
        .await
        .map_err(|error| ClientError::Status(error.to_string()))??;
        self.cached_protobuf_batches(keys, false, preserve_unrouted_reads)
            .unwrap_or_else(|| {
                Err(ClientError::Status(
                    "table has no gRPC topology".to_string(),
                ))
            })
    }

    fn cached_protobuf_batches(
        &self,
        keys: &[(&str, bool)],
        require_fresh: bool,
        preserve_unrouted_reads: bool,
    ) -> Option<Result<Vec<ProtobufShardBatch>, ClientError>> {
        let tables = self
            .client
            .inner
            .grpc_routes
            .read()
            .expect("gRPC route cache lock poisoned");
        let table = tables.get(&self.combined_name)?;
        let route_ttl = self.client.inner.options.route_cache_ttl_ms;
        let age = table.refreshed_at.map(|at| at.elapsed());
        if require_fresh && age.is_none_or(|age| age > Duration::from_millis(route_ttl)) {
            return None;
        }
        let mut groups = std::collections::BTreeMap::new();
        for (position, (key, read)) in keys.iter().enumerate() {
            let shard_id =
                shard_id_for_key(key, table.first_shard_id, table.shard_count, self.shard_id);
            let route = table.shards.get(&shard_id);
            if route.is_none() {
                if require_fresh
                    && age.is_none_or(|age| age > Duration::from_millis(route_ttl.min(250)))
                {
                    return None;
                }
                if !preserve_unrouted_reads {
                    return Some(Err(ClientError::Status(format!(
                        "shard {shard_id} has no gRPC primary"
                    ))));
                }
            }
            let batch = groups
                .entry(shard_id)
                .or_insert_with(|| ProtobufShardBatch {
                    shard_id,
                    route: route.cloned(),
                    positions: Vec::new(),
                    read_only: true,
                });
            batch.positions.push(position);
            batch.read_only &= *read;
        }
        Some(Ok(groups.into_values().collect()))
    }

    async fn execute_protobuf_shard_batch(
        &self,
        route: &GrpcPrimaryRoute,
        group: &ProtobufShardBatch,
        commands: &[Bytes],
        trace_id: u64,
    ) -> Result<ProtobufBatchExecuteReply, ClientError> {
        let checked = route.load_version != 0;
        let path = if checked {
            "/temporalstore.v1.TemporalStoreService/BatchExecuteChecked"
        } else {
            "/temporalstore.v1.TemporalStoreService/BatchExecute"
        };
        let request = EncodedBatchExecuteRequest {
            shard_id: route.shard_id,
            trace_id,
            commands: group
                .positions
                .iter()
                .map(|&index| commands[index].clone())
                .collect(),
            load_version: checked.then_some(route.load_version),
        };
        self.send_protobuf(
            route,
            request,
            path,
            group.read_only,
            |reply: &ProtobufBatchExecuteReply| reply.status.as_ref(),
        )
        .await
    }

    async fn send_protobuf<E, D>(
        &self,
        route: &GrpcPrimaryRoute,
        request: E,
        path: &'static str,
        read_only: bool,
        status: impl Fn(&D) -> Option<&crate::sdk::v1::Status>,
    ) -> Result<D, ClientError>
    where
        E: prost::Message + Send + 'static,
        D: prost::Message + Default + Send + 'static,
    {
        let channel = self.client.grpc_channel(&route.address)?;
        let mut client = Grpc::new(channel)
            .max_decoding_message_size(MESSAGE_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT);
        if let Err(error) = client.ready().await {
            self.invalidate_grpc_route(route);
            return Err(before_protobuf_dispatch(
                ClientError::Status(error.to_string()),
                read_only,
            ));
        }
        let response = client
            .unary(
                tonic::Request::new(request),
                PathAndQuery::from_static(path),
                tonic::codec::ProstCodec::<E, D>::default(),
            )
            .await;
        let invalidate = response.as_ref().map_or(true, |reply| {
            status(reply.get_ref()).is_some_and(|status| {
                matches!(
                    status.code.as_str(),
                    "load_version_mismatch"
                        | "raft_write_not_started"
                        | "raft_not_leader"
                        | "raft_error"
                )
            })
        });
        if invalidate {
            self.invalidate_grpc_route(route);
        }
        response.map(tonic::Response::into_inner).map_err(|error| {
            if read_only {
                ClientError::Status(error.to_string())
            } else {
                ClientError::WriteOutcomeUnknown(error.to_string())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{MetaEntityState, TableMetaInfo, TableShard};
    use crate::sdk::{v1, TemporalStoreSdkExecutor, TemporalStoreTonicAdapter};
    use prost::Message;
    use tonic::transport::{server::TcpIncoming, Server};

    struct CheckedEcho {
        batches: std::sync::Arc<std::sync::Mutex<Vec<(u64, u64, usize)>>>,
    }

    impl TemporalStoreSdkExecutor for CheckedEcho {
        fn execute_sdk(&self, _request: ExecuteRequest) -> ExecuteResponse {
            panic!("expected checked gRPC execution");
        }

        fn execute_checked_sdk(
            &self,
            request: ExecuteRequest,
            load_version: u64,
        ) -> ExecuteResponse {
            let count = match (request.shard_id, load_version) {
                (75, 17) => 42,
                (76, 23) => 73,
                pair => panic!("unexpected shard generation {pair:?}"),
            };
            let response = match request.command {
                Command::StringSet { key, value } => {
                    assert_eq!(key, "chunk");
                    CommandResponse::Bytes { value: Some(value) }
                }
                Command::StringLen { .. } => CommandResponse::Integer { value: count },
                _ => panic!("expected StringSet or StringLen"),
            };
            ExecuteResponse {
                status: Status::ok(),
                response,
            }
        }
        fn batch_execute_checked_sdk(
            &self,
            request: BatchExecuteRequest,
            load_version: u64,
        ) -> BatchExecuteResponse {
            self.batches.lock().unwrap().push((
                request.shard_id,
                load_version,
                request.commands.len(),
            ));
            let responses = request
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
                .collect();
            BatchExecuteResponse {
                status: Status::ok(),
                responses,
            }
        }
    }

    #[tokio::test]
    async fn serialized_command_uses_checked_grpc_and_preserves_response_bytes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let batches = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let worker = tokio::spawn(
            Server::builder()
                .add_service(
                    v1::temporal_store_service_server::TemporalStoreServiceServer::new(
                        TemporalStoreTonicAdapter::new(CheckedEcho { batches: batches.clone() }),
                    ),
                )
                .serve_with_incoming(TcpIncoming::from_listener(listener, true, None).unwrap()),
        );
        let client = TemporalStoreClient::with_options(ClientOptions {
            meta_grpc_target: Some("127.0.0.1:1".to_owned()),
            io_timeout_ms: 3_000,
            connect_timeout_ms: 3_000,
            ..ClientOptions::proxy("127.0.0.1:17003")
        });
        let table = client.open_table("kvcache", "model", TableOptions::default());
        client.update_grpc_routes(
            "kvcache/model",
            &TableMetaInfo {
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
            },
            &[TableShard {
                shard_id: 75,
                start_bucket: 0,
                end_bucket: 1,
                primary: None,
                replicas: Vec::new(),
                primary_endpoint: Some(ServerEndpoint {
                    server_addr: String::new(),
                    grpc_addr: Some(address),
                    location: String::new(),
                }),
                replica_endpoints: Vec::new(),
                load_version: 17,
                raft_route: None,
            }],
            false,
        );
        let payload = b"\x00\xff\x7f\x00";
        let encoded = Bytes::from(
            v1::Command {
                kind: Some(v1::command::Kind::StringSet(v1::StringSet {
                    key: "chunk".to_string(),
                    value: payload.to_vec(),
                    ttl_ms: 0,
                })),
            }
            .encode_to_vec(),
        );
        assert_eq!(protobuf_command_key(&encoded).unwrap(), ("chunk", false));
        for _ in 0..2 {
            let response = table.execute_protobuf(encoded.clone()).await.unwrap();
            assert!(response.status.unwrap().ok);
            let response = v1::CommandResponse::decode(response.response.unwrap()).unwrap();
            assert_eq!(response.value, payload);
        }
        let command = v1::Command::decode(encoded).unwrap();
        let single = table.execute_grpc(command.clone()).await.unwrap();
        assert_eq!(single.response.unwrap().value, payload);
        let batch = table
            .batch_execute_grpc([command.clone(), command])
            .await
            .unwrap();
        assert!(batch.status.unwrap().ok);
        assert_eq!(batch.responses.len(), 2);
        assert!(batch
            .responses
            .iter()
            .all(|response| response.value == payload));
        assert_eq!(client.inner.grpc_channels.read().unwrap().len(), 1);
        {
            let mut routes = client.inner.grpc_routes.write().unwrap();
            routes.get_mut("kvcache/model").unwrap().shard_count = 2;
        }
        let keys: Vec<_> = (0..2)
            .map(|offset| {
                (0..128)
                    .map(|i| format!("test-key-{i}"))
                    .find(|key| shard_id_for_key(key, 75, 2, table.shard_id) == 75 + offset)
                    .unwrap()
            })
            .collect();
        let commands: Vec<_> = keys
            .iter()
            .rev()
            .map(|key| {
                Bytes::from(
                    v1::Command {
                        kind: Some(v1::command::Kind::StringLen(v1::StringLen {
                            key: key.clone(),
                        })),
                    }
                    .encode_to_vec(),
                )
            })
            .collect();
        let routing = [(keys[1].as_str(), true), (keys[0].as_str(), true)];
        let partial = table
            .execute_routed_protobuf_batch(&commands, &routing, 0, usize::MAX, true)
            .await
            .unwrap();
        assert_eq!(partial.status.unwrap().code, "no_grpc_primary");
        let replies: Vec<_> = partial
            .responses
            .into_iter()
            .map(|response| v1::CommandResponse::decode(response).unwrap())
            .collect();
        assert_eq!(replies[0].status.as_ref().unwrap().code, "no_grpc_primary");
        assert!(replies[1].status.as_ref().unwrap().ok);
        assert_eq!(replies[1].count, 42);
        assert!(table
            .execute_routed_protobuf_batch(&commands, &routing, 0, usize::MAX, false)
            .await
            .is_err());
        let mixed = [(keys[1].as_str(), true), (keys[0].as_str(), false)];
        assert!(table
            .execute_routed_protobuf_batch(&commands, &mixed, 0, usize::MAX, true)
            .await
            .is_err());
        {
            let mut routes = client.inner.grpc_routes.write().unwrap();
            let routes = routes.get_mut("kvcache/model").unwrap();
            let mut second = routes.shards[&75].clone();
            second.shard_id = 76;
            second.load_version = 23;
            assert!(routes.shards.insert(76, second).is_none());
            routes.refreshed_at = Some(std::time::Instant::now());
        }

        batches.lock().unwrap().clear();
        let both = [
            commands[0].clone(), commands[1].clone(),
            commands[0].clone(), commands[1].clone(),
        ];
        let routing = [routing[0], routing[1], routing[0], routing[1]];
        let reply = table
            .execute_routed_protobuf_batch(&both, &routing, 0, usize::MAX, false)
            .await
            .unwrap();
        assert!(reply.status.unwrap().ok);
        let count_by_position: Vec<_> = reply.responses.into_iter().map(|response| {
            let response = v1::CommandResponse::decode(response).unwrap();
            assert!(response.status.unwrap().ok);
            response.count
        }).collect();
        assert_eq!(count_by_position, [73, 42, 73, 42]);
        assert_eq!(batches.lock().unwrap().as_slice(), &[(75, 17, 2), (76, 23, 2)]);

        {
            let mut routes = client.inner.grpc_routes.write().unwrap();
            let routes = routes.get_mut("kvcache/model").unwrap();
            assert!(routes.shards.remove(&76).is_some());
            routes.shard_count = 1;
        }
        tokio::task::spawn_blocking(move || {
            let command = Command::StringSet {
                key: "chunk".to_string(),
                value: payload.to_vec(),
            };
            let expected = CommandResponse::Bytes {
                value: Some(payload.to_vec()),
            };
            assert_eq!(table.execute(command.clone()).unwrap().response, expected);
            let batch = table.batch_execute(vec![command.clone(), command]).unwrap();
            assert!(batch.status.ok);
            assert_eq!(batch.responses.len(), 2);
            assert!(batch
                .responses
                .iter()
                .all(|reply| reply.response == expected));
        })
        .await
        .unwrap();
        worker.abort();
    }

    #[test]
    fn serialized_routing_rejects_an_ambiguous_or_truncated_command() {
        let command = v1::Command {
            kind: Some(v1::command::Kind::StringGet(v1::StringGet {
                key: String::new(),
            })),
        }
        .encode_to_vec();
        assert_eq!(protobuf_command_key(&command).unwrap(), ("", true));
        let mut duplicate = command.clone();
        duplicate.extend_from_slice(&command);
        assert!(protobuf_command_key(&duplicate).is_err());
        assert!(protobuf_command_key(&command[..1]).is_err());
    }
}
