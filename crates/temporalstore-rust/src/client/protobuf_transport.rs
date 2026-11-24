// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use bytes::Bytes;
use prost::encoding::{decode_key, decode_varint, skip_field, DecodeContext, WireType};
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

fn length_delimited<'a>(input: &mut &'a [u8]) -> Result<&'a [u8], prost::DecodeError> {
    let size = usize::try_from(decode_varint(input)?)
        .map_err(|_| prost::DecodeError::new("field length overflows usize"))?;
    let (value, rest) = input
        .split_at_checked(size)
        .ok_or_else(|| prost::DecodeError::new("truncated field"))?;
    *input = rest;
    Ok(value)
}

fn protobuf_command_key(mut input: &[u8]) -> Result<(&str, bool), prost::DecodeError> {
    let (kind, wire) = decode_key(&mut input)?;
    // Keyed string, hash, common, list, set, sorted-set and feature
    // commands carry the key at field 1 of the selected message.
    let allowed = matches!(kind, 1..=13 | 20..=30 | 38..=42 | 44 | 63..=68 | 70..=74 | 76..=87);
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
    let read = matches!(kind, 2 | 5 | 7 | 9 | 11 | 13 | 21 | 64 | 68 | 82..=87);
    Ok((key, read))
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
        let (key, read) = protobuf_command_key(&command)
            .map_err(|error| ClientError::InvalidRequest(error.to_string()))?;
        let route = match self.cached_grpc_primary(key, true) {
            Some(cached) => cached?,
            None => {
                let table = self.clone();
                let key = key.to_owned();
                tokio::task::spawn_blocking(move || table.resolve_grpc_primary(&key))
                    .await
                    .map_err(|error| ClientError::Status(error.to_string()))??
            }
        };
        let channel = self.client.grpc_channel(&route.address)?;
        let mut client = Grpc::new(channel)
            .max_decoding_message_size(MESSAGE_LIMIT)
            .max_encoding_message_size(MESSAGE_LIMIT);
        if let Err(error) = client.ready().await {
            self.invalidate_grpc_route(&route);
            return Err(ClientError::Status(error.to_string()));
        }
        let checked = route.load_version != 0;
        let path = if checked {
            "/temporalstore.v1.TemporalStoreService/ExecuteChecked"
        } else {
            "/temporalstore.v1.TemporalStoreService/Execute"
        };
        let request = EncodedExecuteRequest {
            shard_id: route.shard_id,
            command,
            load_version: checked.then_some(route.load_version),
        };
        let response = client
            .unary(
                tonic::Request::new(request),
                PathAndQuery::from_static(path),
                tonic::codec::ProstCodec::<EncodedExecuteRequest, ProtobufExecuteReply>::default(),
            )
            .await;
        let invalidate = response.as_ref().map_or(true, |reply| {
            reply.get_ref().status.as_ref().is_some_and(|status| {
                matches!(
                    status.code.as_str(),
                    "load_version_mismatch" | "raft_write_not_started" | "raft_error"
                )
            })
        });
        if invalidate {
            self.invalidate_grpc_route(&route);
        }
        response.map(tonic::Response::into_inner).map_err(|error| {
            if read {
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

    struct CheckedEcho;

    impl TemporalStoreSdkExecutor for CheckedEcho {
        fn execute_sdk(&self, _request: ExecuteRequest) -> ExecuteResponse {
            panic!("expected checked gRPC execution");
        }

        fn execute_checked_sdk(
            &self,
            request: ExecuteRequest,
            load_version: u64,
        ) -> ExecuteResponse {
            assert_eq!(request.shard_id, 75);
            assert_eq!(load_version, 17);
            let Command::StringSet { key, value } = request.command else {
                panic!("expected binary StringSet");
            };
            assert_eq!(key, "chunk");
            ExecuteResponse {
                status: Status::ok(),
                response: CommandResponse::Bytes { value: Some(value) },
            }
        }
    }

    #[tokio::test]
    async fn serialized_command_uses_checked_grpc_and_preserves_response_bytes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let worker = tokio::spawn(
            Server::builder()
                .add_service(
                    v1::temporal_store_service_server::TemporalStoreServiceServer::new(
                        TemporalStoreTonicAdapter::new(CheckedEcho),
                    ),
                )
                .serve_with_incoming(TcpIncoming::from_listener(listener, true, None).unwrap()),
        );
        let client = TemporalStoreClient::with_options(ClientOptions {
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
        assert_eq!(client.inner.grpc_channels.read().unwrap().len(), 1);
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
