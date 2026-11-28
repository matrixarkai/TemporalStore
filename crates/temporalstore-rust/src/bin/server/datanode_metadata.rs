// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use temporalstore_rust::client::{ClientOptions, TemporalStoreClient};

#[derive(Clone)]
enum DatanodeMetadata {
    Grpc {
        client: TemporalStoreClient,
        target: String,
    },
    Http(String),
}

fn datanode_metadata(http_addr: &str) -> DatanodeMetadata {
    let target = temporalstore_rust::env_flag::env_value("TS_META_GRPC_TARGET")
        .or_else(|| (http_addr == "127.0.0.1:17001").then(|| "127.0.0.1:17201".to_owned()));
    match target {
        Some(target) => {
            let options = HttpRequestOptions::default();
            let client = TemporalStoreClient::with_options(ClientOptions {
                connect_timeout_ms: options.connect_timeout_ms,
                io_timeout_ms: options.io_timeout_ms,
                ..ClientOptions::direct(target.as_str())
            });
            DatanodeMetadata::Grpc { client, target }
        }
        None => DatanodeMetadata::Http(http_addr.to_owned()),
    }
}

impl DatanodeMetadata {
    fn address(&self) -> &str {
        match self {
            Self::Grpc { target, .. } => target,
            Self::Http(addr) => addr,
        }
    }

    fn register_server(&self, request: &RegisterServerRequest) -> Result<Status, String> {
        match self {
            Self::Grpc { client, target } => client
                .register_datanode_grpc(target, request)
                .map_err(|error| error.to_string()),
            Self::Http(addr) => post_json_with_options_and_headers::<_, AckResponse>(
                addr,
                "/servers/register",
                request,
                &temporalstore_rust::meta::admin_auth_header(),
                HttpRequestOptions::default(),
            )
            .map(|response| response.status)
            .map_err(|error| error.to_string()),
        }
    }

    fn register_shard(&self, request: &RegisterShardRequest) -> Result<Status, String> {
        match self {
            Self::Grpc { client, target } => client
                .register_datanode_shard_grpc(target, request)
                .map_err(|error| error.to_string()),
            Self::Http(addr) => post_json_with_options_and_headers::<_, RegisterShardResponse>(
                addr,
                "/register_shard",
                request,
                &temporalstore_rust::meta::admin_auth_header(),
                HttpRequestOptions::default(),
            )
            .map(|response| response.status)
            .map_err(|error| error.to_string()),
        }
    }

    fn heartbeat(
        &self,
        request: &ServerHeartbeatRequest,
    ) -> Result<ServerHeartbeatResponse, String> {
        match self {
            Self::Grpc { client, target } => client
                .datanode_heartbeat_grpc(target, request)
                .map_err(|error| error.to_string()),
            Self::Http(addr) => post_json_with_options_and_headers(
                addr,
                "/servers/heartbeat",
                request,
                &temporalstore_rust::meta::admin_auth_header(),
                HttpRequestOptions::default(),
            )
            .map_err(|error| error.to_string()),
        }
    }
}
