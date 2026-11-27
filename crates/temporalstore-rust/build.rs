// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto = "../../proto/temporalstore/v1/temporalstore.proto";
    let window_reduce = "../../proto/temporalstore/windowreduce/v1/windowreduce.proto";
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&[proto, window_reduce], &["../../proto"])?;

    build_proxy_relay();

    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed={window_reduce}");
    Ok(())
}

fn build_proxy_relay() {
    use tonic_build::manual::{Builder, Method, Service};

    let method = |name, route, input, output| {
        Method::builder()
            .name(name)
            .route_name(route)
            .input_type(input)
            .output_type(output)
            .codec_path("tonic::codec::ProstCodec")
            .server_streaming()
    };
    // Generate a server for the public RPC names with wire-compatible relay
    // types for the data path. The public proto still generates typed clients.
    Builder::new()
        .build_client(false)
        .compile(&[Service::builder()
            .name("ProxyService")
            .package("temporalstore.v1")
            .method(
                method(
                    "proxy_execute_stream",
                    "ProxyExecuteStream",
                    "super::RelayRequest",
                    "super::RelayEvent",
                )
                .client_streaming()
                .build(),
            )
            .method(
                method(
                    "route_callbacks",
                    "RouteCallbacks",
                    "crate::sdk::v1::ProxyRouteCallback",
                    "crate::sdk::v1::ProxyRouteAck",
                )
                .client_streaming()
                .build(),
            )
            .method(
                method(
                    "watch_proxy_preflight",
                    "WatchProxyPreflight",
                    "crate::sdk::v1::ProxyPreflightWatchRequest",
                    "crate::sdk::v1::ProxyPreflightEvent",
                )
                .build(),
            )
            .build()]);
}
