// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    // Messages only -- this crate serves no RPC, so it needs `prost-build` rather than the
    // service codegen the server crate uses.
    let proto = "../../proto/temporalstore/snapshot/v1/snapshot.proto";
    prost_build::compile_protos(&[proto], &["../../proto"])?;

    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
