// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto = "../../proto/temporalstore/v1/temporalstore.proto";
    let cells = "../../proto/temporalstore/cells/v1/cells.proto";
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&[proto, cells], &["../../proto"])?;

    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed={cells}");
    Ok(())
}
