// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Production-named MatrixArk Rust proxy entrypoint.
//
// Benchmarks and production wiring must invoke this binary name so retired
// record-log artifacts are not mistaken for the Rust production path.
#![recursion_limit = "256"]
/// Which of the two bins sharing the implementation below this is: the proxy.
const DIRECT_SDK_BRIDGE: bool = false;
include!("../matrixark_rust_proxy_impl.rs");
