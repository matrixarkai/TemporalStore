// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Production-facing alias for the MatrixArk Rust direct SDK bridge.
//
// This binary shares the long-lived JSON-lines implementation with
// `matrixark_rust_proxy`, but reports `rust-direct-sdk-bridge` mode by default.
#![recursion_limit = "256"]
/// Which of the two bins sharing the implementation below this is: the direct SDK bridge.
const DIRECT_SDK_BRIDGE: bool = true;
include!("../matrixark_rust_proxy_impl.rs");
