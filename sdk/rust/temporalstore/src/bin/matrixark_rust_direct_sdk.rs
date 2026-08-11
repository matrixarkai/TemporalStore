// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Production-facing alias for the MatrixArk Rust direct SDK bridge.
//
// This binary shares the long-lived JSON-lines implementation with
// `matrixark_rust_proxy`, but reports `rust-direct-sdk-bridge` mode by default.
include!("matrixark_rust_proxy_impl.rs");
