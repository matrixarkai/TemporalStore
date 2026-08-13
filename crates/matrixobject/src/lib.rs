// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MatrixObject is the Rust object/segment store foundation that provides a
//! blockserver/chunkserver split without binding the public API
//! to brpc or thrift.

pub mod client;
pub mod local;
pub mod meta;
pub mod pipeline;
pub mod proxy;
pub mod replication;
pub mod service;
pub mod types;

pub use client::{MatrixObjectClient, MatrixObjectClientOptions, ReadTarget};
pub use local::{LocalMatrixObjectStore, MatrixObjectConfig};
pub use meta::*;
pub use pipeline::*;
pub use proxy::*;
pub use replication::*;
pub use service::*;
pub use types::*;
