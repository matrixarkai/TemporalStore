// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::{Error, Result};

pub(crate) fn native_matrixark_c_api_bridge_allowed(op: &str) -> Result<()> {
    let allowed = std::env::var("TEMPORALSTORE_RUST_ALLOW_NATIVE_MATRIXARK_C_API")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    if allowed {
        return Ok(());
    }
    Err(Error {
        code: 1,
        message: format!(
            "Rust MatrixArk hot path {op} would call the shared C API bridge. \
             Use the Rust-native temporalstore-rust matrixark_rust_proxy/direct SDK path, \
             or set TEMPORALSTORE_RUST_ALLOW_NATIVE_MATRIXARK_C_API=1 only for compatibility diagnostics."
        ),
    })
}
