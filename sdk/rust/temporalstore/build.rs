// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// The default build of this SDK is the proxy client: pure Rust speaking the
// TemporalStore proxy protocol over a socket, with nothing native to link.
//
// The optional `direct` feature instead binds the in-process C ABI of a native
// TemporalStore library. This repository does not build that library, so
// enabling `direct` means supplying it yourself and pointing this script at it
// with TEMPORALSTORE_LIB_DIR (and TEMPORALSTORE_LIB_NAME if it is not called
// `temporalstore`). Without it the link of every `direct` target fails with a
// page of undefined `temporalstore_*` symbols, so say so here in one sentence
// instead.

use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=TEMPORALSTORE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=TEMPORALSTORE_LIB_NAME");

    if env::var_os("CARGO_FEATURE_DIRECT").is_none() {
        return;
    }

    let lib_dir = match env::var("TEMPORALSTORE_LIB_DIR") {
        Ok(dir) if !dir.trim().is_empty() => dir,
        _ => panic!(
            "feature `direct` links the native TemporalStore C ABI, which this \
             repository does not build. Set TEMPORALSTORE_LIB_DIR to a directory \
             holding that shared library, or build without `direct` -- the \
             default `proxy` feature needs nothing native."
        ),
    };
    let lib_name =
        env::var("TEMPORALSTORE_LIB_NAME").unwrap_or_else(|_| "temporalstore".to_string());

    println!("cargo:rustc-link-search=native={lib_dir}");
    println!("cargo:rustc-link-lib={lib_name}");
}
