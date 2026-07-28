// Production-facing alias for the MatrixArk Rust direct SDK bridge.
//
// This binary shares the long-lived JSON-lines implementation with
// `matrixark_rust_proxy`, but reports `rust-direct-sdk-bridge` mode by default.
#![recursion_limit = "256"]
include!("matrixark_rust_proxy_impl.rs");
