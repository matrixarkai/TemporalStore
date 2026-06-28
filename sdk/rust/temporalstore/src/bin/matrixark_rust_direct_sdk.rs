// Production-facing alias for the MatrixArk Rust direct SDK bridge.
//
// This binary shares the long-lived JSON-lines implementation with
// `matrixark_rust_proxy`, but reports `rust-direct-sdk-bridge` mode by default.
// The legacy `matrixark_record_log` binary remains only for compatibility and
// debug-only workflows.
include!("matrixark_record_log.rs");
