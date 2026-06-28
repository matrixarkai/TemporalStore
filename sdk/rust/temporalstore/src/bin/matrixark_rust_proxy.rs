// Production-facing alias for the MatrixArk Rust proxy.
//
// The legacy `matrixark_record_log` binary is kept for compatibility and
// debug-only workflows. Both names currently share the same implementation.
include!("matrixark_record_log.rs");
