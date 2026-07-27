// Production-named MatrixArk Rust proxy entrypoint.
//
// Benchmarks and production wiring must invoke this binary name so retired
// record-log artifacts are not mistaken for the Rust production path.
#![recursion_limit = "256"]
include!("matrixark_rust_proxy_impl.rs");
