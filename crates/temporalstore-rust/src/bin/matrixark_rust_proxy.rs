// Production-named MatrixArk Rust proxy entrypoint.
//
// The implementation is intentionally shared with the compatibility
// matrixark_record_log binary for now: both run the same long-lived JSON-lines
// direct-SDK bridge in --serve mode. Benchmarks and production wiring should
// invoke this binary name so old debug record-log artifacts are not mistaken
// for the Rust production path.
include!("matrixark_record_log.rs");
