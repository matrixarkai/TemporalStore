// MatrixArk Rust direct SDK binding descriptor.
//
// The direct SDK product is the in-process Rust cdylib/PyO3-style boundary
// (`libtemporalstore_rust.so`), not the long-lived stdio proxy. Keep this
// binary small so packaging and smoke checks have a stable public executable
// without accidentally routing hot paths through `matrixark_rust_proxy_impl`.

use std::env;
use std::process;

fn has_arg(name: &str) -> bool {
    env::args().skip(1).any(|arg| arg == name)
}

fn main() {
    if has_arg("--serve") {
        eprintln!(
            "matrixark_rust_direct_sdk is an in-process cdylib binding, not a stdio proxy. \
             Use matrixark_rust_proxy --serve for proxy mode, or load libtemporalstore_rust.so \
             through MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB for direct SDK mode."
        );
        process::exit(64);
    }

    let shared_library = env::var("MATRIXARK_TEMPORALSTORE_RUST_DIRECT_LIB")
        .unwrap_or_else(|_| "libtemporalstore_rust.so".to_string());
    let payload = serde_json::json!({
        "ok": true,
        "product": "matrixark_rust_direct_sdk",
        "sdk_mode": "rust_direct_cdylib",
        "transport": "in_process_cdylib",
        "shared_library": shared_library,
        "python_adapter": "MatrixArkRustCdylibClient",
        "proxy_impl": false,
        "stdio_proxy": false,
        "proxy_product": "matrixark_rust_proxy",
        "exports": [
            "temporalstore_rust_connect_json",
            "temporalstore_rust_close",
            "temporalstore_rust_hset",
            "temporalstore_rust_hget",
            "temporalstore_rust_hgetall_json",
            "temporalstore_rust_matrixark_batch_append_records_json",
            "temporalstore_rust_matrixark_scan_candidates_json",
            "temporalstore_rust_matrixark_retrieve_context_pack_json"
        ]
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&payload).expect("serialize direct sdk descriptor")
    );
}
