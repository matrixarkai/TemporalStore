#[path = "../matrixark_rust_proxy_cache.rs"]
mod matrixark_rust_proxy_cache;
#[path = "../matrixark_rust_proxy_candidate_node_path.rs"]
mod matrixark_rust_proxy_candidate_node_path;
#[path = "../matrixark_rust_proxy_candidates.rs"]
mod matrixark_rust_proxy_candidates;
#[path = "../matrixark_rust_proxy_clock.rs"]
mod matrixark_rust_proxy_clock;
#[path = "../matrixark_rust_proxy_command_entries.rs"]
mod matrixark_rust_proxy_command_entries;
#[path = "../matrixark_rust_proxy_command_entries_stats.rs"]
mod matrixark_rust_proxy_command_entries_stats;
#[path = "../matrixark_rust_proxy_command_stats.rs"]
mod matrixark_rust_proxy_command_stats;
#[path = "../matrixark_rust_proxy_cross_session.rs"]
mod matrixark_rust_proxy_cross_session;
#[path = "../matrixark_rust_proxy_cross_session_budget.rs"]
mod matrixark_rust_proxy_cross_session_budget;
#[path = "../matrixark_rust_proxy_metrics.rs"]
mod matrixark_rust_proxy_metrics;
#[path = "../matrixark_rust_proxy_metrics_backend_render.rs"]
mod matrixark_rust_proxy_metrics_backend_render;
#[path = "../matrixark_rust_proxy_metrics_backend_stats.rs"]
mod matrixark_rust_proxy_metrics_backend_stats;
#[path = "../matrixark_rust_proxy_metrics_core_render.rs"]
mod matrixark_rust_proxy_metrics_core_render;
#[path = "../matrixark_rust_proxy_metrics_format.rs"]
mod matrixark_rust_proxy_metrics_format;
#[path = "../matrixark_rust_proxy_metrics_io_render.rs"]
mod matrixark_rust_proxy_metrics_io_render;
#[path = "../matrixark_rust_proxy_metrics_render.rs"]
mod matrixark_rust_proxy_metrics_render;
#[path = "../matrixark_rust_proxy_metrics_retrieve_render.rs"]
mod matrixark_rust_proxy_metrics_retrieve_render;
#[path = "../matrixark_rust_proxy_native_pack.rs"]
mod matrixark_rust_proxy_native_pack;
#[path = "../matrixark_rust_proxy_pack.rs"]
mod matrixark_rust_proxy_pack;
#[path = "../matrixark_rust_proxy_protocol.rs"]
mod matrixark_rust_proxy_protocol;
#[path = "../matrixark_rust_proxy_records.rs"]
mod matrixark_rust_proxy_records;
#[path = "../matrixark_rust_proxy_record_time_index.rs"]
mod matrixark_rust_proxy_record_time_index;
#[path = "../matrixark_rust_proxy_dispatch.rs"]
mod matrixark_rust_proxy_dispatch;
#[path = "../matrixark_rust_proxy_dispatch_hash.rs"]
mod matrixark_rust_proxy_dispatch_hash;
#[path = "../matrixark_rust_proxy_dispatch_matrixark.rs"]
mod matrixark_rust_proxy_dispatch_matrixark;
#[path = "../matrixark_rust_proxy_entry.rs"]
mod matrixark_rust_proxy_entry;
#[path = "../matrixark_rust_proxy_io.rs"]
mod matrixark_rust_proxy_io;
#[path = "../matrixark_rust_proxy_retrieve.rs"]
mod matrixark_rust_proxy_retrieve;
#[path = "../matrixark_rust_proxy_retrieve_policy.rs"]
mod matrixark_rust_proxy_retrieve_policy;
#[path = "../matrixark_rust_proxy_retrieve_result.rs"]
mod matrixark_rust_proxy_retrieve_result;
#[path = "../matrixark_rust_proxy_retrieve_request.rs"]
mod matrixark_rust_proxy_retrieve_request;
#[path = "../matrixark_rust_proxy_retrieve_pack_json.rs"]
mod matrixark_rust_proxy_retrieve_pack_json;
#[path = "../matrixark_rust_proxy_retrieve_response.rs"]
mod matrixark_rust_proxy_retrieve_response;
#[path = "../matrixark_rust_proxy_retrieve_scoring.rs"]
mod matrixark_rust_proxy_retrieve_scoring;
#[path = "../matrixark_rust_proxy_retrieve_select.rs"]
mod matrixark_rust_proxy_retrieve_select;
#[path = "../matrixark_rust_proxy_retrieve_signature.rs"]
mod matrixark_rust_proxy_retrieve_signature;
#[path = "../matrixark_rust_proxy_retrieve_telemetry.rs"]
mod matrixark_rust_proxy_retrieve_telemetry;
#[path = "../matrixark_rust_proxy_runtime.rs"]
mod matrixark_rust_proxy_runtime;
#[path = "../matrixark_rust_proxy_scan.rs"]
mod matrixark_rust_proxy_scan;
#[path = "../matrixark_rust_proxy_scan_node_paths.rs"]
mod matrixark_rust_proxy_scan_node_paths;
#[path = "../matrixark_rust_proxy_scan_records.rs"]
mod matrixark_rust_proxy_scan_records;
#[path = "../matrixark_rust_proxy_scan_response.rs"]
mod matrixark_rust_proxy_scan_response;
#[path = "../matrixark_rust_proxy_scan_secondary.rs"]
mod matrixark_rust_proxy_scan_secondary;
#[path = "../matrixark_rust_proxy_scope.rs"]
mod matrixark_rust_proxy_scope;
#[path = "../matrixark_rust_proxy_scope_boost.rs"]
mod matrixark_rust_proxy_scope_boost;
#[cfg(test)]
#[path = "../matrixark_rust_proxy_impl_tests.rs"]
mod matrixark_rust_proxy_impl_tests;

fn main() {
    let code = if std::env::args().any(|arg| arg == "--serve") {
        matrixark_rust_proxy_entry::serve()
    } else {
        matrixark_rust_proxy_entry::single_shot()
    };
    std::process::exit(code);
}
