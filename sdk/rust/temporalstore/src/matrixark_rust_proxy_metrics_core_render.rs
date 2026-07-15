use crate::matrixark_rust_proxy_metrics::MetricsSnapshot;
use crate::matrixark_rust_proxy_metrics_format::{escape_label, line, metric_header};

pub(crate) fn append_process_metrics(out: &mut String, snapshot: &MetricsSnapshot) {
    metric_header(
        out,
        "matrixark_rust_proxy_process_start_time_ms",
        "gauge",
        "Unix millisecond timestamp when this Rust proxy process started.",
    );
    line(
        out,
        "matrixark_rust_proxy_process_start_time_ms",
        "",
        snapshot.started_at_unix_ms,
    );
}

pub(crate) fn append_command_metrics(out: &mut String, snapshot: &MetricsSnapshot) {
    metric_header(
        out,
        "matrixark_rust_proxy_commands_total",
        "counter",
        "Total MatrixArk Rust proxy commands by op and status.",
    );
    metric_header(
        out,
        "matrixark_rust_proxy_command_latency_ms_sum",
        "counter",
        "Total command latency in milliseconds by op.",
    );
    metric_header(
        out,
        "matrixark_rust_proxy_command_latency_ms_max",
        "gauge",
        "Maximum observed command latency in milliseconds by op.",
    );
    let mut ops: Vec<_> = snapshot.op.iter().collect();
    ops.sort_by(|a, b| a.0.cmp(b.0));
    for (op, metrics) in ops {
        let ok_labels = format!("{{op=\"{}\",status=\"ok\"}}", escape_label(op));
        let fail_labels = format!("{{op=\"{}\",status=\"error\"}}", escape_label(op));
        line(
            out,
            "matrixark_rust_proxy_commands_total",
            &ok_labels,
            metrics.ok,
        );
        line(
            out,
            "matrixark_rust_proxy_commands_total",
            &fail_labels,
            metrics.failed,
        );
        let op_labels = format!("{{op=\"{}\"}}", escape_label(op));
        line(
            out,
            "matrixark_rust_proxy_command_latency_ms_sum",
            &op_labels,
            metrics.latency_ms_sum,
        );
        line(
            out,
            "matrixark_rust_proxy_command_latency_ms_max",
            &op_labels,
            metrics.latency_ms_max,
        );
    }
}

pub(crate) fn append_retrieve_metrics(out: &mut String, snapshot: &MetricsSnapshot) {
    metric_header(
        out,
        "matrixark_backend_rust_engine_time_ms_total",
        "counter",
        "Total Rust engine execution time in milliseconds.",
    );
    line(
        out,
        "matrixark_backend_rust_engine_time_ms_total",
        "{backend=\"rust\"}",
        snapshot.rust_engine_time_ms_sum,
    );
    metric_header(
        out,
        "matrixark_backend_serialization_time_ms_total",
        "counter",
        "Total Rust proxy response serialization time in milliseconds.",
    );
    line(
        out,
        "matrixark_backend_serialization_time_ms_total",
        "{backend=\"rust\"}",
        snapshot.serialization_time_ms_sum,
    );
    metric_header(
        out,
        "matrixark_retrieve_scan_count_total",
        "counter",
        "Total records scanned by native MatrixArk retrieval calls.",
    );
    line(
        out,
        "matrixark_retrieve_scan_count_total",
        "{backend=\"rust\"}",
        snapshot.scan_count_total,
    );
    metric_header(
        out,
        "matrixark_retrieve_cache_hits_total",
        "counter",
        "Total native MatrixArk retrieval cache hits.",
    );
    line(
        out,
        "matrixark_retrieve_cache_hits_total",
        "{backend=\"rust\"}",
        snapshot.cache_hit_total,
    );
    metric_header(
        out,
        "matrixark_context_pack_selected_refs_total",
        "counter",
        "Total refs selected by native MatrixArk ContextPack assembly.",
    );
    line(
        out,
        "matrixark_context_pack_selected_refs_total",
        "{backend=\"rust\"}",
        snapshot.selected_refs_total,
    );
    metric_header(
        out,
        "matrixark_context_pack_dropped_refs_total",
        "counter",
        "Total refs dropped by native MatrixArk ContextPack assembly.",
    );
    line(
        out,
        "matrixark_context_pack_dropped_refs_total",
        "{backend=\"rust\"}",
        snapshot.dropped_refs_total,
    );
}

pub(crate) fn append_proxy_io_metrics(out: &mut String, snapshot: &MetricsSnapshot) {
    metric_header(
        out,
        "matrixark_rust_proxy_records_written_total",
        "counter",
        "Total MatrixArk records/hash entries written by the Rust proxy bridge.",
    );
    line(
        out,
        "matrixark_rust_proxy_records_written_total",
        "",
        snapshot.records_written,
    );
    metric_header(
        out,
        "matrixark_rust_proxy_records_read_total",
        "counter",
        "Total MatrixArk records/hash entries read by the Rust proxy bridge.",
    );
    line(
        out,
        "matrixark_rust_proxy_records_read_total",
        "",
        snapshot.records_read,
    );
    metric_header(
        out,
        "matrixark_rust_proxy_bytes_written_total",
        "counter",
        "Approximate payload bytes written by the Rust proxy bridge.",
    );
    line(
        out,
        "matrixark_rust_proxy_bytes_written_total",
        "",
        snapshot.bytes_written,
    );
    metric_header(
        out,
        "matrixark_rust_proxy_bytes_read_total",
        "counter",
        "Approximate payload bytes read by the Rust proxy bridge.",
    );
    line(
        out,
        "matrixark_rust_proxy_bytes_read_total",
        "",
        snapshot.bytes_read,
    );
    metric_header(
        out,
        "matrixark_rust_proxy_clients_created_total",
        "counter",
        "TemporalStore clients created by the Rust proxy/direct SDK bridge.",
    );
    line(
        out,
        "matrixark_rust_proxy_clients_created_total",
        "",
        snapshot.clients_created,
    );
    metric_header(
        out,
        "matrixark_rust_proxy_parse_errors_total",
        "counter",
        "Invalid JSON command lines received by the Rust proxy bridge.",
    );
    line(
        out,
        "matrixark_rust_proxy_parse_errors_total",
        "",
        snapshot.parse_errors,
    );
    metric_header(
        out,
        "matrixark_rust_proxy_client_connect_errors_total",
        "counter",
        "TemporalStore client connection failures in the Rust proxy bridge.",
    );
    line(
        out,
        "matrixark_rust_proxy_client_connect_errors_total",
        "",
        snapshot.client_connect_errors,
    );
    metric_header(
        out,
        "matrixark_rust_proxy_commands_failed_total",
        "counter",
        "Total failed MatrixArk Rust proxy commands.",
    );
    line(
        out,
        "matrixark_rust_proxy_commands_failed_total",
        "",
        snapshot.commands_failed,
    );
}
