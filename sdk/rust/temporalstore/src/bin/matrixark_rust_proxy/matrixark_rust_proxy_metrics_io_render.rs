use crate::matrixark_rust_proxy_metrics::MetricsSnapshot;
use crate::matrixark_rust_proxy_metrics_format::{line, metric_header};

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
