use crate::matrixark_rust_proxy_metrics::MetricsSnapshot;

use crate::matrixark_rust_proxy_metrics_backend_stats::{
    elapsed_seconds, latency_le_100_count, max_command_latency_ms,
};
use crate::matrixark_rust_proxy_metrics_format::{
    line, matrixark_rust_storage_mode, metric_header,
};

pub(crate) fn append_backend_metrics(out: &mut String, snapshot: &MetricsSnapshot) {
    metric_header(
        out,
        "matrixark_backend_info",
        "gauge",
        "MatrixArk storage backend identity and storage mode.",
    );
    line(
        out,
        "matrixark_backend_info",
        &format!(
            "{{backend=\"rust\",storage_mode=\"{}\"}}",
            matrixark_rust_storage_mode()
        ),
        1,
    );
    metric_header(
        out,
        "matrixark_backend_qps",
        "gauge",
        "MatrixArk storage backend command QPS.",
    );
    line(
        out,
        "matrixark_backend_qps",
        "{backend=\"rust\"}",
        format!(
            "{:.6}",
            snapshot.commands_total as f64 / elapsed_seconds(snapshot)
        ),
    );
    metric_header(
        out,
        "matrixark_backend_commands_total",
        "counter",
        "MatrixArk storage backend command count.",
    );
    line(
        out,
        "matrixark_backend_commands_total",
        "{backend=\"rust\"}",
        snapshot.commands_total,
    );
    metric_header(
        out,
        "matrixark_backend_errors_total",
        "counter",
        "MatrixArk storage backend command errors.",
    );
    line(
        out,
        "matrixark_backend_errors_total",
        "{backend=\"rust\"}",
        snapshot.commands_failed,
    );
    metric_header(
        out,
        "matrixark_backend_records_written_total",
        "counter",
        "MatrixArk storage backend records written.",
    );
    line(
        out,
        "matrixark_backend_records_written_total",
        "{backend=\"rust\"}",
        snapshot.records_written,
    );
    metric_header(
        out,
        "matrixark_backend_records_read_total",
        "counter",
        "MatrixArk storage backend records read.",
    );
    line(
        out,
        "matrixark_backend_records_read_total",
        "{backend=\"rust\"}",
        snapshot.records_read,
    );
    metric_header(
        out,
        "matrixark_backend_cached_clients",
        "gauge",
        "MatrixArk storage backend cached clients.",
    );
    line(
        out,
        "matrixark_backend_cached_clients",
        "{backend=\"rust\"}",
        snapshot.clients_created,
    );
    metric_header(
        out,
        "matrixark_backend_timeouts_total",
        "counter",
        "MatrixArk storage backend command timeouts.",
    );
    line(out, "matrixark_backend_timeouts_total", "{backend=\"rust\"}", 0);
    metric_header(
        out,
        "matrixark_backend_command_latency_ms_bucket",
        "counter",
        "MatrixArk storage backend command latency buckets.",
    );
    line(
        out,
        "matrixark_backend_command_latency_ms_bucket",
        "{backend=\"rust\",le=\"100\"}",
        latency_le_100_count(snapshot),
    );
    metric_header(
        out,
        "matrixark_backend_command_latency_max_ms",
        "gauge",
        "MatrixArk storage backend maximum command latency in milliseconds.",
    );
    line(
        out,
        "matrixark_backend_command_latency_max_ms",
        "{backend=\"rust\"}",
        max_command_latency_ms(snapshot),
    );
}
