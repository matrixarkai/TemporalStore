use crate::matrixark_rust_proxy_clock::unix_ms;
use crate::matrixark_rust_proxy_metrics::MetricsSnapshot;

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
    let elapsed_seconds =
        ((unix_ms().saturating_sub(snapshot.started_at_unix_ms)) as f64 / 1000.0).max(0.001);
    line(
        out,
        "matrixark_backend_qps",
        "{backend=\"rust\"}",
        format!("{:.6}", snapshot.commands_total as f64 / elapsed_seconds),
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
    let le_100 = snapshot
        .op
        .values()
        .map(|metrics| {
            if metrics.latency_ms_max <= 100 {
                metrics.ok + metrics.failed
            } else {
                0
            }
        })
        .sum::<u64>();
    line(
        out,
        "matrixark_backend_command_latency_ms_bucket",
        "{backend=\"rust\",le=\"100\"}",
        le_100,
    );
    metric_header(
        out,
        "matrixark_backend_command_latency_max_ms",
        "gauge",
        "MatrixArk storage backend maximum command latency in milliseconds.",
    );
    let max_latency = snapshot
        .op
        .values()
        .map(|metrics| metrics.latency_ms_max)
        .max()
        .unwrap_or(0);
    line(
        out,
        "matrixark_backend_command_latency_max_ms",
        "{backend=\"rust\"}",
        max_latency,
    );
}
