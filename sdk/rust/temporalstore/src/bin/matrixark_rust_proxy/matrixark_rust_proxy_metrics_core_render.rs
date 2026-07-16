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
