use crate::matrixark_rust_proxy_metrics::MetricsSnapshot;

use crate::matrixark_rust_proxy_metrics_backend_render::append_backend_metrics;
use crate::matrixark_rust_proxy_metrics_format::{escape_label, line, metric_header};

impl MetricsSnapshot {
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        metric_header(
            &mut out,
            "matrixark_rust_proxy_process_start_time_ms",
            "gauge",
            "Unix millisecond timestamp when this Rust proxy process started.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_process_start_time_ms",
            "",
            self.started_at_unix_ms,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_commands_total",
            "counter",
            "Total MatrixArk Rust proxy commands by op and status.",
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_command_latency_ms_sum",
            "counter",
            "Total command latency in milliseconds by op.",
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_command_latency_ms_max",
            "gauge",
            "Maximum observed command latency in milliseconds by op.",
        );
        metric_header(
            &mut out,
            "matrixark_backend_rust_engine_time_ms_total",
            "counter",
            "Total Rust engine execution time in milliseconds.",
        );
        line(
            &mut out,
            "matrixark_backend_rust_engine_time_ms_total",
            "{backend=\"rust\"}",
            self.rust_engine_time_ms_sum,
        );
        metric_header(
            &mut out,
            "matrixark_backend_serialization_time_ms_total",
            "counter",
            "Total Rust proxy response serialization time in milliseconds.",
        );
        line(
            &mut out,
            "matrixark_backend_serialization_time_ms_total",
            "{backend=\"rust\"}",
            self.serialization_time_ms_sum,
        );
        metric_header(
            &mut out,
            "matrixark_retrieve_scan_count_total",
            "counter",
            "Total records scanned by native MatrixArk retrieval calls.",
        );
        line(
            &mut out,
            "matrixark_retrieve_scan_count_total",
            "{backend=\"rust\"}",
            self.scan_count_total,
        );
        metric_header(
            &mut out,
            "matrixark_retrieve_cache_hits_total",
            "counter",
            "Total native MatrixArk retrieval cache hits.",
        );
        line(
            &mut out,
            "matrixark_retrieve_cache_hits_total",
            "{backend=\"rust\"}",
            self.cache_hit_total,
        );
        metric_header(
            &mut out,
            "matrixark_context_pack_selected_refs_total",
            "counter",
            "Total refs selected by native MatrixArk ContextPack assembly.",
        );
        line(
            &mut out,
            "matrixark_context_pack_selected_refs_total",
            "{backend=\"rust\"}",
            self.selected_refs_total,
        );
        metric_header(
            &mut out,
            "matrixark_context_pack_dropped_refs_total",
            "counter",
            "Total refs dropped by native MatrixArk ContextPack assembly.",
        );
        line(
            &mut out,
            "matrixark_context_pack_dropped_refs_total",
            "{backend=\"rust\"}",
            self.dropped_refs_total,
        );
        let mut ops: Vec<_> = self.op.iter().collect();
        ops.sort_by(|a, b| a.0.cmp(b.0));
        for (op, metrics) in ops {
            let ok_labels = format!("{{op=\"{}\",status=\"ok\"}}", escape_label(op));
            let fail_labels = format!("{{op=\"{}\",status=\"error\"}}", escape_label(op));
            line(
                &mut out,
                "matrixark_rust_proxy_commands_total",
                &ok_labels,
                metrics.ok,
            );
            line(
                &mut out,
                "matrixark_rust_proxy_commands_total",
                &fail_labels,
                metrics.failed,
            );
            let op_labels = format!("{{op=\"{}\"}}", escape_label(op));
            line(
                &mut out,
                "matrixark_rust_proxy_command_latency_ms_sum",
                &op_labels,
                metrics.latency_ms_sum,
            );
            line(
                &mut out,
                "matrixark_rust_proxy_command_latency_ms_max",
                &op_labels,
                metrics.latency_ms_max,
            );
        }
        metric_header(
            &mut out,
            "matrixark_rust_proxy_records_written_total",
            "counter",
            "Total MatrixArk records/hash entries written by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_records_written_total",
            "",
            self.records_written,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_records_read_total",
            "counter",
            "Total MatrixArk records/hash entries read by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_records_read_total",
            "",
            self.records_read,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_bytes_written_total",
            "counter",
            "Approximate payload bytes written by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_bytes_written_total",
            "",
            self.bytes_written,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_bytes_read_total",
            "counter",
            "Approximate payload bytes read by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_bytes_read_total",
            "",
            self.bytes_read,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_clients_created_total",
            "counter",
            "TemporalStore clients created by the Rust proxy/direct SDK bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_clients_created_total",
            "",
            self.clients_created,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_parse_errors_total",
            "counter",
            "Invalid JSON command lines received by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_parse_errors_total",
            "",
            self.parse_errors,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_client_connect_errors_total",
            "counter",
            "TemporalStore client connection failures in the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_client_connect_errors_total",
            "",
            self.client_connect_errors,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_commands_failed_total",
            "counter",
            "Total failed MatrixArk Rust proxy commands.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_commands_failed_total",
            "",
            self.commands_failed,
        );
        append_backend_metrics(&mut out, self);
        out
    }
}
