use std::collections::HashMap;

use serde_json::Value;

#[derive(Clone, Debug, Default)]
pub(crate) struct OpMetrics {
    ok: u64,
    failed: u64,
    latency_ms_sum: u128,
    latency_ms_max: u128,
}

#[derive(Clone, Debug)]
pub(crate) struct MetricsSnapshot {
    pub started_at_unix_ms: u128,
    pub commands_total: u64,
    pub commands_failed: u64,
    pub records_written: u64,
    pub records_read: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub clients_created: u64,
    pub parse_errors: u64,
    pub client_connect_errors: u64,
    pub rust_engine_time_ms_sum: u128,
    pub rust_engine_time_ms_max: u128,
    pub serialization_time_ms_sum: u128,
    pub serialization_time_ms_max: u128,
    pub scan_count_total: u64,
    pub cache_hit_total: u64,
    pub selected_refs_total: u64,
    pub dropped_refs_total: u64,
    pub op: HashMap<String, OpMetrics>,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            started_at_unix_ms: unix_ms(),
            commands_total: 0,
            commands_failed: 0,
            records_written: 0,
            records_read: 0,
            bytes_written: 0,
            bytes_read: 0,
            clients_created: 0,
            parse_errors: 0,
            client_connect_errors: 0,
            rust_engine_time_ms_sum: 0,
            rust_engine_time_ms_max: 0,
            serialization_time_ms_sum: 0,
            serialization_time_ms_max: 0,
            scan_count_total: 0,
            cache_hit_total: 0,
            selected_refs_total: 0,
            dropped_refs_total: 0,
            op: HashMap::new(),
        }
    }
}

impl MetricsSnapshot {
    pub fn observe(
        &mut self,
        op: &str,
        ok: bool,
        elapsed_ms: u128,
        serialization_ms: u128,
        result: Option<&Value>,
        stats: CommandStats,
    ) {
        self.commands_total += 1;
        if !ok {
            self.commands_failed += 1;
        }
        self.rust_engine_time_ms_sum += elapsed_ms;
        self.rust_engine_time_ms_max = self.rust_engine_time_ms_max.max(elapsed_ms);
        self.serialization_time_ms_sum += serialization_ms;
        self.serialization_time_ms_max = self.serialization_time_ms_max.max(serialization_ms);
        if let Some(result) = result {
            self.scan_count_total += result
                .get("scan_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if result
                .get("cache_hit")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                self.cache_hit_total += 1;
            }
            self.selected_refs_total += result
                .get("selected_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            self.dropped_refs_total += result
                .get("dropped_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
        }
        self.records_written += stats.records_written;
        self.records_read += stats.records_read;
        self.bytes_written += stats.bytes_written;
        self.bytes_read += stats.bytes_read;
        let entry = self.op.entry(op.to_string()).or_default();
        if ok {
            entry.ok += 1;
        } else {
            entry.failed += 1;
        }
        entry.latency_ms_sum += elapsed_ms;
        entry.latency_ms_max = entry.latency_ms_max.max(elapsed_ms);
    }

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
        metric_header(
            &mut out,
            "matrixark_backend_info",
            "gauge",
            "MatrixArk storage backend identity and storage mode.",
        );
        line(
            &mut out,
            "matrixark_backend_info",
            &format!(
                "{{backend=\"rust\",storage_mode=\"{}\"}}",
                matrixark_rust_storage_mode()
            ),
            1,
        );
        metric_header(
            &mut out,
            "matrixark_backend_qps",
            "gauge",
            "MatrixArk storage backend command QPS.",
        );
        let elapsed_seconds =
            ((unix_ms().saturating_sub(self.started_at_unix_ms)) as f64 / 1000.0).max(0.001);
        line(
            &mut out,
            "matrixark_backend_qps",
            "{backend=\"rust\"}",
            format!("{:.6}", self.commands_total as f64 / elapsed_seconds),
        );
        metric_header(
            &mut out,
            "matrixark_backend_commands_total",
            "counter",
            "MatrixArk storage backend command count.",
        );
        line(
            &mut out,
            "matrixark_backend_commands_total",
            "{backend=\"rust\"}",
            self.commands_total,
        );
        metric_header(
            &mut out,
            "matrixark_backend_errors_total",
            "counter",
            "MatrixArk storage backend command errors.",
        );
        line(
            &mut out,
            "matrixark_backend_errors_total",
            "{backend=\"rust\"}",
            self.commands_failed,
        );
        metric_header(
            &mut out,
            "matrixark_backend_records_written_total",
            "counter",
            "MatrixArk storage backend records written.",
        );
        line(
            &mut out,
            "matrixark_backend_records_written_total",
            "{backend=\"rust\"}",
            self.records_written,
        );
        metric_header(
            &mut out,
            "matrixark_backend_records_read_total",
            "counter",
            "MatrixArk storage backend records read.",
        );
        line(
            &mut out,
            "matrixark_backend_records_read_total",
            "{backend=\"rust\"}",
            self.records_read,
        );
        metric_header(
            &mut out,
            "matrixark_backend_cached_clients",
            "gauge",
            "MatrixArk storage backend cached clients.",
        );
        line(
            &mut out,
            "matrixark_backend_cached_clients",
            "{backend=\"rust\"}",
            self.clients_created,
        );
        metric_header(
            &mut out,
            "matrixark_backend_timeouts_total",
            "counter",
            "MatrixArk storage backend command timeouts.",
        );
        line(
            &mut out,
            "matrixark_backend_timeouts_total",
            "{backend=\"rust\"}",
            0,
        );
        metric_header(
            &mut out,
            "matrixark_backend_command_latency_ms_bucket",
            "counter",
            "MatrixArk storage backend command latency buckets.",
        );
        let le_100 = self
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
            &mut out,
            "matrixark_backend_command_latency_ms_bucket",
            "{backend=\"rust\",le=\"100\"}",
            le_100,
        );
        metric_header(
            &mut out,
            "matrixark_backend_command_latency_max_ms",
            "gauge",
            "MatrixArk storage backend maximum command latency in milliseconds.",
        );
        let max_latency = self
            .op
            .values()
            .map(|metrics| metrics.latency_ms_max)
            .max()
            .unwrap_or(0);
        line(
            &mut out,
            "matrixark_backend_command_latency_max_ms",
            "{backend=\"rust\"}",
            max_latency,
        );
        out
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CommandStats {
    pub records_written: u64,
    pub records_read: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

pub(crate) fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn metric_header(out: &mut String, name: &str, metric_type: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(metric_type);
    out.push('\n');
}

fn line<T: std::fmt::Display>(out: &mut String, name: &str, labels: &str, value: T) {
    out.push_str(name);
    out.push_str(labels);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn matrixark_rust_storage_mode() -> &'static str {
    "rust-proxy"
}

pub(crate) fn matrixark_rust_service_mode() -> &'static str {
    "rust_proxy_stdio"
}

