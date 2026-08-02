use std::collections::HashMap;

use serde_json::Value;

use crate::matrixark_rust_proxy_clock::unix_ms;

#[derive(Clone, Debug, Default)]
pub(crate) struct OpMetrics {
    pub(crate) ok: u64,
    pub(crate) failed: u64,
    pub(crate) latency_ms_sum: u128,
    pub(crate) latency_ms_max: u128,
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
    pub matrixark_append_blob_parity_total: u64,
    pub matrixark_append_hset_count_lowering_total: u64,
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
            matrixark_append_blob_parity_total: 0,
            matrixark_append_hset_count_lowering_total: 0,
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
            if matches!(
                op,
                "matrixark_append_records" | "matrixark_batch_append_records"
            ) {
                if result
                    .get("append_blob_parity")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    self.matrixark_append_blob_parity_total += 1;
                }
                if result
                    .get("batch_lowering")
                    .and_then(Value::as_str)
                    .map(|value| value == "rust_proxy_hset_count_lowering")
                    .unwrap_or(false)
                {
                    self.matrixark_append_hset_count_lowering_total += 1;
                }
            }
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
}


#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CommandStats {
    pub records_written: u64,
    pub records_read: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

pub(crate) fn matrixark_rust_service_mode() -> &'static str {
    "rust_proxy_stdio"
}
