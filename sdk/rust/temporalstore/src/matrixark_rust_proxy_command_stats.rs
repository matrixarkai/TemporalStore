use serde_json::Value;

use crate::matrixark_rust_proxy_command_entries_stats::{
    command_entry_count, command_entry_stats, hash_entry_stats,
};
use crate::matrixark_rust_proxy_metrics::CommandStats;
use crate::matrixark_rust_proxy_protocol::Command;

pub(crate) fn command_stats(command: &Command, result: &Value) -> CommandStats {
    let mut stats = CommandStats::default();
    match command.op.as_str() {
        "put_string" => {
            stats.records_written = 1;
            stats.bytes_written = command.value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
        }
        "get_string" => {
            stats.records_read = 1;
            stats.bytes_read = result
                .get("value")
                .and_then(Value::as_str)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
        }
        "hset" => {
            stats.records_written = 1;
            stats.bytes_written = command.value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
        }
        "batch_hset" => {
            let (entry_count, entry_bytes) = command_entry_stats(command);
            stats.records_written = entry_count;
            stats.bytes_written = entry_bytes;
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            let (records, bytes) = hash_entry_stats(command);
            stats.records_written = records;
            stats.bytes_written = bytes;
            if command
                .key
                .as_ref()
                .filter(|value| !value.is_empty())
                .is_some()
                && command
                    .value
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .is_some()
            {
                stats.records_written += 1;
                stats.bytes_written += command
                    .value
                    .as_ref()
                    .map(|value| value.len() as u64)
                    .unwrap_or(0);
            }
        }
        "batch_hget" | "hgetall" | "scan_hash" => {
            stats.records_read = result
                .get("read")
                .and_then(Value::as_u64)
                .or_else(|| result.get("count").and_then(Value::as_u64))
                .or_else(|| Some(command_entry_count(command)))
                .unwrap_or(0);
            stats.bytes_read = result.to_string().len() as u64;
        }
        "matrixark_scan_candidates" | "matrixark_retrieve_context_pack" => {
            stats.records_read = result.get("count").and_then(Value::as_u64).unwrap_or(0);
            stats.bytes_read = result.to_string().len() as u64;
        }
        "write_matrixark_record" => {
            stats.records_written = 1;
            stats.bytes_written = command
                .record
                .as_ref()
                .map(|record| record.to_string().len() as u64)
                .unwrap_or(0);
        }
        "write_matrixark_records" => {
            if let Some(records) = &command.records {
                stats.records_written = records.len() as u64;
                stats.bytes_written = records
                    .iter()
                    .map(|record| record.to_string().len() as u64)
                    .sum();
            }
        }
        "read_matrixark_record" => {
            stats.records_read = 1;
            stats.bytes_read = result.to_string().len() as u64;
        }
        "read_matrixark_records" => {
            stats.records_read = result
                .get("read")
                .and_then(Value::as_u64)
                .or_else(|| command.record_ids.as_ref().map(|ids| ids.len() as u64))
                .unwrap_or(0);
            stats.bytes_read = result.to_string().len() as u64;
        }
        "hget" => {
            stats.records_read = 1;
            stats.bytes_read = result
                .get("value")
                .and_then(Value::as_str)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
        }
        _ => {}
    }
    stats
}
