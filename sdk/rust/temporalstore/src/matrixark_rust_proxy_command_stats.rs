use serde_json::Value;

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

fn command_entry_count(command: &Command) -> u64 {
    command
        .entries_compact
        .as_ref()
        .map(|entries| entries.len() as u64)
        .or_else(|| command.entries.as_ref().map(|entries| entries.len() as u64))
        .unwrap_or(0)
}

fn command_entry_stats(command: &Command) -> (u64, u64) {
    if let Some(entries) = &command.entries_compact {
        let bytes = entries.iter().map(|entry| entry[2].len() as u64).sum();
        return (entries.len() as u64, bytes);
    }
    if let Some(entries) = &command.entries {
        let bytes = entries
            .iter()
            .map(|entry| {
                entry
                    .value
                    .as_ref()
                    .map(|value| value.len() as u64)
                    .unwrap_or(0)
            })
            .sum();
        return (entries.len() as u64, bytes);
    }
    (0, 0)
}

fn hash_entry_stats(command: &Command) -> (u64, u64) {
    let mut records = 0_u64;
    let mut bytes = 0_u64;
    if let Some(entries) = &command.entries {
        for entry in entries {
            if let Some(value) = entry.value.as_ref() {
                records += 1;
                bytes += value.len() as u64;
            }
        }
    }
    if let Some(entries) = &command.entries_compact {
        for entry in entries {
            records += 1;
            bytes += entry[2].len() as u64;
        }
    }
    (records, bytes)
}
