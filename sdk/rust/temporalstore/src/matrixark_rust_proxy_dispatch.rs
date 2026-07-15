use serde_json::{json, Value};
use temporalstore::Client;

use crate::matrixark_rust_proxy_command_stats::command_entries;
use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_records::{read_matrixark_record, write_matrixark_record};
use crate::matrixark_rust_proxy_retrieve::retrieve_context_pack_native;
use crate::matrixark_rust_proxy_runtime::{connect, required};
use crate::matrixark_rust_proxy_scan::scan_matrixark_candidates;

pub(crate) fn run_with_client(client: &Client, command: Command) -> Result<Value, String> {
    match command.op.as_str() {
        "put_string" => {
            client
                .put_string(
                    &required(command.key, "key")?,
                    &required(command.value, "value")?,
                )
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true}))
        }
        "get_string" => {
            let value = client
                .get_string(&required(command.key, "key")?)
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true, "value": value}))
        }
        "hset" => {
            client
                .hset(
                    &required(command.key, "key")?,
                    &required(command.field, "field")?,
                    &required(command.value, "value")?,
                )
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true}))
        }
        "batch_hset" => {
            let entries = command_entries(&command)?;
            if entries.is_empty() {
                return Err("missing entries".to_string());
            }
            for entry in &entries {
                client
                    .hset(entry.key, entry.field, entry.value)
                    .map_err(|err| err.to_string())?;
            }
            Ok(json!({"ok": true, "written": entries.len(), "batch_lowering": "raw_hset"}))
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            let entries = command_entries(&command)?;
            if entries.is_empty()
                && command
                    .key
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .is_none()
            {
                return Err("missing entries".to_string());
            }
            let count_key = command.key.as_deref().filter(|value| !value.is_empty());
            let count_value = command.value.as_deref().filter(|value| !value.is_empty());
            let batch: Vec<(&str, &str, &str)> = entries
                .iter()
                .map(|entry| (entry.key, entry.field, entry.value))
                .collect();
            client
                .matrixark_batch_append_records(&batch, count_key, count_value)
                .map_err(|err| err.to_string())?;
            let mut written = entries.len();
            if count_key.is_some() && count_value.is_some() {
                written += 1;
            }
            let append_options = command.append_options.as_ref();
            let raw_backend = append_options
                .and_then(|options| options.get("raw_storage_backend"))
                .and_then(Value::as_str)
                .unwrap_or("temporalstore");
            let append_path = append_options
                .and_then(|options| options.get("append_path"))
                .and_then(Value::as_str)
                .unwrap_or("native_batch_append_records");
            Ok(json!({
                "ok": true,
                "written": written,
                "append_api": command.op,
                "native_append": true,
                "append_path": append_path,
                "raw_storage_backend": raw_backend,
                "batch_lowering": "none"
            }))
        }
        "batch_hget" => {
            let entries = command_entries(&command)?;
            if entries.is_empty() {
                return Err("missing entries".to_string());
            }
            let mut reads = Vec::with_capacity(entries.len());
            for entry in &entries {
                let value = client
                    .hget(entry.key, entry.field)
                    .map_err(|err| err.to_string())?;
                reads.push(json!({"key": entry.key, "field": entry.field, "value": value}));
            }
            Ok(json!({"ok": true, "read": reads.len(), "records": reads}))
        }
        "hgetall" | "scan_hash" => {
            let key = required(command.key, "key")?;
            let rows = client.scan_hash(&key).map_err(|err| err.to_string())?;
            let records: Vec<Value> = rows
                .iter()
                .map(|(field, value)| json!({"key": key, "field": field, "value": value}))
                .collect();
            Ok(
                json!({"ok": true, "count": records.len(), "read": records.len(), "records": records}),
            )
        }
        "matrixark_scan_candidates" => scan_matrixark_candidates(client, &command),
        "matrixark_retrieve_context_pack" => retrieve_context_pack_native(client, &command),
        "write_matrixark_record" => {
            let record = command
                .record
                .as_ref()
                .ok_or_else(|| "missing record".to_string())?;
            let write = write_matrixark_record(
                client,
                record,
                command.record_type.as_ref(),
                command.tenant_hash,
                command.record_id.as_ref(),
            )?;
            Ok(json!({"ok": true, "write": write}))
        }
        "write_matrixark_records" => {
            let records = command
                .records
                .as_ref()
                .ok_or_else(|| "missing records".to_string())?;
            let mut writes = Vec::with_capacity(records.len());
            for record in records {
                writes.push(write_matrixark_record(
                    client,
                    record,
                    command.record_type.as_ref(),
                    command.tenant_hash,
                    None,
                )?);
            }
            Ok(json!({"ok": true, "written": writes.len(), "writes": writes}))
        }
        "read_matrixark_record" => {
            let record_type = required(command.record_type, "record_type")?;
            let tenant_hash = command
                .tenant_hash
                .ok_or_else(|| "missing tenant_hash".to_string())?;
            let record_id = required(command.record_id, "record_id")?;
            let read = read_matrixark_record(client, &record_type, tenant_hash, &record_id)?;
            Ok(json!({"ok": true, "read": read}))
        }
        "read_matrixark_records" => {
            let record_type = required(command.record_type, "record_type")?;
            let tenant_hash = command
                .tenant_hash
                .ok_or_else(|| "missing tenant_hash".to_string())?;
            let record_ids = command
                .record_ids
                .as_ref()
                .ok_or_else(|| "missing record_ids".to_string())?;
            let mut reads = Vec::with_capacity(record_ids.len());
            for record_id in record_ids {
                reads.push(read_matrixark_record(
                    client,
                    &record_type,
                    tenant_hash,
                    record_id,
                )?);
            }
            Ok(json!({"ok": true, "read": reads.len(), "records": reads}))
        }
        "hget" => {
            let value = client
                .hget(
                    &required(command.key, "key")?,
                    &required(command.field, "field")?,
                )
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true, "value": value}))
        }
        other => Err(format!("unsupported op {other}")),
    }
}

pub(crate) fn run(command: Command) -> Result<Value, String> {
    let client = connect(&command)?;
    run_with_client(&client, command)
}
