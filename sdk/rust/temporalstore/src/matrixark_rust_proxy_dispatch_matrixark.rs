use serde_json::{json, Value};
use temporalstore::Client;

use crate::matrixark_rust_proxy_command_entries::command_entries;
use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_records::{read_matrixark_record, write_matrixark_record};
use crate::matrixark_rust_proxy_runtime::required;

pub(crate) fn append_records(client: &Client, command: &Command) -> Result<Value, String> {
    let entries = command_entries(command)?;
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

pub(crate) fn write_record(client: &Client, command: &Command) -> Result<Value, String> {
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

pub(crate) fn write_records(client: &Client, command: &Command) -> Result<Value, String> {
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

pub(crate) fn read_record(client: &Client, command: Command) -> Result<Value, String> {
    let record_type = required(command.record_type, "record_type")?;
    let tenant_hash = command
        .tenant_hash
        .ok_or_else(|| "missing tenant_hash".to_string())?;
    let record_id = required(command.record_id, "record_id")?;
    let read = read_matrixark_record(client, &record_type, tenant_hash, &record_id)?;
    Ok(json!({"ok": true, "read": read}))
}

pub(crate) fn read_records(client: &Client, command: Command) -> Result<Value, String> {
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
