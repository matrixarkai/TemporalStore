use serde_json::{json, Value};
use temporalstore::Client;

use crate::matrixark_rust_proxy_command_stats::command_entries;
use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_runtime::required;

pub(crate) fn put_string(client: &Client, command: Command) -> Result<Value, String> {
    client
        .put_string(
            &required(command.key, "key")?,
            &required(command.value, "value")?,
        )
        .map_err(|err| err.to_string())?;
    Ok(json!({"ok": true}))
}

pub(crate) fn get_string(client: &Client, command: Command) -> Result<Value, String> {
    let value = client
        .get_string(&required(command.key, "key")?)
        .map_err(|err| err.to_string())?;
    Ok(json!({"ok": true, "value": value}))
}

pub(crate) fn hset(client: &Client, command: Command) -> Result<Value, String> {
    client
        .hset(
            &required(command.key, "key")?,
            &required(command.field, "field")?,
            &required(command.value, "value")?,
        )
        .map_err(|err| err.to_string())?;
    Ok(json!({"ok": true}))
}

pub(crate) fn batch_hset(client: &Client, command: &Command) -> Result<Value, String> {
    let entries = command_entries(command)?;
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

pub(crate) fn batch_hget(client: &Client, command: &Command) -> Result<Value, String> {
    let entries = command_entries(command)?;
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

pub(crate) fn scan_hash(client: &Client, command: Command) -> Result<Value, String> {
    let key = required(command.key, "key")?;
    let rows = client.scan_hash(&key).map_err(|err| err.to_string())?;
    let records: Vec<Value> = rows
        .iter()
        .map(|(field, value)| json!({"key": key, "field": field, "value": value}))
        .collect();
    Ok(json!({"ok": true, "count": records.len(), "read": records.len(), "records": records}))
}

pub(crate) fn hget(client: &Client, command: Command) -> Result<Value, String> {
    let value = client
        .hget(
            &required(command.key, "key")?,
            &required(command.field, "field")?,
        )
        .map_err(|err| err.to_string())?;
    Ok(json!({"ok": true, "value": value}))
}
