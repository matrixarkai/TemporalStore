use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};

use serde::Deserialize;
use serde_json::{json, Value};
use temporalstore::{Client, Options};

#[derive(Debug, Deserialize)]
struct Command {
    op: String,
    key: Option<String>,
    field: Option<String>,
    value: Option<String>,
    entries: Option<Vec<HashEntry>>,
    record: Option<Value>,
    records: Option<Vec<Value>>,
    record_type: Option<String>,
    tenant_hash: Option<u64>,
    record_id: Option<String>,
    metaserver: Option<String>,
    namespace: Option<String>,
    table: Option<String>,
    request_timeout_ms: Option<i32>,
    io_timeout_ms: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct HashEntry {
    key: String,
    field: String,
    value: String,
}

fn required(value: Option<String>, name: &str) -> Result<String, String> {
    value
        .filter(|item| !item.is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

fn effective_config(command: &Command) -> (String, String, String, i32, i32) {
    (
        command
            .metaserver
            .clone()
            .unwrap_or_else(|| "127.0.0.1:18000".to_string()),
        command
            .namespace
            .clone()
            .unwrap_or_else(|| "deploy_ns".to_string()),
        command
            .table
            .clone()
            .unwrap_or_else(|| "deploy_table".to_string()),
        command.request_timeout_ms.unwrap_or(20_000),
        command.io_timeout_ms.unwrap_or(20_000),
    )
}

fn connect(command: &Command) -> Result<Client, String> {
    let (metaserver, namespace, table, request_timeout_ms, io_timeout_ms) =
        effective_config(command);
    let mut options = Options::new(metaserver, namespace, table);
    options.psm = "matrixark.rust.mcp".to_string();
    options.request_timeout_ms = request_timeout_ms;
    options.io_timeout_ms = io_timeout_ms;
    Client::connect(options).map_err(|err| err.to_string())
}

fn config_key(command: &Command) -> String {
    let (metaserver, namespace, table, request_timeout_ms, io_timeout_ms) =
        effective_config(command);
    format!(
        "{metaserver}\u{1f}{namespace}\u{1f}{table}\u{1f}{request_timeout_ms}\u{1f}{io_timeout_ms}"
    )
}

fn value_u64(record: &Value, field: &str) -> Option<u64> {
    record.get(field).and_then(Value::as_u64)
}

fn value_str(record: &Value, field: &str) -> Option<String> {
    record
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn matrixark_record_type(record: &Value, fallback: Option<&String>) -> Result<String, String> {
    value_str(record, "record_type")
        .or_else(|| fallback.cloned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "matrixark record missing record_type".to_string())
}

fn matrixark_tenant_hash(record: &Value, fallback: Option<u64>) -> Result<u64, String> {
    value_u64(record, "tenant_hash")
        .or(fallback)
        .ok_or_else(|| "matrixark record missing tenant_hash".to_string())
}

fn matrixark_record_id(record: &Value, fallback: Option<&String>) -> Result<String, String> {
    if let Some(value) = fallback.filter(|value| !value.is_empty()) {
        return Ok(value.clone());
    }
    for field in [
        "record_id",
        "node_hash",
        "event_id_hash",
        "entity_hash",
        "resource_hash",
        "chunk_hash",
        "skill_hash",
        "section_hash",
        "summary_hash",
        "ref_hash",
        "query_id_hash",
        "compression_id_hash",
    ] {
        if let Some(value) = record.get(field) {
            if let Some(number) = value.as_u64() {
                return Ok(number.to_string());
            }
            if let Some(text) = value.as_str() {
                if !text.is_empty() {
                    return Ok(text.to_string());
                }
            }
        }
    }
    Err("matrixark record missing stable id".to_string())
}

fn matrixark_storage_key(record_type: &str, tenant_hash: u64) -> String {
    format!("matrixark:record:{record_type}:{tenant_hash}")
}

fn matrixark_storage_field(record_id: &str) -> String {
    record_id.to_string()
}

fn write_matrixark_record(
    client: &Client,
    record: &Value,
    record_type_fallback: Option<&String>,
    tenant_hash_fallback: Option<u64>,
    record_id_fallback: Option<&String>,
) -> Result<Value, String> {
    let record_type = matrixark_record_type(record, record_type_fallback)?;
    let tenant_hash = matrixark_tenant_hash(record, tenant_hash_fallback)?;
    let record_id = matrixark_record_id(record, record_id_fallback)?;
    let key = matrixark_storage_key(&record_type, tenant_hash);
    let field = matrixark_storage_field(&record_id);
    client
        .hset(
            &key,
            &field,
            &serde_json::to_string(record).map_err(|err| err.to_string())?,
        )
        .map_err(|err| err.to_string())?;
    Ok(json!({"key": key, "field": field, "record_type": record_type, "record_id": record_id}))
}

fn run_with_client(client: &Client, command: Command) -> Result<Value, String> {
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
            let entries = command
                .entries
                .as_ref()
                .ok_or_else(|| "missing entries".to_string())?;
            for entry in entries {
                client
                    .hset(&entry.key, &entry.field, &entry.value)
                    .map_err(|err| err.to_string())?;
            }
            Ok(json!({"ok": true, "written": entries.len()}))
        }
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
            let key = matrixark_storage_key(&record_type, tenant_hash);
            let field = matrixark_storage_field(&record_id);
            let value = client.hget(&key, &field).map_err(|err| err.to_string())?;
            Ok(json!({"ok": true, "key": key, "field": field, "value": value}))
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

fn run(command: Command) -> Result<Value, String> {
    let client = connect(&command)?;
    run_with_client(&client, command)
}

fn print_result(result: Result<Value, String>) -> bool {
    match result {
        Ok(value) => {
            println!("{}", value);
            true
        }
        Err(err) => {
            println!("{}", json!({"ok": false, "error": err}));
            false
        }
    }
}

fn serve() -> i32 {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut clients: HashMap<String, Client> = HashMap::new();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(value) => value,
            Err(err) => {
                println!("{}", json!({"ok": false, "error": err.to_string()}));
                let _ = stdout.flush();
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let command: Command = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(err) => {
                println!("{}", json!({"ok": false, "error": err.to_string()}));
                let _ = stdout.flush();
                continue;
            }
        };
        let key = config_key(&command);
        if !clients.contains_key(&key) {
            match connect(&command) {
                Ok(client) => {
                    clients.insert(key.clone(), client);
                }
                Err(err) => {
                    println!("{}", json!({"ok": false, "error": err}));
                    let _ = stdout.flush();
                    continue;
                }
            }
        }
        let result = clients
            .get(&key)
            .ok_or_else(|| "missing cached TemporalStore client".to_string())
            .and_then(|client| run_with_client(client, command));
        print_result(result);
        let _ = stdout.flush();
    }
    0
}

fn single_shot() -> i32 {
    let mut input = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut input) {
        println!("{}", json!({"ok": false, "error": err.to_string()}));
        return 1;
    }
    let command: Command = match serde_json::from_str(&input) {
        Ok(value) => value,
        Err(err) => {
            println!("{}", json!({"ok": false, "error": err.to_string()}));
            return 1;
        }
    };
    if print_result(run(command)) {
        0
    } else {
        1
    }
}

fn main() {
    let code = if std::env::args().any(|arg| arg == "--serve") {
        serve()
    } else {
        single_shot()
    };
    std::process::exit(code);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrixark_record_derives_storage_key_from_common_ids() {
        let record = json!({
            "record_type": "resource_manifest",
            "tenant_hash": 77,
            "resource_hash": 7001,
            "raw_uri": "file:///runbooks/gpu.md"
        });
        assert_eq!(
            matrixark_record_type(&record, None).unwrap(),
            "resource_manifest"
        );
        assert_eq!(matrixark_tenant_hash(&record, None).unwrap(), 77);
        assert_eq!(matrixark_record_id(&record, None).unwrap(), "7001");
        assert_eq!(
            matrixark_storage_key("resource_manifest", 77),
            "matrixark:record:resource_manifest:77"
        );
        assert_eq!(matrixark_storage_field("7001"), "7001");
    }

    #[test]
    fn matrixark_record_allows_explicit_fallbacks() {
        let record = json!({"payload": "minimal"});
        assert_eq!(
            matrixark_record_type(&record, Some(&"skill_section".to_string())).unwrap(),
            "skill_section"
        );
        assert_eq!(matrixark_tenant_hash(&record, Some(9)).unwrap(), 9);
        assert_eq!(
            matrixark_record_id(&record, Some(&"section-a".to_string())).unwrap(),
            "section-a"
        );
    }

    #[test]
    fn matrixark_record_rejects_missing_identity() {
        let record = json!({"record_type": "context_event", "tenant_hash": 1});
        assert!(matrixark_record_id(&record, None).is_err());
        assert!(matrixark_record_type(&json!({}), None).is_err());
        assert!(matrixark_tenant_hash(&json!({}), None).is_err());
    }
}
