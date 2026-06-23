use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::io::{self, Read};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::json;
use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, TemporalEngine};

const DEFAULT_SHARD_ID: u64 = 1;

#[derive(Debug, Deserialize)]
struct RecordLogRequest {
    op: String,
    #[serde(default)]
    metaserver: String,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    table: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    field: String,
    #[serde(default)]
    value: String,
}

#[derive(Debug, Serialize)]
struct RecordLogResponse {
    ok: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    value: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
}

fn main() {
    let response = match run() {
        Ok(value) => RecordLogResponse {
            ok: true,
            value,
            error: String::new(),
        },
        Err(error) => RecordLogResponse {
            ok: false,
            value: String::new(),
            error,
        },
    };
    println!(
        "{}",
        serde_json::to_string(&response).expect("record-log response should serialize")
    );
    if !response.ok {
        std::process::exit(1);
    }
}

fn run() -> Result<String, String> {
    let mut input = String::new();
    io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| format!("failed to read request: {error}"))?;
    let request: RecordLogRequest =
        serde_json::from_str(&input).map_err(|error| format!("invalid JSON request: {error}"))?;
    let engine = open_engine(&request)?;
    match request.op.as_str() {
        "put_string" => {
            execute_empty(
                &engine,
                Command::StringSet {
                    key: request.key,
                    value: request.value.into_bytes(),
                },
            )?;
            Ok(String::new())
        }
        "get_string" => read_bytes(&engine, Command::StringGet { key: request.key }),
        "hset" => {
            execute_empty(
                &engine,
                Command::HashSet {
                    key: request.key,
                    field: request.field,
                    value: request.value.into_bytes(),
                },
            )?;
            Ok(String::new())
        }
        "hget" => read_bytes(
            &engine,
            Command::HashGet {
                key: request.key,
                field: request.field,
            },
        ),
        other => Err(format!("unsupported op {other:?}")),
    }
}

fn open_engine(request: &RecordLogRequest) -> Result<TemporalEngine, String> {
    let root = record_log_root(request);
    std::fs::create_dir_all(&root).map_err(|error| {
        format!(
            "failed to create record-log root {}: {error}",
            root.display()
        )
    })?;
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(DEFAULT_SHARD_ID);
    Ok(engine)
}

fn execute_empty(engine: &TemporalEngine, command: Command) -> Result<(), String> {
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command,
    });
    if !response.status.ok {
        return Err(format!(
            "{}: {}",
            response.status.code, response.status.message
        ));
    }
    match response.response {
        CommandResponse::Empty => Ok(()),
        other => Err(format!("unexpected response for write: {other:?}")),
    }
}

fn read_bytes(engine: &TemporalEngine, command: Command) -> Result<String, String> {
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command,
    });
    if !response.status.ok {
        return Err(format!(
            "{}: {}",
            response.status.code, response.status.message
        ));
    }
    match response.response {
        CommandResponse::Bytes { value } => value
            .map(|bytes| {
                String::from_utf8(bytes)
                    .map_err(|error| format!("stored value is not UTF-8: {error}"))
            })
            .transpose()
            .map(|value| value.unwrap_or_default()),
        other => Err(format!("unexpected response for read: {other:?}")),
    }
}

fn record_log_root(request: &RecordLogRequest) -> PathBuf {
    if let Ok(root) = env::var("MATRIXARK_TEMPORALSTORE_RUST_ROOT") {
        return PathBuf::from(root);
    }
    let namespace = non_empty_or(&request.namespace, "deploy_ns");
    let table = non_empty_or(&request.table, "deploy_table");
    let metaserver_hash = stable_hash64(non_empty_or(&request.metaserver, "local"));
    env::temp_dir()
        .join("temporalstore-rust-matrixark-record-log")
        .join(sanitize_path_component(namespace))
        .join(sanitize_path_component(table))
        .join(format!("{metaserver_hash:016x}"))
}

fn non_empty_or<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.trim().is_empty() {
        fallback
    } else {
        value
    }
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn stable_hash64(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[allow(dead_code)]
fn _request_shape_for_docs() -> serde_json::Value {
    json!({
        "op": "hset",
        "metaserver": "127.0.0.1:18000",
        "namespace": "deploy_ns",
        "table": "deploy_table",
        "key": "matrixark:mcp:records:000000",
        "field": "00000000000000000000",
        "value": "{\"record_type\":\"raw_event\"}"
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn request(op: &str) -> RecordLogRequest {
        RecordLogRequest {
            op: op.to_string(),
            metaserver: "127.0.0.1:18000".to_string(),
            namespace: "codex_ns".to_string(),
            table: "codex_table".to_string(),
            key: String::new(),
            field: String::new(),
            value: String::new(),
        }
    }

    // shared-corpus: codex_mcp_temporalstore_rust_record_log_backend
    #[test]
    fn record_log_root_is_stable_and_partitioned() {
        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
        let first = request("get_string");
        let mut second = request("get_string");
        second.table = "other_table".to_string();

        let first_root = record_log_root(&first);
        assert_eq!(
            first_root.file_name().and_then(|value| value.to_str()),
            Some(&format!("{:016x}", stable_hash64("127.0.0.1:18000"))[..])
        );
        assert!(first_root.to_string_lossy().contains("codex_ns"));
        assert!(first_root.to_string_lossy().contains("codex_table"));
        assert_ne!(first_root, record_log_root(&second));
    }

    // shared-corpus: codex_mcp_temporalstore_rust_record_log_backend
    #[test]
    fn rust_record_log_persists_string_and_hash_records() {
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());

        let mut put = request("put_string");
        put.key = "matrixark:test:string".to_string();
        put.value = "hello-rust-mcp".to_string();
        let engine = open_engine(&put).expect("engine");
        execute_empty(
            &engine,
            Command::StringSet {
                key: put.key.clone(),
                value: put.value.clone().into_bytes(),
            },
        )
        .expect("put string");

        let reopened = open_engine(&put).expect("reopened engine");
        assert_eq!(
            read_bytes(
                &reopened,
                Command::StringGet {
                    key: put.key.clone(),
                },
            )
            .expect("get string"),
            "hello-rust-mcp"
        );

        execute_empty(
            &reopened,
            Command::HashSet {
                key: "matrixark:test:hash".to_string(),
                field: "00000000000000000000".to_string(),
                value: br#"{"record_type":"raw_event"}"#.to_vec(),
            },
        )
        .expect("hset");

        let reopened_again = open_engine(&put).expect("reopened engine again");
        assert_eq!(
            read_bytes(
                &reopened_again,
                Command::HashGet {
                    key: "matrixark:test:hash".to_string(),
                    field: "00000000000000000000".to_string(),
                },
            )
            .expect("hget"),
            r#"{"record_type":"raw_event"}"#
        );

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }
}
