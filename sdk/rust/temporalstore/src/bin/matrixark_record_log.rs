use std::io::{self, Read};

use serde::Deserialize;
use serde_json::{json, Value};
use temporalstore::{Client, Options};

#[derive(Debug, Deserialize)]
struct Command {
    op: String,
    key: Option<String>,
    field: Option<String>,
    value: Option<String>,
    metaserver: Option<String>,
    namespace: Option<String>,
    table: Option<String>,
    request_timeout_ms: Option<i32>,
    io_timeout_ms: Option<i32>,
}

fn required(value: Option<String>, name: &str) -> Result<String, String> {
    value.filter(|item| !item.is_empty()).ok_or_else(|| format!("missing {name}"))
}

fn run(command: Command) -> Result<Value, String> {
    let metaserver = command.metaserver.unwrap_or_else(|| "127.0.0.1:18000".to_string());
    let namespace = command.namespace.unwrap_or_else(|| "deploy_ns".to_string());
    let table = command.table.unwrap_or_else(|| "deploy_table".to_string());
    let mut options = Options::new(metaserver, namespace, table);
    options.psm = "matrixark.rust.mcp".to_string();
    options.request_timeout_ms = command.request_timeout_ms.unwrap_or(20_000);
    options.io_timeout_ms = command.io_timeout_ms.unwrap_or(20_000);
    let client = Client::connect(options).map_err(|err| err.to_string())?;

    match command.op.as_str() {
        "put_string" => {
            client
                .put_string(&required(command.key, "key")?, &required(command.value, "value")?)
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
        "hget" => {
            let value = client
                .hget(&required(command.key, "key")?, &required(command.field, "field")?)
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true, "value": value}))
        }
        other => Err(format!("unsupported op {other}")),
    }
}

fn main() {
    let mut input = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut input) {
        println!("{}", json!({"ok": false, "error": err.to_string()}));
        std::process::exit(1);
    }
    let command: Command = match serde_json::from_str(&input) {
        Ok(value) => value,
        Err(err) => {
            println!("{}", json!({"ok": false, "error": err.to_string()}));
            std::process::exit(1);
        }
    };
    match run(command) {
        Ok(value) => println!("{}", value),
        Err(err) => {
            println!("{}", json!({"ok": false, "error": err}));
            std::process::exit(1);
        }
    }
}
