use serde_json::Value;
use temporalstore::Client;

use crate::matrixark_rust_proxy_dispatch_hash;
use crate::matrixark_rust_proxy_dispatch_matrixark;
use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_retrieve::retrieve_context_pack_native;
use crate::matrixark_rust_proxy_runtime::connect;
use crate::matrixark_rust_proxy_scan::scan_matrixark_candidates;

pub(crate) fn run_with_client(client: &Client, command: Command) -> Result<Value, String> {
    match command.op.as_str() {
        "put_string" => matrixark_rust_proxy_dispatch_hash::put_string(client, command),
        "get_string" => matrixark_rust_proxy_dispatch_hash::get_string(client, command),
        "hset" => matrixark_rust_proxy_dispatch_hash::hset(client, command),
        "batch_hset" => matrixark_rust_proxy_dispatch_hash::batch_hset(client, &command),
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            matrixark_rust_proxy_dispatch_matrixark::append_records(client, &command)
        }
        "batch_hget" => matrixark_rust_proxy_dispatch_hash::batch_hget(client, &command),
        "hgetall" | "scan_hash" => matrixark_rust_proxy_dispatch_hash::scan_hash(client, command),
        "matrixark_scan_candidates" => scan_matrixark_candidates(client, &command),
        "matrixark_retrieve_context_pack" => retrieve_context_pack_native(client, &command),
        "write_matrixark_record" => {
            matrixark_rust_proxy_dispatch_matrixark::write_record(client, &command)
        }
        "write_matrixark_records" => {
            matrixark_rust_proxy_dispatch_matrixark::write_records(client, &command)
        }
        "read_matrixark_record" => {
            matrixark_rust_proxy_dispatch_matrixark::read_record(client, command)
        }
        "read_matrixark_records" => {
            matrixark_rust_proxy_dispatch_matrixark::read_records(client, command)
        }
        "hget" => matrixark_rust_proxy_dispatch_hash::hget(client, command),
        other => Err(format!("unsupported op {other}")),
    }
}

pub(crate) fn run(command: Command) -> Result<Value, String> {
    let client = connect(&command)?;
    run_with_client(&client, command)
}
