// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde_json::{json, Value};
use temporalstore::Client;

use crate::matrixark_rust_proxy_record_time_index::{
    matrixark_context_event_time_field, matrixark_context_event_time_key,
    matrixark_context_event_time_payload,
};

fn value_u64(record: &Value, field: &str) -> Option<u64> {
    record.get(field).and_then(Value::as_u64)
}

fn value_str(record: &Value, field: &str) -> Option<String> {
    record
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
}

pub(crate) fn matrixark_record_type(
    record: &Value,
    fallback: Option<&String>,
) -> Result<String, String> {
    value_str(record, "record_type")
        .or_else(|| fallback.cloned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "matrixark record missing record_type".to_string())
}

pub(crate) fn matrixark_tenant_hash(record: &Value, fallback: Option<u64>) -> Result<u64, String> {
    value_u64(record, "tenant_hash")
        .or(fallback)
        .ok_or_else(|| "matrixark record missing tenant_hash".to_string())
}

pub(crate) fn matrixark_record_id(
    record: &Value,
    fallback: Option<&String>,
) -> Result<String, String> {
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

pub(crate) fn matrixark_storage_key(record_type: &str, tenant_hash: u64) -> String {
    format!("matrixark:record:{record_type}:{tenant_hash}")
}

pub(crate) fn matrixark_storage_field(record_id: &str) -> String {
    record_id.to_string()
}

pub(crate) fn write_matrixark_record(
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
    let payload = serde_json::to_string(record).map_err(|err| err.to_string())?;
    let mut time_index: Option<Value> = None;
    if record_type == "context_event" {
        let time_key = matrixark_context_event_time_key(tenant_hash);
        let time_field = matrixark_context_event_time_field(record, &record_id);
        let time_payload = matrixark_context_event_time_payload(record)?;
        client
            .hset(&time_key, &time_field, &time_payload)
            .map_err(|err| err.to_string())?;
        time_index = Some(json!({"key": time_key, "field": time_field}));
    }
    client
        .hset(&key, &field, &payload)
        .map_err(|err| err.to_string())?;
    Ok(
        json!({"key": key, "field": field, "record_type": record_type, "record_id": record_id, "time_index": time_index}),
    )
}

pub(crate) fn read_matrixark_record(
    client: &Client,
    record_type: &str,
    tenant_hash: u64,
    record_id: &str,
) -> Result<Value, String> {
    let key = matrixark_storage_key(record_type, tenant_hash);
    let field = matrixark_storage_field(record_id);
    let value = client.hget(&key, &field).map_err(|err| err.to_string())?;
    Ok(json!({"key": key, "field": field, "record_id": record_id, "value": value}))
}
