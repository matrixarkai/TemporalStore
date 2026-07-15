use serde_json::Value;

use crate::matrixark_rust_proxy_clock::unix_ms;

fn matrixark_event_ingestion_time_ms(record: &Value) -> u64 {
    for field in ["ingestion_time_ms", "updated_at_ms", "created_at_ms"] {
        if let Some(value) = record.get(field).and_then(Value::as_u64) {
            if value > 0 {
                return value;
            }
        }
    }
    record
        .get("envelope")
        .and_then(|value| value.get("ingestion_time_ms"))
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .unwrap_or_else(|| unix_ms() as u64)
}

pub(crate) fn matrixark_context_event_time_key(tenant_hash: u64) -> String {
    format!("matrixark:record:context_event_by_ingestion_time:{tenant_hash}")
}

pub(crate) fn matrixark_context_event_time_field(record: &Value, record_id: &str) -> String {
    format!(
        "{:020}:{}",
        matrixark_event_ingestion_time_ms(record),
        record_id
    )
}

pub(crate) fn matrixark_context_event_time_payload(record: &Value) -> Result<String, String> {
    let mut payload = record.clone();
    if let Some(object) = payload.as_object_mut() {
        object.remove("event_time_key");
        object.remove("ingestion_time_ms");
    }
    serde_json::to_string(&payload).map_err(|err| err.to_string())
}
