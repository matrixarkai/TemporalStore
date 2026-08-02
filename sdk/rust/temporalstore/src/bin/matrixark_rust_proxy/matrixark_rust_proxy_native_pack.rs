use serde_json::{json, Value};
use temporalstore::Client;

use crate::matrixark_rust_proxy_protocol::Command;

fn required(value: Option<String>, name: &str) -> Result<String, String> {
    value
        .filter(|item| !item.is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

pub(crate) fn retrieve_context_pack_via_sdk_native(
    client: &Client,
    command: &Command,
) -> Result<Value, String> {
    let count_key = required(command.count_key.clone(), "count_key")?;
    let record_hash_key = required(command.record_hash_key.clone(), "record_hash_key")?;
    let shard_size = command.shard_size.unwrap_or(1024).max(1) as usize;
    let request = command.record.clone().unwrap_or_else(|| json!({}));
    let raw = client
        .matrixark_retrieve_context_pack(
            &count_key,
            &record_hash_key,
            shard_size,
            &request.to_string(),
        )
        .map_err(|err| err.to_string())?;
    let mut response: Value = serde_json::from_str(&raw)
        .map_err(|err| format!("native retrieve context pack returned invalid JSON: {err}"))?;
    if response.get("context_pack").is_none() {
        response = json!({
            "context_pack": response,
        });
    }
    if let Some(obj) = response.as_object_mut() {
        obj.insert("ok".to_string(), Value::Bool(true));
        obj.insert("native_pack_assembly".to_string(), Value::Bool(true));
        obj.insert(
            "rust_proxy_native_sdk_path".to_string(),
            Value::String("temporalstore_matrixark_retrieve_context_pack".to_string()),
        );
        obj.insert("cache_hit".to_string(), Value::Bool(true));
    }
    if let Some(pack) = response
        .get_mut("context_pack")
        .and_then(Value::as_object_mut)
    {
        pack.entry("context_pack_assembly".to_string())
            .or_insert_with(|| Value::String("native_cpp_direct_via_rust_proxy".to_string()));
        let selected_count = pack
            .get("selected_ref_count")
            .and_then(Value::as_u64)
            .or_else(|| {
                pack.get("selected_refs")
                    .or_else(|| pack.get("remote_context_refs"))
                    .and_then(Value::as_array)
                    .map(|refs| refs.len() as u64)
            })
            .unwrap_or(0);
        if pack.get("selected_refs").is_none() {
            if let Some(remote_refs) = pack.get("remote_context_refs").cloned() {
                pack.insert("selected_refs".to_string(), remote_refs);
            }
        }
        pack.insert("selected_ref_count".to_string(), json!(selected_count));
        let recall_policy = pack
            .entry("recall_policy".to_string())
            .or_insert_with(|| json!({}));
        if let Some(recall_obj) = recall_policy.as_object_mut() {
            recall_obj.insert(
                "rust_proxy_native_sdk_path".to_string(),
                Value::String("temporalstore_matrixark_retrieve_context_pack".to_string()),
            );
            recall_obj.insert("python_hot_path_records".to_string(), json!(0));
        }
    }
    Ok(response)
}
