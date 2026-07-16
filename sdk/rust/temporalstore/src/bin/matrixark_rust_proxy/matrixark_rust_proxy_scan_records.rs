use std::sync::Arc;

use serde_json::Value;
use temporalstore::Client;

use crate::matrixark_rust_proxy_cache::{
    get_scan_record_cache, put_scan_record_cache, ScanRecordCacheEntry,
};

pub(crate) fn required(value: Option<String>, name: &str) -> Result<String, String> {
    value
        .filter(|item| !item.is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

pub(crate) fn serving_count_key(count_key: &str) -> String {
    format!("{count_key}:serving")
}

fn decode_matrixark_payload(value: &str) -> Vec<Value> {
    let Ok(decoded) = serde_json::from_str::<Value>(value) else {
        return Vec::new();
    };
    if let Some(bundle) = decoded.get("record_bundle").and_then(Value::as_array) {
        return bundle
            .iter()
            .filter(|item| item.is_object())
            .cloned()
            .collect();
    }
    if decoded.is_object() {
        vec![decoded]
    } else {
        Vec::new()
    }
}

pub(crate) fn load_scan_records(
    client: &Client,
    record_hash_key: &str,
    shard_size: u64,
    count: u64,
    cache_key: String,
) -> Result<(Arc<Vec<Value>>, u64, bool), String> {
    if let Some(entry) = get_scan_record_cache(&cache_key) {
        return Ok((entry.records, entry.scanned_records, true));
    }
    let max_shard = if count == 0 {
        0
    } else {
        (count - 1) / shard_size
    };
    let mut scanned_records = 0_u64;
    let mut records = Vec::new();
    for shard in 0..=max_shard {
        let key = format!("{}:{:06}", record_hash_key, shard);
        for (_field, value) in client.hgetall(&key).map_err(|err| err.to_string())? {
            for record in decode_matrixark_payload(&value) {
                scanned_records += 1;
                records.push(record);
            }
        }
    }
    let records_source = Arc::new(records);
    put_scan_record_cache(
        cache_key,
        ScanRecordCacheEntry {
            records: Arc::clone(&records_source),
            scanned_records,
        },
    );
    Ok((records_source, scanned_records, false))
}
