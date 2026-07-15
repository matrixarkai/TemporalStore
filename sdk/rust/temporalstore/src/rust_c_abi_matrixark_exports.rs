use std::os::raw::{c_char, c_int};

use crate::rust_c_abi_helpers::{client_from_handle, cstr_arg, run_c_abi, run_c_abi_string};
use crate::Error;

#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_matrixark_batch_append_records_json(
    handle: *mut std::ffi::c_void,
    entries_json: *const c_char,
    count_key: *const c_char,
    count_value: *const c_char,
    error: *mut *mut c_char,
) -> c_int {
    run_c_abi(error, || {
        let raw = cstr_arg(entries_json, "entries_json")?;
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&raw).map_err(|err| Error {
            code: 1,
            message: format!("invalid entries json: {err}"),
        })?;
        let entries_owned: Vec<(String, String, String)> = parsed
            .iter()
            .filter_map(|entry| {
                Some((
                    entry.get("key")?.as_str()?.to_string(),
                    entry.get("field")?.as_str()?.to_string(),
                    entry.get("value")?.as_str()?.to_string(),
                ))
            })
            .collect();
        let entries: Vec<(&str, &str, &str)> = entries_owned
            .iter()
            .map(|(key, field, value)| (key.as_str(), field.as_str(), value.as_str()))
            .collect();
        let count_key = if count_key.is_null() {
            String::new()
        } else {
            cstr_arg(count_key, "count_key")?
        };
        let count_value = if count_value.is_null() {
            String::new()
        } else {
            cstr_arg(count_value, "count_value")?
        };
        let count_key_opt = if count_key.is_empty() {
            None
        } else {
            Some(count_key.as_str())
        };
        let count_value_opt = if count_value.is_empty() {
            None
        } else {
            Some(count_value.as_str())
        };
        client_from_handle(handle)?.matrixark_batch_append_records(
            &entries,
            count_key_opt,
            count_value_opt,
        )
    })
}

#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_matrixark_scan_candidates_json(
    handle: *mut std::ffi::c_void,
    count_key: *const c_char,
    record_hash_key: *const c_char,
    shard_size: usize,
    request_json: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> c_int {
    run_c_abi_string(out, error, || {
        client_from_handle(handle)?.matrixark_scan_candidates(
            &cstr_arg(count_key, "count_key")?,
            &cstr_arg(record_hash_key, "record_hash_key")?,
            shard_size,
            &cstr_arg(request_json, "request_json")?,
        )
    })
}

#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_matrixark_retrieve_context_pack_json(
    handle: *mut std::ffi::c_void,
    count_key: *const c_char,
    record_hash_key: *const c_char,
    shard_size: usize,
    request_json: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> c_int {
    run_c_abi_string(out, error, || {
        let raw = client_from_handle(handle)?.matrixark_retrieve_context_pack(
            &cstr_arg(count_key, "count_key")?,
            &cstr_arg(record_hash_key, "record_hash_key")?,
            shard_size,
            &cstr_arg(request_json, "request_json")?,
        )?;
        let mut pack: serde_json::Value =
            serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::json!({}));
        if pack.get("context_pack").is_some() {
            if let Some(obj) = pack.as_object_mut() {
                obj.insert("ok".to_string(), serde_json::Value::Bool(true));
                obj.insert(
                    "native_pack_assembly".to_string(),
                    serde_json::Value::Bool(true),
                );
            }
            return Ok(pack.to_string());
        }
        Ok(serde_json::json!({
            "ok": true,
            "native_pack_assembly": true,
            "context_pack": pack
        })
        .to_string())
    })
}
