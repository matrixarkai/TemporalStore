use std::fmt;

#[cfg(feature = "direct")]
use std::ffi::CString;
#[cfg(feature = "direct")]
use std::os::raw::{c_char, c_int};

#[derive(Debug)]
pub struct Error {
    pub code: i32,
    pub message: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

mod types;
pub use types::{
    ControlStateFolType, ControlStateHType, ControlStatePrecision, ControlStateWindow,
    ControlStateWindowUnit, FeatureFilter, FeatureFilterOp, FeaturePoint, FeatureWritePolicy,
    IpsFeatureStat, IpsInstance, IpsLastQuery, SequenceFeatureRow,
};

mod options;
pub use options::Options;

#[cfg(feature = "direct")]
mod direct_client;
#[cfg(feature = "direct")]
mod direct_ffi;
#[cfg(feature = "direct")]
mod direct_helpers;
#[cfg(feature = "direct")]
pub use direct_client::Client;
#[cfg(feature = "direct")]
pub(crate) use direct_ffi::*;

#[cfg(feature = "proxy")]
mod proxy_client;
#[cfg(feature = "proxy")]
mod proxy_helpers;
#[cfg(feature = "proxy")]
pub use proxy_client::{ProxyClient, ProxyOptions};

#[cfg(feature = "direct")]
mod rust_c_abi_helpers;
#[cfg(feature = "direct")]
use rust_c_abi_helpers::{
    client_from_handle, cstr_arg, options_from_json, run_c_abi, run_c_abi_string, set_error,
};

#[cfg(feature = "direct")]
#[no_mangle]
pub extern "C" fn temporalstore_rust_free_string(value: *mut c_char) {
    if !value.is_null() {
        unsafe {
            let _ = CString::from_raw(value);
        }
    }
}

#[cfg(feature = "direct")]
#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_connect_json(
    options_json: *const c_char,
    out: *mut *mut std::ffi::c_void,
    error: *mut *mut c_char,
) -> c_int {
    if out.is_null() {
        set_error(error, "out handle is null");
        return 1;
    }
    match (|| -> Result<Client> {
        let raw = cstr_arg(options_json, "options_json")?;
        Client::connect(options_from_json(&raw)?)
    })() {
        Ok(client) => {
            *out = Box::into_raw(Box::new(client)) as *mut std::ffi::c_void;
            0
        }
        Err(err) => {
            set_error(error, err.message);
            1
        }
    }
}

#[cfg(feature = "direct")]
#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_close(
    handle: *mut std::ffi::c_void,
    error: *mut *mut c_char,
) -> c_int {
    if handle.is_null() {
        return 0;
    }
    let mut client = Box::from_raw(handle as *mut Client);
    run_c_abi(error, || client.close())
}

#[cfg(feature = "direct")]
#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_hset(
    handle: *mut std::ffi::c_void,
    key: *const c_char,
    field: *const c_char,
    value: *const c_char,
    error: *mut *mut c_char,
) -> c_int {
    run_c_abi(error, || {
        client_from_handle(handle)?.hset(
            &cstr_arg(key, "key")?,
            &cstr_arg(field, "field")?,
            &cstr_arg(value, "value")?,
        )
    })
}

#[cfg(feature = "direct")]
#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_hget(
    handle: *mut std::ffi::c_void,
    key: *const c_char,
    field: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> c_int {
    run_c_abi_string(out, error, || {
        client_from_handle(handle)?.hget(&cstr_arg(key, "key")?, &cstr_arg(field, "field")?)
    })
}

#[cfg(feature = "direct")]
#[no_mangle]
pub unsafe extern "C" fn temporalstore_rust_hgetall_json(
    handle: *mut std::ffi::c_void,
    key: *const c_char,
    out: *mut *mut c_char,
    error: *mut *mut c_char,
) -> c_int {
    run_c_abi_string(out, error, || {
        let records: Vec<serde_json::Value> = client_from_handle(handle)?
            .hgetall(&cstr_arg(key, "key")?)?
            .into_iter()
            .map(|(field, value)| serde_json::json!({"field": field, "value": value}))
            .collect();
        Ok(serde_json::json!({"ok": true, "count": records.len(), "records": records}).to_string())
    })
}

#[cfg(feature = "direct")]
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

#[cfg(feature = "direct")]
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

#[cfg(feature = "direct")]
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

#[cfg(test)]
mod tests {
    #[cfg(feature = "direct")]
    use super::Client;
    #[cfg(feature = "proxy")]
    use super::ProxyClient;

    #[cfg(feature = "direct")]
    #[test]
    fn direct_client_exposes_cpp_c_abi_parity_methods() {
        let _: fn(&Client, &str, &str) -> super::Result<()> = Client::put_string;
        let _: fn(&Client, &str, &str, u64) -> super::Result<()> = Client::put_string_with_ttl;
        let _: fn(&Client, &str) -> super::Result<String> = Client::get_string;
        let _: fn(&Client, &str, &str, &str) -> super::Result<()> = Client::hset;
        let _: fn(&Client, &str, &str) -> super::Result<String> = Client::hget;
        let _: fn(&Client, &str) -> super::Result<Vec<(String, String)>> = Client::hgetall;
        let _: fn(&Client, &str) -> super::Result<Vec<(String, String)>> = Client::scan_hash;
        let _: fn(&Client, &str, &str) -> super::Result<()> = Client::hdel;
        let _: fn(&Client, &str) -> super::Result<()> = Client::delete_object;
        let _: fn(&Client, &str, u64) -> super::Result<()> = Client::expire;
        let _: fn(&Client, &str) -> super::Result<u64> = Client::ttl;
        let _: fn(&Client, &str, &str) -> super::Result<()> = Client::sadd;
        let _: fn(&Client, &str) -> super::Result<Vec<String>> = Client::smembers;
        let _: fn(&Client, &str, &[super::FeaturePoint]) -> super::Result<()> =
            Client::add_feature_points;
        let _: fn(&Client, &str, u64, u64, u64) -> super::Result<Vec<super::FeaturePoint>> =
            Client::query_feature_points;
        let _: fn(
            &Client,
            &str,
            u64,
            u64,
            u64,
            &[super::FeatureFilter],
        ) -> super::Result<Vec<super::FeaturePoint>> = Client::query_feature_points_filtered;
        let _: fn(&Client, &super::IpsInstance) -> super::Result<()> = Client::add_ips_instance;
        let _: fn(&Client, &super::IpsLastQuery) -> super::Result<Vec<super::IpsFeatureStat>> =
            Client::query_ips_last_instances;
        let _: fn(
            &Client,
            &str,
            i64,
            u64,
            super::ControlStatePrecision,
            &str,
            u64,
        ) -> super::Result<()> = Client::control_state_increment;
        let _: fn(
            &Client,
            &str,
            super::ControlStatePrecision,
            super::ControlStateWindow,
        ) -> super::Result<i64> = Client::control_state_count;
        let _: fn(&Client, &[(&str, &str, &str)], Option<&str>, Option<&str>) -> super::Result<()> =
            Client::matrixark_batch_append_records;
    }

    #[cfg(feature = "proxy")]
    #[test]
    fn proxy_client_exposes_cpp_proxy_parity_methods() {
        let _: fn(&ProxyClient, &str, &[super::FeaturePoint]) -> super::Result<()> =
            ProxyClient::feature_add;
        let _: fn(
            &ProxyClient,
            &str,
            u64,
            u64,
            Option<usize>,
        ) -> super::Result<Vec<super::FeaturePoint>> = ProxyClient::feature_query;
        let _: fn(
            &ProxyClient,
            &str,
            u64,
            u64,
            Option<usize>,
            &[super::FeatureFilter],
        ) -> super::Result<Vec<super::FeaturePoint>> = ProxyClient::feature_query_filtered;
        let _: fn(&ProxyClient, &super::IpsInstance) -> super::Result<()> =
            ProxyClient::add_ips_instance;
        let _: fn(&ProxyClient, &super::IpsLastQuery) -> super::Result<Vec<super::IpsFeatureStat>> =
            ProxyClient::query_ips_last_instances;
        let _: fn(
            &ProxyClient,
            &str,
            i64,
            u64,
            super::ControlStatePrecision,
            &str,
            u64,
        ) -> super::Result<()> = ProxyClient::control_state_increment;
        let _: fn(
            &ProxyClient,
            &str,
            super::ControlStatePrecision,
            super::ControlStateWindow,
        ) -> super::Result<i64> = ProxyClient::control_state_count;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<()> = ProxyClient::set;
        let _: fn(&ProxyClient, &str) -> super::Result<Option<String>> = ProxyClient::get;
        let _: fn(&ProxyClient, &str, u64, i64) -> super::Result<()> =
            ProxyClient::control_state_hset;
        let _: fn(
            &ProxyClient,
            &str,
            &str,
            u64,
            super::ControlStateHType,
            u64,
            super::ControlStatePrecision,
        ) -> super::Result<()> = ProxyClient::control_state_hset_with_options;
        let _: fn(
            &ProxyClient,
            &str,
            super::ControlStatePrecision,
            super::ControlStateWindow,
            &str,
        ) -> super::Result<Vec<i64>> = ProxyClient::control_state_hquery;
        let _: fn(
            &ProxyClient,
            &str,
            &[&str],
            u64,
            u64,
            super::ControlStatePrecision,
            bool,
        ) -> super::Result<()> = ProxyClient::control_state_cpc_set;
        let _: fn(
            &ProxyClient,
            &str,
            super::ControlStatePrecision,
            super::ControlStateWindow,
        ) -> super::Result<Vec<i64>> = ProxyClient::control_state_cpc_query;
        let _: fn(
            &ProxyClient,
            &str,
            &str,
            u64,
            u64,
            super::ControlStateFolType,
        ) -> super::Result<()> = ProxyClient::control_state_fol_set;
        let _: fn(&ProxyClient, &str) -> super::Result<Option<String>> =
            ProxyClient::control_state_fol_query;
        let _: fn(&ProxyClient, &str) -> super::Result<Vec<(String, String)>> =
            ProxyClient::control_state_manager;
        let _: fn(
            &ProxyClient,
            &str,
            &str,
            &[(&str, &str)],
            &str,
            &str,
            bool,
        ) -> super::Result<Vec<(String, String)>> = ProxyClient::control_state_manager_with_options;
        let _: fn(&ProxyClient, &str, &str, &str) -> super::Result<()> = ProxyClient::hset;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<String> = ProxyClient::hget;
        let _: fn(&ProxyClient, &str, &[(&str, &str)]) -> super::Result<()> = ProxyClient::hmset;
        let _: fn(&ProxyClient, &str, &[&str]) -> super::Result<Vec<Option<String>>> =
            ProxyClient::hmget;
        let _: fn(&ProxyClient, &str) -> super::Result<Vec<(String, String)>> =
            ProxyClient::hgetall;
        let _: fn(&ProxyClient, &str) -> super::Result<u64> = ProxyClient::hlen;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<()> = ProxyClient::hdel;
        let _: fn(&ProxyClient, &str) -> super::Result<()> = ProxyClient::delete_object;
        let _: fn(&ProxyClient, &str, u64) -> super::Result<()> = ProxyClient::expire;
        let _: fn(&ProxyClient, &str) -> super::Result<u64> = ProxyClient::ttl;
        let _: fn(&ProxyClient, &str, &str) -> super::Result<()> = ProxyClient::sadd;
        let _: fn(&ProxyClient, &str) -> super::Result<Vec<String>> = ProxyClient::smembers;
    }
}
