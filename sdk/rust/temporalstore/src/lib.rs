// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fmt;

#[cfg(feature = "direct")]
use std::ffi::{CStr, CString};
#[cfg(feature = "proxy")]
use std::io::{Read, Write};
#[cfg(feature = "proxy")]
use std::net::TcpStream;
#[cfg(feature = "direct")]
use std::os::raw::{c_char, c_int};
#[cfg(feature = "direct")]
use std::ptr;

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

#[cfg(feature = "direct")]
fn native_matrixark_c_api_bridge_allowed(op: &str) -> Result<()> {
    let allowed = std::env::var("TEMPORALSTORE_RUST_ALLOW_NATIVE_MATRIXARK_C_API")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    if allowed {
        return Ok(());
    }
    Err(Error {
        code: 1,
        message: format!(
            "Rust MatrixArk hot path {op} would call the shared C API bridge. \
             Use the Rust-native temporalstore-rust matrixark_rust_proxy/direct SDK path, \
             or set TEMPORALSTORE_RUST_ALLOW_NATIVE_MATRIXARK_C_API=1 only for compatibility diagnostics."
        ),
    })
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum FeatureFilterOp {
    Equal = 0,
    NotEqual = 1,
    GreaterThan = 2,
    LessThan = 3,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum FeatureWritePolicy {
    Upsert = 0,
    Block = 1,
    First = 2,
    Update = 3,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct FeatureFilter {
    pub field: String,
    pub op: FeatureFilterOp,
    pub value: u64,
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct FeaturePoint {
    pub timestamp_ms: u64,
    pub value: Vec<u8>,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum RiskPrecision {
    OneSecond = 0,
    FiveSeconds = 1,
    TenSeconds = 2,
    OneMinute = 3,
    FiveMinutes = 4,
    TenMinutes = 5,
    OneHour = 6,
    OneDay = 7,
    OneMonth = 8,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum RiskWindowUnit {
    Second = 0,
    Minute = 1,
    Hour = 2,
    Day = 3,
}

#[derive(Clone, Copy, Debug)]
pub struct RiskWindow {
    pub start: i64,
    pub end: i64,
    pub unit: RiskWindowUnit,
}

impl Default for RiskWindow {
    fn default() -> Self {
        Self {
            start: -1,
            end: 0,
            unit: RiskWindowUnit::Hour,
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "proxy", serde(rename_all = "snake_case"))]
pub enum RiskFolType {
    First,
    Last,
}

#[derive(Clone, Copy, Debug)]
#[cfg_attr(feature = "proxy", derive(serde::Serialize, serde::Deserialize))]
pub struct SequenceFeatureRow {
    // The engine's row names this `timestamp_ms` and requires it. The Rust name stays as it was
    // so callers do not change; only the wire name is corrected.
    #[cfg_attr(feature = "proxy", serde(rename = "timestamp_ms"))]
    pub timestamp: u64,
    pub gid: u64,
    pub action_type: u32,
    pub duration: u32,
    pub author_id: u64,
}

#[cfg(feature = "direct")]
#[repr(C)]
struct TemporalStoreClientOpaque {
    _private: [u8; 0],
}

#[cfg(feature = "direct")]
#[repr(C)]
struct COptions {
    metaserver_addr: *const c_char,
    metaserver_consul: *const c_char,
    namespace_name: *const c_char,
    table_name: *const c_char,
    idc: *const c_char,
    host: *const c_char,
    psm: *const c_char,
    log_dir: *const c_char,
    log_level: c_int,
    io_timeout_ms: c_int,
    connect_timeout_ms: c_int,
    request_timeout_ms: c_int,
    max_read_retries: c_int,
    max_write_retries: c_int,
    retry_backoff_ms: c_int,
    max_feature_points_per_request: c_int,
    max_feature_query_count: u64,
    max_key_bytes: u64,
    max_value_bytes: u64,
    pin_primary: c_int,
}

#[cfg(feature = "direct")]
#[repr(C)]
#[derive(Clone, Copy)]
struct CFeatureFilter {
    field: *const c_char,
    op: c_int,
    value: u64,
}

#[cfg(feature = "direct")]
#[repr(C)]
#[derive(Clone, Copy)]
struct CFeaturePoint {
    timestamp: u64,
    value: *const c_char,
}

#[cfg(feature = "direct")]
#[repr(C)]
#[derive(Clone, Copy)]
struct CSequenceFeatureRow {
    timestamp: u64,
    gid: u64,
    action_type: u32,
    duration: u32,
    author_id: u64,
}

#[cfg(feature = "direct")]
#[repr(C)]
#[derive(Clone, Copy)]
struct CHashEntry {
    key: *const c_char,
    field: *const c_char,
    value: *const c_char,
    route_json: *const c_char,
}

#[cfg(feature = "direct")]
#[repr(C)]
struct CStringArray {
    count: usize,
    values: *mut *mut c_char,
}

#[cfg(feature = "direct")]
#[repr(C)]
struct CHashEntryArray {
    count: usize,
    entries: *mut CHashEntry,
}

#[cfg(feature = "direct")]
#[repr(C)]
struct CFeaturePointArray {
    count: usize,
    points: *mut CFeaturePoint,
}

#[cfg(feature = "direct")]
#[repr(C)]
struct CSequenceFeatureRowArray {
    count: usize,
    rows: *mut CSequenceFeatureRow,
}

#[cfg(feature = "direct")]
extern "C" {
    fn temporalstore_options_init(options: *mut COptions);
    fn temporalstore_free_string(value: *mut c_char);
    fn temporalstore_connect(
        options: *const COptions,
        client: *mut *mut TemporalStoreClientOpaque,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_close(
        client: *mut TemporalStoreClientOpaque,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_put_string(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_put_string_with_ttl(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *const c_char,
        ttl_ms: u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_get_string(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_delete_object(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_expire(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        ttl_ms: u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_ttl(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        ttl_ms: *mut u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_hset(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_hget(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        value: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_hgetall(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        entries: *mut CHashEntryArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_hdel(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_sadd(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        member: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_smembers(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        members: *mut CStringArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_string_array_free(array: *mut CStringArray);
    fn temporalstore_hash_entry_array_free(array: *mut CHashEntryArray);
    fn temporalstore_matrixark_batch_append_records(
        client: *mut TemporalStoreClientOpaque,
        entries: *const CHashEntry,
        entry_count: usize,
        count_key: *const c_char,
        count_value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_matrixark_batch_append_records_v2(
        client: *mut TemporalStoreClientOpaque,
        entries: *const CHashEntry,
        entry_count: usize,
        count_key: *const c_char,
        count_value: *const c_char,
        append_options_json: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_matrixark_scan_candidates(
        client: *mut TemporalStoreClientOpaque,
        count_key: *const c_char,
        record_hash_key: *const c_char,
        shard_size: usize,
        request_json: *const c_char,
        candidates_json: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_matrixark_retrieve_context_pack(
        client: *mut TemporalStoreClientOpaque,
        count_key: *const c_char,
        record_hash_key: *const c_char,
        shard_size: usize,
        request_json: *const c_char,
        context_pack_json: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_add_feature_points_with_policy(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        points: *const CFeaturePoint,
        count: usize,
        policy: c_int,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_query_feature_points(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        points: *mut CFeaturePointArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_query_feature_points_with_filters(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: *const CFeatureFilter,
        filter_count: usize,
        points: *mut CFeaturePointArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_feature_point_array_free(points: *mut CFeaturePointArray);
    fn temporalstore_add_sequence_feature_rows_with_policy(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        rows: *const CSequenceFeatureRow,
        count: usize,
        policy: c_int,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_query_sequence_feature_rows(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: *const CFeatureFilter,
        filter_count: usize,
        rows: *mut CSequenceFeatureRowArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_sequence_feature_row_array_free(rows: *mut CSequenceFeatureRowArray);
    fn temporalstore_risk_increment(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        amount: i64,
        ttl_seconds: u64,
        precision: i32,
        uuid: *const c_char,
        occur_time_seconds: u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    fn temporalstore_risk_count(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        precision: i32,
        window_start: i64,
        window_end: i64,
        window_unit: i32,
        count: *mut i64,
        error_message: *mut *mut c_char,
    ) -> c_int;
}

#[cfg(feature = "direct")]
#[derive(Clone, Debug)]
pub struct Options {
    pub metaserver_addr: String,
    pub namespace_name: String,
    pub table_name: String,
    pub metaserver_consul: String,
    pub idc: String,
    pub host: String,
    pub psm: String,
    pub log_dir: String,
    pub log_level: i32,
    pub io_timeout_ms: i32,
    pub connect_timeout_ms: i32,
    pub request_timeout_ms: i32,
    pub max_read_retries: i32,
    pub max_write_retries: i32,
    pub retry_backoff_ms: i32,
    pub max_feature_points_per_request: i32,
    pub max_feature_query_count: u64,
    pub max_key_bytes: u64,
    pub max_value_bytes: u64,
    pub pin_primary: bool,
}

#[cfg(feature = "direct")]
impl Options {
    pub fn new(
        metaserver_addr: impl Into<String>,
        namespace_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            metaserver_addr: metaserver_addr.into(),
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            metaserver_consul: String::new(),
            idc: "vdc1".to_string(),
            host: "127.0.0.1".to_string(),
            psm: "temporalstore.rust.client".to_string(),
            log_dir: "./".to_string(),
            log_level: 3,
            io_timeout_ms: 1000,
            connect_timeout_ms: 1000,
            request_timeout_ms: 5000,
            max_read_retries: 1,
            max_write_retries: 0,
            retry_backoff_ms: 2,
            max_feature_points_per_request: 1000,
            max_feature_query_count: 5000,
            max_key_bytes: 4096,
            max_value_bytes: 16 * 1024 * 1024,
            pin_primary: true,
        }
    }
}

#[cfg(feature = "direct")]
pub struct Client {
    raw: *mut TemporalStoreClientOpaque,
}

#[cfg(feature = "direct")]
unsafe impl Send for Client {}

#[cfg(feature = "direct")]
impl Client {
    pub fn connect(options: Options) -> Result<Self> {
        let strings = CStringOptions::new(&options)?;
        let mut c_options = unsafe {
            let mut value = std::mem::MaybeUninit::<COptions>::zeroed().assume_init();
            temporalstore_options_init(&mut value);
            value
        };
        strings.apply(&mut c_options, &options);

        let mut raw: *mut TemporalStoreClientOpaque = ptr::null_mut();
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_connect(&c_options, &mut raw, &mut error) };
        check(code, error)?;
        Ok(Self { raw })
    }

    pub fn close(&mut self) -> Result<()> {
        if self.raw.is_null() {
            return Ok(());
        }
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_close(self.raw, &mut error) };
        self.raw = ptr::null_mut();
        check(code, error)
    }

    pub fn put_string(&self, key: &str, value: &str) -> Result<()> {
        let key = cstring(key)?;
        let value = cstring(value)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code =
            unsafe { temporalstore_put_string(self.raw, key.as_ptr(), value.as_ptr(), &mut error) };
        check(code, error)
    }

    pub fn put_string_with_ttl(&self, key: &str, value: &str, ttl_ms: u64) -> Result<()> {
        let key = cstring(key)?;
        let value = cstring(value)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_put_string_with_ttl(
                self.raw,
                key.as_ptr(),
                value.as_ptr(),
                ttl_ms,
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn get_string(&self, key: &str) -> Result<String> {
        let key = cstring(key)?;
        let mut value: *mut c_char = ptr::null_mut();
        let mut error: *mut c_char = ptr::null_mut();
        let code =
            unsafe { temporalstore_get_string(self.raw, key.as_ptr(), &mut value, &mut error) };
        check(code, error)?;
        let out = unsafe { CStr::from_ptr(value).to_string_lossy().into_owned() };
        unsafe { temporalstore_free_string(value) };
        Ok(out)
    }

    pub fn delete_object(&self, key: &str) -> Result<()> {
        let key = cstring(key)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_delete_object(self.raw, key.as_ptr(), &mut error) };
        check(code, error)
    }

    pub fn expire(&self, key: &str, ttl_ms: u64) -> Result<()> {
        let key = cstring(key)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_expire(self.raw, key.as_ptr(), ttl_ms, &mut error) };
        check(code, error)
    }

    pub fn ttl(&self, key: &str) -> Result<u64> {
        let key = cstring(key)?;
        let mut ttl_ms = 0_u64;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_ttl(self.raw, key.as_ptr(), &mut ttl_ms, &mut error) };
        check(code, error)?;
        Ok(ttl_ms)
    }

    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<()> {
        let key = cstring(key)?;
        let field = cstring(field)?;
        let value = cstring(value)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_hset(
                self.raw,
                key.as_ptr(),
                field.as_ptr(),
                value.as_ptr(),
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<String> {
        let key = cstring(key)?;
        let field = cstring(field)?;
        let mut value: *mut c_char = ptr::null_mut();
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_hget(
                self.raw,
                key.as_ptr(),
                field.as_ptr(),
                &mut value,
                &mut error,
            )
        };
        check(code, error)?;
        let out = unsafe { CStr::from_ptr(value).to_string_lossy().into_owned() };
        unsafe { temporalstore_free_string(value) };
        Ok(out)
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>> {
        let key = cstring(key)?;
        let mut out = CHashEntryArray {
            count: 0,
            entries: ptr::null_mut(),
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_hgetall(self.raw, key.as_ptr(), &mut out, &mut error) };
        check(code, error)?;
        let entries = if out.entries.is_null() {
            Vec::new()
        } else {
            let slice = unsafe { std::slice::from_raw_parts(out.entries, out.count) };
            slice
                .iter()
                .map(|entry| {
                    let field = if entry.field.is_null() {
                        String::new()
                    } else {
                        unsafe { CStr::from_ptr(entry.field).to_string_lossy().into_owned() }
                    };
                    let value = if entry.value.is_null() {
                        String::new()
                    } else {
                        unsafe { CStr::from_ptr(entry.value).to_string_lossy().into_owned() }
                    };
                    (field, value)
                })
                .collect()
        };
        unsafe { temporalstore_hash_entry_array_free(&mut out) };
        Ok(entries)
    }

    pub fn scan_hash(&self, key: &str) -> Result<Vec<(String, String)>> {
        self.hgetall(key)
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<()> {
        let key = cstring(key)?;
        let field = cstring(field)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code =
            unsafe { temporalstore_hdel(self.raw, key.as_ptr(), field.as_ptr(), &mut error) };
        check(code, error)
    }

    pub fn sadd(&self, key: &str, member: &str) -> Result<()> {
        let key = cstring(key)?;
        let member = cstring(member)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code =
            unsafe { temporalstore_sadd(self.raw, key.as_ptr(), member.as_ptr(), &mut error) };
        check(code, error)
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>> {
        let key = cstring(key)?;
        let mut out = CStringArray {
            count: 0,
            values: ptr::null_mut(),
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe { temporalstore_smembers(self.raw, key.as_ptr(), &mut out, &mut error) };
        check(code, error)?;
        let values = if out.values.is_null() {
            Vec::new()
        } else {
            let slice = unsafe { std::slice::from_raw_parts(out.values, out.count) };
            slice
                .iter()
                .filter_map(|value| {
                    if value.is_null() {
                        None
                    } else {
                        Some(unsafe { CStr::from_ptr(*value).to_string_lossy().into_owned() })
                    }
                })
                .collect()
        };
        unsafe { temporalstore_string_array_free(&mut out) };
        Ok(values)
    }

    pub fn matrixark_batch_append_records(
        &self,
        entries: &[(&str, &str, &str)],
        count_key: Option<&str>,
        count_value: Option<&str>,
    ) -> Result<()> {
        self.matrixark_batch_append_records_with_options(entries, count_key, count_value, "{}")
    }

    pub fn matrixark_batch_append_records_with_options(
        &self,
        entries: &[(&str, &str, &str)],
        count_key: Option<&str>,
        count_value: Option<&str>,
        append_options_json: &str,
    ) -> Result<()> {
        let routed_entries: Vec<(&str, &str, &str, &str)> = entries
            .iter()
            .map(|(key, field, value)| (*key, *field, *value, "{}"))
            .collect();
        self.matrixark_batch_append_records_with_routes_and_options(
            &routed_entries,
            count_key,
            count_value,
            append_options_json,
        )
    }

    pub fn matrixark_batch_append_records_with_routes_and_options(
        &self,
        entries: &[(&str, &str, &str, &str)],
        count_key: Option<&str>,
        count_value: Option<&str>,
        append_options_json: &str,
    ) -> Result<()> {
        if entries.is_empty() && count_key.unwrap_or("").is_empty() {
            return Ok(());
        }
        native_matrixark_c_api_bridge_allowed("matrixark_batch_append_records")?;
        let mut strings: Vec<CString> = Vec::with_capacity(entries.len() * 4 + 3);
        let mut c_entries: Vec<CHashEntry> = Vec::with_capacity(entries.len());
        for (key, field, value, route_json) in entries {
            let key_index = strings.len();
            strings.push(cstring(key)?);
            let field_index = strings.len();
            strings.push(cstring(field)?);
            let value_index = strings.len();
            strings.push(cstring(value)?);
            let route_index = strings.len();
            strings.push(cstring(route_json)?);
            c_entries.push(CHashEntry {
                key: strings[key_index].as_ptr(),
                field: strings[field_index].as_ptr(),
                value: strings[value_index].as_ptr(),
                route_json: strings[route_index].as_ptr(),
            });
        }
        let count_key_c = match count_key {
            Some(value) if !value.is_empty() => Some(cstring(value)?),
            _ => None,
        };
        let count_value_c = match count_value {
            Some(value) => Some(cstring(value)?),
            None => None,
        };
        let append_options_c = cstring(append_options_json)?;
        let mut error: *mut c_char = ptr::null_mut();
        let entries_ptr = if c_entries.is_empty() {
            ptr::null()
        } else {
            c_entries.as_ptr()
        };
        let code = unsafe {
            temporalstore_matrixark_batch_append_records_v2(
                self.raw,
                entries_ptr,
                c_entries.len(),
                count_key_c
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                count_value_c
                    .as_ref()
                    .map_or(ptr::null(), |value| value.as_ptr()),
                append_options_c.as_ptr(),
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn matrixark_scan_candidates(
        &self,
        count_key: &str,
        record_hash_key: &str,
        shard_size: usize,
        request_json: &str,
    ) -> Result<String> {
        native_matrixark_c_api_bridge_allowed("matrixark_scan_candidates")?;
        let count_key = cstring(count_key)?;
        let record_hash_key = cstring(record_hash_key)?;
        let request_json = cstring(request_json)?;
        let mut value: *mut c_char = ptr::null_mut();
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_matrixark_scan_candidates(
                self.raw,
                count_key.as_ptr(),
                record_hash_key.as_ptr(),
                shard_size,
                request_json.as_ptr(),
                &mut value,
                &mut error,
            )
        };
        check(code, error)?;
        let out = unsafe { CStr::from_ptr(value).to_string_lossy().into_owned() };
        unsafe { temporalstore_free_string(value) };
        Ok(out)
    }

    pub fn matrixark_retrieve_context_pack(
        &self,
        count_key: &str,
        record_hash_key: &str,
        shard_size: usize,
        request_json: &str,
    ) -> Result<String> {
        native_matrixark_c_api_bridge_allowed("matrixark_retrieve_context_pack")?;
        let count_key = cstring(count_key)?;
        let record_hash_key = cstring(record_hash_key)?;
        let request_json = cstring(request_json)?;
        let mut value: *mut c_char = ptr::null_mut();
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_matrixark_retrieve_context_pack(
                self.raw,
                count_key.as_ptr(),
                record_hash_key.as_ptr(),
                shard_size,
                request_json.as_ptr(),
                &mut value,
                &mut error,
            )
        };
        check(code, error)?;
        let out = unsafe { CStr::from_ptr(value).to_string_lossy().into_owned() };
        unsafe { temporalstore_free_string(value) };
        Ok(out)
    }

    pub fn add_feature_points(&self, key: &str, points: &[FeaturePoint]) -> Result<()> {
        self.add_feature_points_with_policy(key, points, FeatureWritePolicy::Upsert)
    }

    pub fn add_feature_points_with_policy(
        &self,
        key: &str,
        points: &[FeaturePoint],
        policy: FeatureWritePolicy,
    ) -> Result<()> {
        let key = cstring(key)?;
        let values = points
            .iter()
            .map(|point| cstring(&String::from_utf8_lossy(&point.value)))
            .collect::<Result<Vec<_>>>()?;
        let c_points = points
            .iter()
            .zip(values.iter())
            .map(|(point, value)| CFeaturePoint {
                timestamp: point.timestamp_ms,
                value: value.as_ptr(),
            })
            .collect::<Vec<_>>();
        let mut error: *mut c_char = ptr::null_mut();
        let points_ptr = if c_points.is_empty() {
            ptr::null()
        } else {
            c_points.as_ptr()
        };
        let code = unsafe {
            temporalstore_add_feature_points_with_policy(
                self.raw,
                key.as_ptr(),
                points_ptr,
                c_points.len(),
                policy as c_int,
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn query_feature_points(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
    ) -> Result<Vec<FeaturePoint>> {
        let key = cstring(key)?;
        let mut out = CFeaturePointArray {
            count: 0,
            points: ptr::null_mut(),
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_query_feature_points(
                self.raw,
                key.as_ptr(),
                start_ts,
                end_ts,
                count,
                &mut out,
                &mut error,
            )
        };
        check(code, error)?;
        let points = feature_points_from_c_array(&out);
        unsafe { temporalstore_feature_point_array_free(&mut out) };
        Ok(points)
    }

    pub fn query_feature_points_filtered(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<FeaturePoint>> {
        let key = cstring(key)?;
        let c_fields: Vec<CString> = filters
            .iter()
            .map(|filter| cstring(&filter.field))
            .collect::<Result<Vec<_>>>()?;
        let c_filters: Vec<CFeatureFilter> = filters
            .iter()
            .zip(c_fields.iter())
            .map(|(filter, field)| CFeatureFilter {
                field: field.as_ptr(),
                op: filter.op as c_int,
                value: filter.value,
            })
            .collect();
        let mut out = CFeaturePointArray {
            count: 0,
            points: ptr::null_mut(),
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_query_feature_points_with_filters(
                self.raw,
                key.as_ptr(),
                start_ts,
                end_ts,
                count,
                c_filters.as_ptr(),
                c_filters.len(),
                &mut out,
                &mut error,
            )
        };
        check(code, error)?;
        let points = feature_points_from_c_array(&out);
        unsafe { temporalstore_feature_point_array_free(&mut out) };
        Ok(points)
    }

    pub fn add_sequence_feature_rows(&self, key: &str, rows: &[SequenceFeatureRow]) -> Result<()> {
        self.add_sequence_feature_rows_with_policy(key, rows, FeatureWritePolicy::Upsert)
    }

    pub fn add_sequence_feature_rows_with_policy(
        &self,
        key: &str,
        rows: &[SequenceFeatureRow],
        policy: FeatureWritePolicy,
    ) -> Result<()> {
        let key = cstring(key)?;
        let c_rows: Vec<CSequenceFeatureRow> = rows
            .iter()
            .map(|row| CSequenceFeatureRow {
                timestamp: row.timestamp,
                gid: row.gid,
                action_type: row.action_type,
                duration: row.duration,
                author_id: row.author_id,
            })
            .collect();
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_add_sequence_feature_rows_with_policy(
                self.raw,
                key.as_ptr(),
                c_rows.as_ptr(),
                c_rows.len(),
                policy as c_int,
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn query_sequence_feature_rows(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<SequenceFeatureRow>> {
        let key = cstring(key)?;
        let c_fields: Vec<CString> = filters
            .iter()
            .map(|filter| cstring(&filter.field))
            .collect::<Result<Vec<_>>>()?;
        let c_filters: Vec<CFeatureFilter> = filters
            .iter()
            .zip(c_fields.iter())
            .map(|(filter, field)| CFeatureFilter {
                field: field.as_ptr(),
                op: filter.op as c_int,
                value: filter.value,
            })
            .collect();
        let mut out = CSequenceFeatureRowArray {
            count: 0,
            rows: ptr::null_mut(),
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_query_sequence_feature_rows(
                self.raw,
                key.as_ptr(),
                start_ts,
                end_ts,
                count,
                c_filters.as_ptr(),
                c_filters.len(),
                &mut out,
                &mut error,
            )
        };
        check(code, error)?;
        let slice = if out.rows.is_null() {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(out.rows, out.count) }
        };
        let rows = slice
            .iter()
            .map(|row| SequenceFeatureRow {
                timestamp: row.timestamp,
                gid: row.gid,
                action_type: row.action_type,
                duration: row.duration,
                author_id: row.author_id,
            })
            .collect();
        unsafe { temporalstore_sequence_feature_row_array_free(&mut out) };
        Ok(rows)
    }

    pub fn risk_increment(
        &self,
        key: &str,
        amount: i64,
        ttl_seconds: u64,
        precision: RiskPrecision,
        uuid: &str,
        occur_time_seconds: u64,
    ) -> Result<()> {
        let key = cstring(key)?;
        let uuid = cstring(uuid)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_risk_increment(
                self.raw,
                key.as_ptr(),
                amount,
                ttl_seconds,
                precision as i32,
                uuid.as_ptr(),
                occur_time_seconds,
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn risk_count(
        &self,
        key: &str,
        precision: RiskPrecision,
        window: RiskWindow,
    ) -> Result<i64> {
        let key = cstring(key)?;
        let mut count = 0_i64;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_risk_count(
                self.raw,
                key.as_ptr(),
                precision as i32,
                window.start,
                window.end,
                window.unit as i32,
                &mut count,
                &mut error,
            )
        };
        check(code, error)?;
        Ok(count)
    }
}

#[cfg(feature = "direct")]
fn feature_points_from_c_array(out: &CFeaturePointArray) -> Vec<FeaturePoint> {
    if out.points.is_null() {
        return Vec::new();
    }
    let slice = unsafe { std::slice::from_raw_parts(out.points, out.count) };
    slice
        .iter()
        .map(|point| {
            let value = if point.value.is_null() {
                Vec::new()
            } else {
                unsafe { CStr::from_ptr(point.value).to_bytes().to_vec() }
            };
            FeaturePoint {
                timestamp_ms: point.timestamp,
                value,
            }
        })
        .collect()
}

#[cfg(feature = "direct")]
impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(feature = "direct")]
struct CStringOptions {
    metaserver_addr: CString,
    metaserver_consul: CString,
    namespace_name: CString,
    table_name: CString,
    idc: CString,
    host: CString,
    psm: CString,
    log_dir: CString,
}

#[cfg(feature = "direct")]
impl CStringOptions {
    fn new(options: &Options) -> Result<Self> {
        Ok(Self {
            metaserver_addr: cstring(&options.metaserver_addr)?,
            metaserver_consul: cstring(&options.metaserver_consul)?,
            namespace_name: cstring(&options.namespace_name)?,
            table_name: cstring(&options.table_name)?,
            idc: cstring(&options.idc)?,
            host: cstring(&options.host)?,
            psm: cstring(&options.psm)?,
            log_dir: cstring(&options.log_dir)?,
        })
    }

    fn apply(&self, c_options: &mut COptions, options: &Options) {
        c_options.metaserver_addr = self.metaserver_addr.as_ptr();
        c_options.metaserver_consul = self.metaserver_consul.as_ptr();
        c_options.namespace_name = self.namespace_name.as_ptr();
        c_options.table_name = self.table_name.as_ptr();
        c_options.idc = self.idc.as_ptr();
        c_options.host = self.host.as_ptr();
        c_options.psm = self.psm.as_ptr();
        c_options.log_dir = self.log_dir.as_ptr();
        c_options.log_level = options.log_level;
        c_options.io_timeout_ms = options.io_timeout_ms;
        c_options.connect_timeout_ms = options.connect_timeout_ms;
        c_options.request_timeout_ms = options.request_timeout_ms;
        c_options.max_read_retries = options.max_read_retries;
        c_options.max_write_retries = options.max_write_retries;
        c_options.retry_backoff_ms = options.retry_backoff_ms;
        c_options.max_feature_points_per_request = options.max_feature_points_per_request;
        c_options.max_feature_query_count = options.max_feature_query_count;
        c_options.max_key_bytes = options.max_key_bytes;
        c_options.max_value_bytes = options.max_value_bytes;
        c_options.pin_primary = if options.pin_primary { 1 } else { 0 };
    }
}

#[cfg(feature = "direct")]
fn cstring(value: &str) -> Result<CString> {
    CString::new(value).map_err(|_| Error {
        code: 3,
        message: "string contains interior null byte".to_string(),
    })
}

#[cfg(feature = "direct")]
fn check(code: c_int, error: *mut c_char) -> Result<()> {
    if code == 0 {
        return Ok(());
    }
    let message = if error.is_null() {
        "unknown TemporalStore error".to_string()
    } else {
        let message = unsafe { CStr::from_ptr(error).to_string_lossy().into_owned() };
        unsafe { temporalstore_free_string(error) };
        message
    };
    Err(Error { code, message })
}

#[cfg(feature = "proxy")]
#[derive(Clone, Debug)]
pub struct ProxyOptions {
    pub endpoint: String,
    pub namespace_name: String,
    pub table_name: String,
    pub api_key: String,
}

#[cfg(feature = "proxy")]
impl ProxyOptions {
    pub fn new(
        endpoint: impl Into<String>,
        namespace_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            namespace_name: namespace_name.into(),
            table_name: table_name.into(),
            api_key: String::new(),
        }
    }
}

#[cfg(feature = "proxy")]
pub struct ProxyClient {
    endpoint: String,
    options: ProxyOptions,
}

#[cfg(feature = "proxy")]
impl ProxyClient {
    pub fn connect(options: ProxyOptions) -> Self {
        let endpoint = options.endpoint.trim_end_matches('/').to_string();
        Self { endpoint, options }
    }

    pub fn open_table(&self) -> Result<serde_json::Value> {
        self.open_table_with_options(None, None)
    }

    pub fn open_table_with_options(
        &self,
        pin_primary: Option<bool>,
        replica_read_policy: Option<&str>,
    ) -> Result<serde_json::Value> {
        let mut body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "pin_primary": pin_primary,
        });
        if let Some(replica_read_policy) = replica_read_policy {
            body.as_object_mut().expect("object body").insert(
                "replica_read_policy".to_string(),
                serde_json::json!(replica_read_policy),
            );
        }
        self.post_raw("/ProxyService/OpenTable", body)
    }

    pub fn get_proxy_config(&self) -> Result<serde_json::Value> {
        self.get_raw("/ProxyService/GetConfig")
    }

    pub fn update_proxy_config(&self, options: serde_json::Value) -> Result<serde_json::Value> {
        self.post_raw("/ProxyService/UpdateConfig", options)
    }

    pub fn refresh_topology(&self) -> Result<serde_json::Value> {
        self.post_raw("/ProxyService/RefreshTopology", serde_json::json!({}))
    }

    pub fn execute_command(
        &self,
        shard_id: u64,
        command: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "shard_id": shard_id,
            "command": command,
        });
        self.proxy_service_request("/ProxyService/ExecuteCmd", body)
    }

    pub fn batch_execute_commands(
        &self,
        shard_id: u64,
        commands: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "shard_id": shard_id,
            "commands": commands,
        });
        self.proxy_service_request("/ProxyService/BatchExecuteCmd", body)
    }

    pub fn table_execute_command(&self, command: serde_json::Value) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "command": command,
        });
        self.proxy_service_request("/ProxyService/TableExecuteCmd", body)
    }

    pub fn table_batch_execute_commands(
        &self,
        commands: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "commands": commands,
        });
        self.proxy_service_request("/ProxyService/TableBatchExecuteCmd", body)
    }

    pub fn put_string(&self, key: &str, value: &str) -> Result<()> {
        // Delegates, as `put_string_with_ttl` already delegates to `set_ex`. This used to post to
        // `/v1/string/put`, which the proxy does not serve.
        self.set(key, value)
    }

    pub fn put_string_with_ttl(&self, key: &str, value: &str, ttl_ms: u64) -> Result<()> {
        self.set_ex(key, value, ttl_ms)
    }

    pub fn get_string(&self, key: &str) -> Result<String> {
        // A missing key reads as empty, which is what the old `/v1/string/get` shape returned.
        Ok(self.get(key)?.unwrap_or_default())
    }

    pub fn add_sequence_feature_rows(&self, key: &str, rows: &[SequenceFeatureRow]) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[("rows", serde_json::to_value(rows).map_err(json_error)?)],
        );
        self.proxy_service_execute("/ProxyService/SequenceAdd", body)
            .map(|_| ())
    }

    pub fn query_sequence_feature_rows(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<SequenceFeatureRow>> {
        let encoded_filters: Vec<serde_json::Value> = filters
            .iter()
            .map(|filter| {
                serde_json::json!({
                    "field": filter.field,
                    "op": filter.op as i32,
                    "value": filter.value,
                })
            })
            .collect();
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ts)),
                ("end_ms", serde_json::json!(end_ts)),
                ("count", serde_json::json!(count)),
                ("filters", serde_json::json!(encoded_filters)),
            ],
        );
        let data = self.proxy_service_execute("/ProxyService/SequenceQuery", body)?;
        serde_json::from_value(
            data.get("rows")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        )
        .map_err(json_error)
    }

    pub fn add_feature_points(&self, key: &str, points: &[FeaturePoint]) -> Result<()> {
        self.feature_add(key, points)
    }

    pub fn add_feature_points_with_policy(
        &self,
        key: &str,
        points: &[FeaturePoint],
        policy: FeatureWritePolicy,
    ) -> Result<()> {
        self.feature_add_with_policy(key, points, Some(policy))
    }

    pub fn query_feature_points(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
    ) -> Result<Vec<FeaturePoint>> {
        self.feature_query(key, start_ts, end_ts, Some(count as usize))
    }

    pub fn query_feature_points_filtered(
        &self,
        key: &str,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: &[FeatureFilter],
    ) -> Result<Vec<FeaturePoint>> {
        self.feature_query_filtered(key, start_ts, end_ts, Some(count as usize), filters)
    }

    pub fn feature_add(&self, key: &str, points: &[FeaturePoint]) -> Result<()> {
        self.feature_add_with_policy(key, points, None)
    }

    pub fn feature_add_with_policy(
        &self,
        key: &str,
        points: &[FeaturePoint],
        policy: Option<FeatureWritePolicy>,
    ) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("points", serde_json::to_value(points).map_err(json_error)?),
                ("policy", serde_json::to_value(policy).map_err(json_error)?),
            ],
        );
        self.proxy_service_execute("/ProxyService/FeatureAdd", body)
            .map(|_| ())
    }

    pub fn feature_query(
        &self,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Result<Vec<FeaturePoint>> {
        self.feature_query_filtered(key, start_ms, end_ms, count, &[])
    }

    pub fn feature_query_filtered(
        &self,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
        filters: &[FeatureFilter],
    ) -> Result<Vec<FeaturePoint>> {
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ms)),
                ("end_ms", serde_json::json!(end_ms)),
                ("count", serde_json::json!(count)),
                (
                    "filters",
                    serde_json::to_value(filters).map_err(json_error)?,
                ),
            ],
        );
        let response = self.proxy_service_execute("/ProxyService/FeatureQuery", body)?;
        response_feature_points(response)
    }

    pub fn feature_replace(
        &self,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        points: &[FeaturePoint],
    ) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ms)),
                ("end_ms", serde_json::json!(end_ms)),
                ("points", serde_json::to_value(points).map_err(json_error)?),
            ],
        );
        self.proxy_service_execute("/ProxyService/FeatureReplace", body)
            .map(|_| ())
    }

    pub fn feature_delete(&self, key: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[]);
        self.proxy_service_execute("/ProxyService/FeatureDelete", body)
            .map(|_| ())
    }

    pub fn feature_aggregate(
        &self,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        aggregator: &str,
        count: Option<usize>,
    ) -> Result<i64> {
        let body = self.proxy_service_body(
            key,
            &[
                ("start_ms", serde_json::json!(start_ms)),
                ("end_ms", serde_json::json!(end_ms)),
                ("aggregator", serde_json::json!(aggregator)),
                ("count", serde_json::json!(count)),
            ],
        );
        let response = self.proxy_service_execute("/ProxyService/FeatureAggQuery", body)?;
        Ok(response
            .get("value")
            .and_then(|value| value.as_i64())
            .unwrap_or_default())
    }

    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[("value", serde_json::json!(value.as_bytes()))]);
        self.proxy_service_execute("/ProxyService/Set", body)
            .map(|_| ())
    }

    pub fn set_ex(&self, key: &str, value: &str, ttl_ms: u64) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("value", serde_json::json!(value.as_bytes())),
                ("ttl_ms", serde_json::json!(ttl_ms)),
            ],
        );
        self.proxy_service_execute("/ProxyService/SetEx", body)
            .map(|_| ())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/Get", body)?;
        Ok(response
            .get("value")
            .cloned()
            .filter(|value| !value.is_null())
            .map(json_byte_array_to_string))
    }

    pub fn risk_hset(&self, key: &str, timestamp_ms: u64, amount: i64) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("timestamp_ms", serde_json::json!(timestamp_ms)),
                ("amount", serde_json::json!(amount)),
            ],
        );
        self.proxy_service_execute("/ProxyService/ControlStateHset", body)
            .map(|_| ())
    }

    pub fn risk_fol_set(
        &self,
        key: &str,
        value: &str,
        occur_time_ms: u64,
        ttl_ms: u64,
        fol_type: RiskFolType,
    ) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("value", serde_json::json!(value.as_bytes())),
                ("occur_time_ms", serde_json::json!(occur_time_ms)),
                ("ttl_ms", serde_json::json!(ttl_ms)),
                (
                    // The route parses this as `selection_type`; the argument keeps its name.
                    "selection_type",
                    serde_json::to_value(fol_type).map_err(json_error)?,
                ),
            ],
        );
        self.proxy_service_execute("/ProxyService/ControlStateFolSet", body)
            .map(|_| ())
    }

    pub fn risk_fol_query(&self, key: &str) -> Result<Option<String>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/ControlStateFolQuery", body)?;
        Ok(response
            .get("value")
            .cloned()
            .filter(|value| !value.is_null())
            .map(json_byte_array_to_string))
    }

    pub fn risk_manager(&self, key: &str) -> Result<Vec<(String, String)>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/ControlStateManager", body)?;
        Ok(response_hash_entries_to_strings(response))
    }

    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<()> {
        let body = self.proxy_service_body(
            key,
            &[
                ("field", serde_json::json!(field)),
                ("value", serde_json::json!(value.as_bytes())),
            ],
        );
        self.proxy_service_execute("/ProxyService/HSet", body)
            .map(|_| ())
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<String> {
        let body = self.proxy_service_body(key, &[("field", serde_json::json!(field))]);
        let response = self.proxy_service_execute("/ProxyService/HGet", body)?;
        Ok(json_byte_array_to_string(
            response
                .get("value")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        ))
    }

    pub fn hmset(&self, key: &str, entries: &[(&str, &str)]) -> Result<()> {
        let entries = entries
            .iter()
            .map(|(field, value)| serde_json::json!([field, value.as_bytes()]))
            .collect::<Vec<_>>();
        let body = self.proxy_service_body(key, &[("entries", serde_json::json!(entries))]);
        self.proxy_service_execute("/ProxyService/HMSet", body)
            .map(|_| ())
    }

    pub fn hmget(&self, key: &str, fields: &[&str]) -> Result<Vec<Option<String>>> {
        let body = self.proxy_service_body(key, &[("fields", serde_json::json!(fields))]);
        let response = self.proxy_service_execute("/ProxyService/HMGet", body)?;
        let values = response
            .get("values")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|value| {
                if value.is_null() {
                    None
                } else {
                    Some(json_byte_array_to_string(value))
                }
            })
            .collect();
        Ok(values)
    }

    pub fn hgetall(&self, key: &str) -> Result<Vec<(String, String)>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/HGetAll", body)?;
        let entries = response
            .get("entries")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|entry| {
                let entry = entry.as_array()?;
                let field = entry.first()?.as_str()?.to_string();
                let value = json_byte_array_to_string(entry.get(1)?.clone());
                Some((field, value))
            })
            .collect();
        Ok(entries)
    }

    pub fn scan_hash(&self, key: &str) -> Result<Vec<(String, String)>> {
        self.hgetall(key)
    }

    pub fn hlen(&self, key: &str) -> Result<u64> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/HLen", body)?;
        Ok(response
            .get("value")
            .and_then(|value| value.as_u64())
            .unwrap_or_default())
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[("field", serde_json::json!(field))]);
        self.proxy_service_execute("/ProxyService/HDel", body)
            .map(|_| ())
    }

    pub fn sadd(&self, key: &str, member: &str) -> Result<()> {
        let body =
            self.proxy_service_body(key, &[("member", serde_json::json!(member.as_bytes()))]);
        self.proxy_service_execute("/ProxyService/SAdd", body)
            .map(|_| ())
    }

    pub fn smembers(&self, key: &str) -> Result<Vec<String>> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/SMembers", body)?;
        let members = response
            .get("members")
            .and_then(|value| value.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|member| {
                let bytes = member
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|item| item.as_u64().unwrap_or_default() as u8)
                    .collect::<Vec<_>>();
                String::from_utf8_lossy(&bytes).into_owned()
            })
            .collect();
        Ok(members)
    }

    pub fn srem(&self, key: &str, member: &str) -> Result<()> {
        let body =
            self.proxy_service_body(key, &[("member", serde_json::json!(member.as_bytes()))]);
        self.proxy_service_execute("/ProxyService/SRem", body)
            .map(|_| ())
    }

    pub fn delete_object(&self, key: &str) -> Result<()> {
        let body = self.proxy_service_body(key, &[]);
        self.proxy_service_execute("/ProxyService/Delete", body)
            .map(|_| ())
    }

    pub fn expire(&self, key: &str, ttl_ms: u64) -> Result<()> {
        let body = self.proxy_service_body(key, &[("ttl_ms", serde_json::json!(ttl_ms))]);
        self.proxy_service_execute("/ProxyService/Expire", body)
            .map(|_| ())
    }

    pub fn ttl(&self, key: &str) -> Result<u64> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/Ttl", body)?;
        Ok(response
            .get("value")
            .and_then(|value| value.as_u64())
            .unwrap_or_default())
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        let body = self.proxy_service_body(key, &[]);
        let response = self.proxy_service_execute("/ProxyService/Exists", body)?;
        Ok(response
            .get("value")
            .and_then(|value| value.as_i64())
            .unwrap_or_default()
            != 0)
    }

    fn proxy_service_body(
        &self,
        key: &str,
        extra: &[(&str, serde_json::Value)],
    ) -> serde_json::Value {
        let mut body = serde_json::json!({
            "namespace": self.options.namespace_name,
            "table_name": self.options.table_name,
            "key": key,
        });
        let object = body.as_object_mut().expect("object body");
        for (name, value) in extra {
            object.insert((*name).to_string(), value.clone());
        }
        body
    }

    fn proxy_service_execute(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let response = self.proxy_service_request(path, body)?;
        Ok(response
            .get("response")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})))
    }

    fn proxy_service_request(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let response = self.post_raw(path, body)?;
        let status_ok = response
            .get("status")
            .and_then(|status| status.get("ok"))
            .and_then(|ok| ok.as_bool())
            .unwrap_or(false);
        if !status_ok {
            return Err(Error {
                code: 0,
                message: response
                    .get("status")
                    .and_then(|status| status.get("message"))
                    .and_then(|value| value.as_str())
                    .unwrap_or("proxy service request failed")
                    .to_string(),
            });
        }
        Ok(response)
    }

    fn get_raw(&self, path: &str) -> Result<serde_json::Value> {
        self.http_json("GET", path, None)
    }

    fn post_raw(&self, path: &str, body: serde_json::Value) -> Result<serde_json::Value> {
        self.http_json("POST", path, Some(body))
    }

    fn http_json(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let (host, port, base_path) = parse_http_endpoint(&self.endpoint)?;
        let request_path = format!("{base_path}{path}");
        let payload = match body {
            Some(body) => serde_json::to_string(&body).map_err(json_error)?,
            None => String::new(),
        };
        let mut headers = format!(
            "{method} {request_path} HTTP/1.1\r\nHost: {host}:{port}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            payload.len()
        );
        if !self.options.api_key.is_empty() {
            headers.push_str(&format!(
                "Authorization: Bearer {}\r\n",
                self.options.api_key
            ));
        }

        headers.push_str("\r\n");
        let mut stream = TcpStream::connect(format!("{host}:{port}")).map_err(io_error)?;
        stream.write_all(headers.as_bytes()).map_err(io_error)?;
        stream.write_all(payload.as_bytes()).map_err(io_error)?;
        stream.flush().map_err(io_error)?;

        let mut response = String::new();
        stream.read_to_string(&mut response).map_err(io_error)?;
        let (head, body) = response.split_once("\r\n\r\n").ok_or_else(|| Error {
            code: 0,
            message: "invalid proxy HTTP response".to_string(),
        })?;
        if !head.starts_with("HTTP/1.1 2") && !head.starts_with("HTTP/1.0 2") {
            return Err(Error {
                code: 0,
                message: head
                    .lines()
                    .next()
                    .unwrap_or("proxy HTTP request failed")
                    .to_string(),
            });
        }
        serde_json::from_str(body).map_err(json_error)
    }
}

#[cfg(feature = "proxy")]
fn json_error(err: serde_json::Error) -> Error {
    Error {
        code: 0,
        message: err.to_string(),
    }
}

#[cfg(feature = "proxy")]
fn json_byte_array_to_string(value: serde_json::Value) -> String {
    let bytes = value
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|item| item.as_u64().unwrap_or_default() as u8)
        .collect::<Vec<_>>();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[cfg(feature = "proxy")]
fn response_hash_entries_to_strings(response: serde_json::Value) -> Vec<(String, String)> {
    response
        .get("entries")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.as_array()?;
            let field = entry.first()?.as_str()?.to_string();
            let value = json_byte_array_to_string(entry.get(1)?.clone());
            Some((field, value))
        })
        .collect()
}

#[cfg(feature = "proxy")]
fn response_feature_points(response: serde_json::Value) -> Result<Vec<FeaturePoint>> {
    serde_json::from_value(
        response
            .get("points")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .map_err(json_error)
}

#[cfg(feature = "proxy")]
fn io_error(err: std::io::Error) -> Error {
    Error {
        code: 0,
        message: err.to_string(),
    }
}

#[cfg(feature = "proxy")]
fn parse_http_endpoint(endpoint: &str) -> Result<(String, u16, String)> {
    let without_scheme = endpoint.strip_prefix("http://").ok_or_else(|| Error {
        code: 0,
        message: "Rust proxy SDK currently expects an http:// endpoint".to_string(),
    })?;
    let (authority, path) = without_scheme
        .split_once('/')
        .unwrap_or((without_scheme, ""));
    let (host, port) = if let Some((host, port)) = authority.rsplit_once(':') {
        let port = port.parse::<u16>().map_err(|_| Error {
            code: 0,
            message: "invalid proxy endpoint port".to_string(),
        })?;
        (host.to_string(), port)
    } else {
        (authority.to_string(), 80)
    };
    let base_path = if path.is_empty() {
        String::new()
    } else {
        format!("/{path}")
    };
    Ok((host, port, base_path))
}

#[cfg(feature = "direct")]
fn rust_c_error(message: impl Into<String>) -> *mut c_char {
    CString::new(message.into())
        .unwrap_or_else(|_| CString::new("Rust TemporalStore direct SDK error").unwrap())
        .into_raw()
}

#[cfg(feature = "direct")]
fn rust_c_string(value: impl Into<String>) -> *mut c_char {
    CString::new(value.into())
        .unwrap_or_else(|_| CString::new("").unwrap())
        .into_raw()
}

#[cfg(feature = "direct")]
unsafe fn set_error(error: *mut *mut c_char, message: impl Into<String>) {
    if !error.is_null() {
        *error = rust_c_error(message);
    }
}

#[cfg(feature = "direct")]
unsafe fn client_from_handle<'a>(handle: *mut std::ffi::c_void) -> Result<&'a mut Client> {
    if handle.is_null() {
        return Err(Error {
            code: 1,
            message: "null Rust TemporalStore direct SDK handle".to_string(),
        });
    }
    Ok(&mut *(handle as *mut Client))
}

#[cfg(feature = "direct")]
unsafe fn cstr_arg(value: *const c_char, name: &str) -> Result<String> {
    if value.is_null() {
        return Err(Error {
            code: 1,
            message: format!("{name} is null"),
        });
    }
    Ok(CStr::from_ptr(value).to_string_lossy().into_owned())
}

#[cfg(feature = "direct")]
fn options_from_json(raw: &str) -> Result<Options> {
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|err| Error {
        code: 1,
        message: format!("invalid options json: {err}"),
    })?;
    let get_str = |name: &str, default: &str| -> String {
        value
            .get(name)
            .and_then(|item| item.as_str())
            .unwrap_or(default)
            .to_string()
    };
    let get_i32 = |name: &str, default: i32| -> i32 {
        value
            .get(name)
            .and_then(|item| item.as_i64())
            .map(|item| item as i32)
            .unwrap_or(default)
    };
    let get_bool = |name: &str, default: bool| -> bool {
        value
            .get(name)
            .and_then(|item| item.as_bool())
            .unwrap_or(default)
    };
    let mut options = Options::new(
        get_str("metaserver_addr", "127.0.0.1:18000"),
        get_str("namespace_name", "deploy_ns"),
        get_str("table_name", "deploy_table"),
    );
    options.request_timeout_ms = get_i32("request_timeout_ms", options.request_timeout_ms);
    options.io_timeout_ms = get_i32("io_timeout_ms", options.io_timeout_ms);
    options.connect_timeout_ms = get_i32("connect_timeout_ms", options.connect_timeout_ms);
    options.max_read_retries = get_i32("max_read_retries", options.max_read_retries);
    options.max_write_retries = get_i32("max_write_retries", options.max_write_retries);
    options.retry_backoff_ms = get_i32("retry_backoff_ms", options.retry_backoff_ms);
    options.pin_primary = get_bool("pin_primary", options.pin_primary);
    Ok(options)
}

#[cfg(feature = "direct")]
fn run_c_abi<T>(error: *mut *mut c_char, f: impl FnOnce() -> Result<T>) -> c_int {
    match f() {
        Ok(_) => 0,
        Err(err) => {
            unsafe { set_error(error, err.message) };
            1
        }
    }
}

#[cfg(feature = "direct")]
fn run_c_abi_string(
    out: *mut *mut c_char,
    error: *mut *mut c_char,
    f: impl FnOnce() -> Result<String>,
) -> c_int {
    match f() {
        Ok(value) => {
            unsafe {
                if !out.is_null() {
                    *out = rust_c_string(value);
                }
            }
            0
        }
        Err(err) => {
            unsafe { set_error(error, err.message) };
            1
        }
    }
}

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
    fn direct_client_exposes_c_abi_parity_methods() {
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
        let _: fn(&Client, &str, i64, u64, super::RiskPrecision, &str, u64) -> super::Result<()> =
            Client::risk_increment;
        let _: fn(&Client, &str, super::RiskPrecision, super::RiskWindow) -> super::Result<i64> =
            Client::risk_count;
        let _: fn(&Client, &[(&str, &str, &str)], Option<&str>, Option<&str>) -> super::Result<()> =
            Client::matrixark_batch_append_records;
    }

    #[cfg(feature = "proxy")]
    #[test]
    fn proxy_client_exposes_proxy_parity_methods() {
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
        let _: fn(&ProxyClient, &str, &str) -> super::Result<()> = ProxyClient::set;
        let _: fn(&ProxyClient, &str) -> super::Result<Option<String>> = ProxyClient::get;
        let _: fn(&ProxyClient, &str, u64, i64) -> super::Result<()> = ProxyClient::risk_hset;
        let _: fn(&ProxyClient, &str, &str, u64, u64, super::RiskFolType) -> super::Result<()> =
            ProxyClient::risk_fol_set;
        let _: fn(&ProxyClient, &str) -> super::Result<Option<String>> =
            ProxyClient::risk_fol_query;
        let _: fn(&ProxyClient, &str) -> super::Result<Vec<(String, String)>> =
            ProxyClient::risk_manager;
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
