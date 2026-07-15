use std::fmt;

#[cfg(feature = "direct")]
use std::ffi::{CStr, CString};
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

mod types;
pub use types::{
    ControlStateFolType, ControlStateHType, ControlStatePrecision, ControlStateWindow,
    ControlStateWindowUnit, FeatureFilter, FeatureFilterOp, FeaturePoint, FeatureWritePolicy,
    IpsFeatureStat, IpsInstance, IpsLastQuery, SequenceFeatureRow,
};

mod options;
pub use options::Options;

#[cfg(feature = "direct")]
mod direct_helpers;
#[cfg(feature = "direct")]
use direct_helpers::{
    check, cstring, feature_points_from_c_array, ips_features_from_c_array, CStringOptions,
};

#[cfg(feature = "direct")]
mod direct_ffi;
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
fn cpp_matrixark_c_api_bridge_allowed(op: &str) -> Result<()> {
    let allowed = std::env::var("TEMPORALSTORE_RUST_ALLOW_CPP_MATRIXARK_C_API")
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
            "Rust MatrixArk hot path {op} would call the shared C++ C API bridge. \
             Use the Rust-native temporalstore-rust matrixark_rust_proxy/direct SDK path, \
             or set TEMPORALSTORE_RUST_ALLOW_CPP_MATRIXARK_C_API=1 only for compatibility diagnostics."
        ),
    })
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
        if entries.is_empty() && count_key.unwrap_or("").is_empty() {
            return Ok(());
        }
        cpp_matrixark_c_api_bridge_allowed("matrixark_batch_append_records")?;
        let mut strings: Vec<CString> = Vec::with_capacity(entries.len() * 4 + 3);
        let mut c_entries: Vec<CHashEntry> = Vec::with_capacity(entries.len());
        for (key, field, value) in entries {
            let key_index = strings.len();
            strings.push(cstring(key)?);
            let field_index = strings.len();
            strings.push(cstring(field)?);
            let value_index = strings.len();
            strings.push(cstring(value)?);
            let route_index = strings.len();
            strings.push(cstring("{}")?);
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
        cpp_matrixark_c_api_bridge_allowed("matrixark_scan_candidates")?;
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
        cpp_matrixark_c_api_bridge_allowed("matrixark_retrieve_context_pack")?;
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

    pub fn add_ips_instance(&self, instance: &IpsInstance) -> Result<()> {
        let table = cstring(&instance.table)?;
        let features = instance
            .features
            .iter()
            .map(|feature| CIpsFeatureStat {
                id: feature.id,
                slot: feature.slot,
                has_slot: if feature.has_slot { 1 } else { 0 },
                kind: feature.kind,
                v1: feature.v1,
                v2: feature.v2,
            })
            .collect::<Vec<_>>();
        let features_ptr = if features.is_empty() {
            ptr::null()
        } else {
            features.as_ptr()
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_add_ips_instance(
                self.raw,
                table.as_ptr(),
                instance.uid,
                instance.timestamp_us,
                instance.action_type,
                instance.logical_table,
                features_ptr,
                features.len(),
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn query_ips_last_instances(&self, query: &IpsLastQuery) -> Result<Vec<IpsFeatureStat>> {
        let table = cstring(&query.table)?;
        let mut out = CIpsFeatureArray {
            count: 0,
            features: ptr::null_mut(),
        };
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_query_ips_last_instances(
                self.raw,
                table.as_ptr(),
                query.uid,
                query.action_type,
                query.logical_table,
                query.slot,
                query.top_k,
                query.last_instances,
                &mut out,
                &mut error,
            )
        };
        check(code, error)?;
        let features = ips_features_from_c_array(&out);
        unsafe { temporalstore_ips_feature_array_free(&mut out) };
        Ok(features)
    }

    pub fn control_state_increment(
        &self,
        key: &str,
        amount: i64,
        ttl_seconds: u64,
        precision: ControlStatePrecision,
        uuid: &str,
        occur_time_seconds: u64,
    ) -> Result<()> {
        let key = cstring(key)?;
        let uuid = cstring(uuid)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_control_state_increment(
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

    pub fn control_state_count(
        &self,
        key: &str,
        precision: ControlStatePrecision,
        window: ControlStateWindow,
    ) -> Result<i64> {
        let key = cstring(key)?;
        let mut count = 0_i64;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_control_state_count(
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
impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.close();
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
