use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;

use crate::direct_ffi::*;
use crate::direct_helpers::{
    check, cstring, feature_points_from_c_array, ips_features_from_c_array, CStringOptions,
};
use crate::{
    ControlStatePrecision, ControlStateWindow, Error, FeatureFilter, FeaturePoint,
    FeatureWritePolicy, IpsFeatureStat, IpsInstance, IpsLastQuery, Options, Result,
    SequenceFeatureRow,
};

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

pub struct Client {
    pub(crate) raw: *mut TemporalStoreClientOpaque,
}

unsafe impl Send for Client {}

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

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
