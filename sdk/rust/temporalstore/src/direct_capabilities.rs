use std::ffi::CString;
use std::os::raw::{c_char, c_int};
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{
    check, cstring, feature_points_from_c_array,
};
use crate::{
    FeatureFilter, FeaturePoint, FeatureWritePolicy, Result, SequenceFeatureRow,
};

impl Client {
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

}
