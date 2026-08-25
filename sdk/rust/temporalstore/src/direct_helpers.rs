// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

use crate::{
    temporalstore_free_string, CFeaturePointArray, COptions, Error, FeaturePoint,
};

pub(crate) struct CStringOptions {
    metaserver_addr: CString,
    metaserver_consul: CString,
    namespace_name: CString,
    table_name: CString,
    idc: CString,
    host: CString,
    psm: CString,
    log_dir: CString,
}

impl CStringOptions {
    pub(crate) fn new(options: &Options) -> Result<Self> {
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

    pub(crate) fn apply(&self, c_options: &mut COptions, options: &Options) {
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

pub(crate) fn cstring(value: &str) -> Result<CString> {
    CString::new(value).map_err(|_| Error {
        code: 3,
        message: "string contains interior null byte".to_string(),
    })
}

pub(crate) fn check(code: c_int, error: *mut c_char) -> Result<()> {
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

pub(crate) fn feature_points_from_c_array(out: &CFeaturePointArray) -> Vec<FeaturePoint> {
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

