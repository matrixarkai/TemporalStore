// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{check, cstring};
use crate::direct_matrixark_guard::cpp_matrixark_c_api_bridge_allowed;
use crate::Result;

impl Client {
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
}
