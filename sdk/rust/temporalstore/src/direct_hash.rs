// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::{
    temporalstore_free_string, temporalstore_hash_entry_array_free, temporalstore_hdel,
    temporalstore_hget, temporalstore_hgetall, temporalstore_hset, CHashEntryArray,
};
use crate::direct_helpers::{check, cstring};
use crate::Result;

impl Client {
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
}
