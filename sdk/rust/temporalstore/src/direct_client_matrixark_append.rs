// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{check, cstring};
use crate::direct_matrixark_guard::cpp_matrixark_c_api_bridge_allowed;
use crate::Result;

impl Client {
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
        let count_value_c = count_value.map(cstring).transpose()?;
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
}
