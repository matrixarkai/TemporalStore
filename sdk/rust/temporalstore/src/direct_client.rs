use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use crate::direct_ffi::*;
use crate::direct_helpers::{check, cstring, CStringOptions};
use crate::direct_matrixark_guard::cpp_matrixark_c_api_bridge_allowed;
use crate::{Options, Result};

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
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
