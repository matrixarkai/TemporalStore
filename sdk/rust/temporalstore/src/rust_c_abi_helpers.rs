// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};

use crate::{Client, Error, Options, Result};

fn rust_c_error(message: impl Into<String>) -> *mut c_char {
    CString::new(message.into())
        .unwrap_or_else(|_| CString::new("Rust TemporalStore direct SDK error").unwrap())
        .into_raw()
}

pub(crate) fn rust_c_string(value: impl Into<String>) -> *mut c_char {
    CString::new(value.into())
        .unwrap_or_else(|_| CString::new("").unwrap())
        .into_raw()
}

pub(crate) unsafe fn set_error(error: *mut *mut c_char, message: impl Into<String>) {
    if !error.is_null() {
        *error = rust_c_error(message);
    }
}

pub(crate) unsafe fn client_from_handle<'a>(
    handle: *mut std::ffi::c_void,
) -> Result<&'a mut Client> {
    if handle.is_null() {
        return Err(Error {
            code: 1,
            message: "null Rust TemporalStore direct SDK handle".to_string(),
        });
    }
    Ok(&mut *(handle as *mut Client))
}

pub(crate) unsafe fn cstr_arg(value: *const c_char, name: &str) -> Result<String> {
    if value.is_null() {
        return Err(Error {
            code: 1,
            message: format!("{name} is null"),
        });
    }
    Ok(CStr::from_ptr(value).to_string_lossy().into_owned())
}

pub(crate) fn options_from_json(raw: &str) -> Result<Options> {
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

pub(crate) fn run_c_abi<T>(error: *mut *mut c_char, f: impl FnOnce() -> Result<T>) -> c_int {
    match f() {
        Ok(_) => 0,
        Err(err) => {
            unsafe { set_error(error, err.message) };
            1
        }
    }
}

pub(crate) fn run_c_abi_string(
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
