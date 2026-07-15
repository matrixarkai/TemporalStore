use std::ffi::CString;
use std::os::raw::{c_char, c_int};

use crate::rust_c_abi_helpers::{
    client_from_handle, cstr_arg, options_from_json, run_c_abi, run_c_abi_string, set_error,
};
use crate::{Client, Result};

#[no_mangle]
pub extern "C" fn temporalstore_rust_free_string(value: *mut c_char) {
    if !value.is_null() {
        unsafe {
            let _ = CString::from_raw(value);
        }
    }
}

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
