use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{check, cstring};
use crate::Result;

impl Client {
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

}
