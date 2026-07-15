use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::{
    temporalstore_free_string, temporalstore_get_string, temporalstore_put_string,
    temporalstore_put_string_with_ttl,
};
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

}
