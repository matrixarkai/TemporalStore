use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::{
    temporalstore_sadd, temporalstore_smembers, temporalstore_string_array_free, CStringArray,
};
use crate::direct_helpers::{check, cstring};
use crate::Result;

impl Client {
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
