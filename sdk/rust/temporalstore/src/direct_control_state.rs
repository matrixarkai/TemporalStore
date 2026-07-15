use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::*;
use crate::direct_helpers::{check, cstring};
use crate::{ControlStatePrecision, ControlStateWindow, Result};

impl Client {
    pub fn control_state_increment(
        &self,
        key: &str,
        amount: i64,
        ttl_seconds: u64,
        precision: ControlStatePrecision,
        uuid: &str,
        occur_time_seconds: u64,
    ) -> Result<()> {
        let key = cstring(key)?;
        let uuid = cstring(uuid)?;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_control_state_increment(
                self.raw,
                key.as_ptr(),
                amount,
                ttl_seconds,
                precision as i32,
                uuid.as_ptr(),
                occur_time_seconds,
                &mut error,
            )
        };
        check(code, error)
    }

    pub fn control_state_count(
        &self,
        key: &str,
        precision: ControlStatePrecision,
        window: ControlStateWindow,
    ) -> Result<i64> {
        let key = cstring(key)?;
        let mut count = 0_i64;
        let mut error: *mut c_char = ptr::null_mut();
        let code = unsafe {
            temporalstore_control_state_count(
                self.raw,
                key.as_ptr(),
                precision as i32,
                window.start,
                window.end,
                window.unit as i32,
                &mut count,
                &mut error,
            )
        };
        check(code, error)?;
        Ok(count)
    }
}
