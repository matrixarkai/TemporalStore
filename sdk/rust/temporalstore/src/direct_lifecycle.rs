// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::c_char;
use std::ptr;

use crate::direct_client::Client;
use crate::direct_ffi::{temporalstore_delete_object, temporalstore_expire, temporalstore_ttl};
use crate::direct_helpers::{check, cstring};
use crate::Result;

impl Client {
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
}
