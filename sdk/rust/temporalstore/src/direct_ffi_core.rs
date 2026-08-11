// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::{c_char, c_int};

use crate::direct_ffi_types::{COptions, TemporalStoreClientOpaque};

extern "C" {
    pub(crate) fn temporalstore_options_init(options: *mut COptions);
    pub(crate) fn temporalstore_free_string(value: *mut c_char);
    pub(crate) fn temporalstore_connect(
        options: *const COptions,
        client: *mut *mut TemporalStoreClientOpaque,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_close(
        client: *mut TemporalStoreClientOpaque,
        error_message: *mut *mut c_char,
    ) -> c_int;
}
