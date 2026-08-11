// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::{c_char, c_int};

use crate::direct_ffi_types::{CHashEntry, TemporalStoreClientOpaque};

extern "C" {
    pub(crate) fn temporalstore_matrixark_batch_append_records(
        client: *mut TemporalStoreClientOpaque,
        entries: *const CHashEntry,
        entry_count: usize,
        count_key: *const c_char,
        count_value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_matrixark_batch_append_records_v2(
        client: *mut TemporalStoreClientOpaque,
        entries: *const CHashEntry,
        entry_count: usize,
        count_key: *const c_char,
        count_value: *const c_char,
        append_options_json: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_matrixark_scan_candidates(
        client: *mut TemporalStoreClientOpaque,
        count_key: *const c_char,
        record_hash_key: *const c_char,
        shard_size: usize,
        request_json: *const c_char,
        candidates_json: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_matrixark_retrieve_context_pack(
        client: *mut TemporalStoreClientOpaque,
        count_key: *const c_char,
        record_hash_key: *const c_char,
        shard_size: usize,
        request_json: *const c_char,
        context_pack_json: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
}
