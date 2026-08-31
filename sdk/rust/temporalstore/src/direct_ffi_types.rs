// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::{c_char, c_int};

#[repr(C)]
pub(crate) struct TemporalStoreClientOpaque {
    pub(crate) _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct COptions {
    pub(crate) metaserver_addr: *const c_char,
    pub(crate) metaserver_consul: *const c_char,
    pub(crate) namespace_name: *const c_char,
    pub(crate) table_name: *const c_char,
    pub(crate) idc: *const c_char,
    pub(crate) host: *const c_char,
    pub(crate) psm: *const c_char,
    pub(crate) log_dir: *const c_char,
    pub(crate) log_level: c_int,
    pub(crate) io_timeout_ms: c_int,
    pub(crate) connect_timeout_ms: c_int,
    pub(crate) request_timeout_ms: c_int,
    pub(crate) max_read_retries: c_int,
    pub(crate) max_write_retries: c_int,
    pub(crate) retry_backoff_ms: c_int,
    pub(crate) max_feature_points_per_request: c_int,
    pub(crate) max_feature_query_count: u64,
    pub(crate) max_key_bytes: u64,
    pub(crate) max_value_bytes: u64,
    pub(crate) pin_primary: c_int,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct CFeatureFilter {
    pub(crate) field: *const c_char,
    pub(crate) op: c_int,
    pub(crate) value: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct CFeaturePoint {
    pub(crate) timestamp: u64,
    pub(crate) value: *const c_char,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct CSequenceFeatureRow {
    pub(crate) timestamp: u64,
    pub(crate) gid: u64,
    pub(crate) action_type: u32,
    pub(crate) duration: u32,
    pub(crate) author_id: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct CHashEntry {
    pub(crate) key: *const c_char,
    pub(crate) field: *const c_char,
    pub(crate) value: *const c_char,
    pub(crate) route_json: *const c_char,
}

#[repr(C)]
pub(crate) struct CStringArray {
    pub(crate) count: usize,
    pub(crate) values: *mut *mut c_char,
}

#[repr(C)]
pub(crate) struct CHashEntryArray {
    pub(crate) count: usize,
    pub(crate) entries: *mut CHashEntry,
}

#[repr(C)]
pub(crate) struct CFeaturePointArray {
    pub(crate) count: usize,
    pub(crate) points: *mut CFeaturePoint,
}

#[repr(C)]
pub(crate) struct CSequenceFeatureRowArray {
    pub(crate) count: usize,
    pub(crate) rows: *mut CSequenceFeatureRow,
}
