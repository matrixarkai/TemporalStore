// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::os::raw::{c_char, c_int};

use crate::direct_ffi_types::{
    CFeatureFilter, CFeaturePoint, CFeaturePointArray, CSequenceFeatureRow,
    CSequenceFeatureRowArray, TemporalStoreClientOpaque,
};

extern "C" {
    pub(crate) fn temporalstore_add_feature_points_with_policy(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        points: *const CFeaturePoint,
        count: usize,
        policy: c_int,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_query_feature_points(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        points: *mut CFeaturePointArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_query_feature_points_with_filters(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: *const CFeatureFilter,
        filter_count: usize,
        points: *mut CFeaturePointArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_feature_point_array_free(points: *mut CFeaturePointArray);
    pub(crate) fn temporalstore_add_sequence_feature_rows_with_policy(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        rows: *const CSequenceFeatureRow,
        count: usize,
        policy: c_int,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_query_sequence_feature_rows(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        start_ts: u64,
        end_ts: u64,
        count: u64,
        filters: *const CFeatureFilter,
        filter_count: usize,
        rows: *mut CSequenceFeatureRowArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_sequence_feature_row_array_free(
        rows: *mut CSequenceFeatureRowArray,
    );
    pub(crate) fn temporalstore_control_state_increment(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        amount: i64,
        ttl_seconds: u64,
        precision: i32,
        uuid: *const c_char,
        occur_time_seconds: u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_control_state_count(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        precision: i32,
        window_start: i64,
        window_end: i64,
        window_unit: i32,
        count: *mut i64,
        error_message: *mut *mut c_char,
    ) -> c_int;
}
