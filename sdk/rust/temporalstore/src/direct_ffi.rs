use std::os::raw::{c_char, c_int};

pub(crate) use crate::direct_ffi_matrixark::*;
pub(crate) use crate::direct_ffi_types::*;

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
    pub(crate) fn temporalstore_put_string(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_put_string_with_ttl(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *const c_char,
        ttl_ms: u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_get_string(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        value: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_delete_object(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_expire(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        ttl_ms: u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_ttl(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        ttl_ms: *mut u64,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_hset(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        value: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_hget(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        value: *mut *mut c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_hgetall(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        entries: *mut CHashEntryArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_hdel(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        field: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_sadd(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        member: *const c_char,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_smembers(
        client: *mut TemporalStoreClientOpaque,
        key: *const c_char,
        members: *mut CStringArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_string_array_free(array: *mut CStringArray);
    pub(crate) fn temporalstore_hash_entry_array_free(array: *mut CHashEntryArray);
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
    pub(crate) fn temporalstore_add_ips_instance(
        client: *mut TemporalStoreClientOpaque,
        table: *const c_char,
        uid: i64,
        timestamp_us: i64,
        action_type: i32,
        logical_table: i32,
        features: *const CIpsFeatureStat,
        feature_count: usize,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_query_ips_last_instances(
        client: *mut TemporalStoreClientOpaque,
        table: *const c_char,
        uid: i64,
        action_type: i32,
        logical_table: i32,
        slot: i32,
        top_k: i32,
        last_instances: i64,
        features: *mut CIpsFeatureArray,
        error_message: *mut *mut c_char,
    ) -> c_int;
    pub(crate) fn temporalstore_ips_feature_array_free(features: *mut CIpsFeatureArray);
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
