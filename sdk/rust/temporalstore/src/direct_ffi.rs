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
pub(crate) struct CIpsFeatureStat {
    pub(crate) id: i64,
    pub(crate) slot: i32,
    pub(crate) has_slot: c_int,
    pub(crate) kind: i32,
    pub(crate) v1: i32,
    pub(crate) v2: i32,
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

#[repr(C)]
pub(crate) struct CIpsFeatureArray {
    pub(crate) count: usize,
    pub(crate) features: *mut CIpsFeatureStat,
}

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
