// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <gflags/gflags.h>
#include <string>

#include "brpc/reloadable_flags.h"

static bool ValidateZoneSize(const char* flagname, int32_t value) {
    (void)flagname;
    return value >= 0;
}

static bool ValidateDataReplicationMode(const char* flagname, const std::string& value) {
    (void)flagname;
    return value == "shared_store" || value == "primary_pull" || value == "raft_consensus";
}

DEFINE_uint64(storage_loop_interval_us, 30000, "storage loop interval in us");
BRPC_VALIDATE_GFLAG(storage_loop_interval_us, brpc::PassValidate);

DEFINE_bool(start_storage_manager_when_loading, true,
            "start storage loopwork when loading partition");
BRPC_VALIDATE_GFLAG(start_storage_manager_when_loading, brpc::PassValidate);
DEFINE_bool(storage_enable_oplog_rolling, true, "enable oplog rolling");
BRPC_VALIDATE_GFLAG(storage_enable_oplog_rolling, brpc::PassValidate);
DEFINE_bool(storage_enable_evict, true, "enable evict");
BRPC_VALIDATE_GFLAG(storage_enable_evict, brpc::PassValidate);
DEFINE_bool(storage_enable_expire, true, "enable expire");
BRPC_VALIDATE_GFLAG(storage_enable_expire, brpc::PassValidate);
DEFINE_bool(storage_enable_page_gc, true, "enable page gc");
BRPC_VALIDATE_GFLAG(storage_enable_page_gc, brpc::PassValidate);
DEFINE_bool(storage_enable_page_compaction, true, "enable page compaction");
BRPC_VALIDATE_GFLAG(storage_enable_page_compaction, brpc::PassValidate);
DEFINE_bool(storage_enable_index_gc, true, "enable index gc");
BRPC_VALIDATE_GFLAG(storage_enable_index_gc, brpc::PassValidate);
DEFINE_uint64(storage_dump_index_meta_oplog_gap, 1 * 1024 * 1024,
              "oplog gap in bytes to dump index meta");
BRPC_VALIDATE_GFLAG(storage_dump_index_meta_oplog_gap, brpc::PassValidate);
DEFINE_bool(storage_async, false, "async write storage");
BRPC_VALIDATE_GFLAG(storage_async, brpc::PassValidate);
DEFINE_bool(partition_commit_oplog, true, "commit oplog after execute cmd finish");
DEFINE_string(storage_canonical_log_ack_policy, "durable",
              "Write ack boundary for the minimal canonical oplog. durable waits for the "
              "oplog stream commit before acknowledging, while best_effort keeps the legacy "
              "async fire-and-ack behavior and can lose the last acknowledged commands on an "
              "unexpected primary shutdown.");
DEFINE_string(data_replication_mode, "shared_store",
              "data-node replication mode: shared_store, primary_pull, or raft_consensus. "
              "shared_store preserves the existing shared stream/object-store replay path; "
              "primary_pull uses remote primary stream reads for secondary catch-up; "
              "raft_consensus uses Byteraft for quorum replication, transport, WAL, and "
              "snapshots.");
DEFINE_validator(data_replication_mode, ValidateDataReplicationMode);
DEFINE_uint64(expirer_scan_slots_per_round, 5, "scan slots for expirer in each round");
BRPC_VALIDATE_GFLAG(expirer_scan_slots_per_round, brpc::PassValidate);
DEFINE_uint64(expirer_scan_in_memory_slots_per_round, 100,
              "scan in memory slots for expirer in each round");
BRPC_VALIDATE_GFLAG(expirer_scan_in_memory_slots_per_round, brpc::PassValidate);
DEFINE_uint64(trigger_update_index_meta_time_ms, 1000, "trigger update index meta time");
BRPC_VALIDATE_GFLAG(trigger_update_index_meta_time_ms, brpc::PassValidate);

// flags for page store
DEFINE_bool(page_store_enable_compress, false, "enable compress");
BRPC_VALIDATE_GFLAG(page_store_enable_compress, brpc::PassValidate);
DEFINE_uint64(page_store_compress_trigger_threshold, 10240, "lowest threshold to trigger compress");
BRPC_VALIDATE_GFLAG(page_store_compress_trigger_threshold, brpc::PassValidate);

// flags for oplog dump
DEFINE_uint64(storage_dump_slots_per_round, 200, "max slots for dump in each round");
BRPC_VALIDATE_GFLAG(storage_dump_slots_per_round, brpc::PassValidate);
DEFINE_uint64(storage_oplog_delay_dump_length, 134217728, "delay dump oplog length");
BRPC_VALIDATE_GFLAG(storage_oplog_delay_dump_length, brpc::PassValidate);

// flags for gc
DEFINE_uint64(storage_gc_max_bytes_per_round, 1048576, "max bytes for gc each round");
BRPC_VALIDATE_GFLAG(storage_gc_max_bytes_per_round, brpc::PassValidate);
DEFINE_uint64(storage_gc_max_slots_per_round, 500, "max slots for gc each round");
BRPC_VALIDATE_GFLAG(storage_gc_max_slots_per_round, brpc::PassValidate);
DEFINE_double(storage_gc_space_utility_threshold, 0.5, "space utility threshold for gc");
BRPC_VALIDATE_GFLAG(storage_gc_space_utility_threshold, brpc::PassValidate);
DEFINE_uint64(storage_gc_zone_destroy_delay_ms, 24 * 3600 * 1000,
              "will be soon destroyed after compacted");
BRPC_VALIDATE_GFLAG(storage_gc_zone_destroy_delay_ms, brpc::PassValidate);
DEFINE_int32(storage_zone_size, 1 << 30, "zone size, 1GB by default");
DEFINE_validator(storage_zone_size, ValidateZoneSize);
DEFINE_int32(storage_max_zone_count, 10000, "max zone count, 1024 by default");

DEFINE_uint64(slow_request_threshold_us, 10 * 1000, "slow request threshold");
BRPC_VALIDATE_GFLAG(slow_request_threshold_us, brpc::PassValidate);

// flags for evict
DEFINE_uint64(evict_count_limit, 100, "default number for limit");
BRPC_VALIDATE_GFLAG(evict_count_limit, brpc::PassValidate);
DEFINE_uint64(evicter_max_memory_usage, 0, "max memory size, in MB");
BRPC_VALIDATE_GFLAG(evicter_max_memory_usage, brpc::PassValidate);
DEFINE_uint64(evict_batch_size, 10, "batch size to evict slot");
BRPC_VALIDATE_GFLAG(evict_batch_size, brpc::PassValidate);
DEFINE_uint64(limit_evict_scan_turns, 1, "max scanned turns for evict a single batch");
BRPC_VALIDATE_GFLAG(limit_evict_scan_turns, brpc::PassValidate);

// flags for index gc
DEFINE_double(index_gc_usage_trigger, 0.5, "usage triger for index gc");
BRPC_VALIDATE_GFLAG(index_gc_usage_trigger, brpc::PassValidate);
DEFINE_uint64(index_gc_max_num_per_round, 200, "max num per round for index gc");
BRPC_VALIDATE_GFLAG(index_gc_max_num_per_round, brpc::PassValidate);
DEFINE_uint64(index_gc_bytes_threshold, 1024 * 1024, "stream length threshold for index gc");
BRPC_VALIDATE_GFLAG(index_gc_bytes_threshold, brpc::PassValidate);

// flags for check & alarm
DEFINE_uint64(object_size_alarm_threshold, 1UL << 23, "object szie threshold to trigger alarm");
BRPC_VALIDATE_GFLAG(object_size_alarm_threshold, brpc::PassValidate);

// flags for replicator
DEFINE_uint64(replicator_loop_interval_us, 1000, "replicator loop interval in us");
BRPC_VALIDATE_GFLAG(replicator_loop_interval_us, brpc::PassValidate);
DEFINE_uint64(replicator_max_oplog_per_loop, 20000, "max oplog per loop");
BRPC_VALIDATE_GFLAG(replicator_max_oplog_per_loop, brpc::PassValidate);
DEFINE_uint64(replicator_max_indexlog_per_loop, 20000, "max indexlog per loop");
BRPC_VALIDATE_GFLAG(replicator_max_indexlog_per_loop, brpc::PassValidate);
DEFINE_uint64(replicator_out_of_sync_s, 1800, "out of sync time");
BRPC_VALIDATE_GFLAG(replicator_out_of_sync_s, brpc::PassValidate);
DEFINE_uint64(replicator_update_remote_interval_ms, 20, "update remote info interval");
BRPC_VALIDATE_GFLAG(replicator_update_remote_interval_ms, brpc::PassValidate);

// flags for page compaction
DEFINE_uint64(page_compaction_max_slots_per_round, 200, "max slots for page compaction each round");
BRPC_VALIDATE_GFLAG(page_compaction_max_slots_per_round, brpc::PassValidate);
DEFINE_uint64(page_compaction_max_bytes_per_round, 1048576,
              "max bytes for page compaction each round");
BRPC_VALIDATE_GFLAG(page_compaction_max_bytes_per_round, brpc::PassValidate);

// flags for reap metrics
DEFINE_uint64(reap_metrics_max_slots_per_round, 500, "max slots for reap metrics each round");
BRPC_VALIDATE_GFLAG(reap_metrics_max_slots_per_round, brpc::PassValidate);
