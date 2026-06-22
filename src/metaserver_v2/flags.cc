// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "metaserver_v2/flags.h"

#include "brpc/reloadable_flags.h"

namespace bcache2 {
namespace metaserver {

DEFINE_string(metaserver_cluster_name, "dev", "cluster name");
DEFINE_int32(metaserver_server_port, 7000, "main service port");
DEFINE_string(metaserver_work_dir, "./data", "dir for data");
DEFINE_string(metaserver_log_dir, "./log", "dir for log");
DEFINE_int32(metaserver_log_level, 2, "A:0,D:1,I:2,W:3,E:4,F:5,N:100");
DEFINE_int32(metaserver_log_file_num, 10, "max file num");
DEFINE_int32(metaserver_log_file_size, 512 * 1024 * 1024, "max file size");
DEFINE_string(metaserver_announce_consul_name, "dev.bcache2.dev", "consul name");
DEFINE_string(metaserver_announce_consul_name_leader, "dev.bcache2.dev_leader", "consul name");
DEFINE_bool(metaserver_consul_announce_enabled, true, "enable metaserver consul announce");
BRPC_VALIDATE_GFLAG(metaserver_consul_announce_enabled, brpc::PassValidate);

DEFINE_uint64(metaserver_task_scheduler_interval_ms, 500,
              "task scheduler sleep interval in milliseconds");
BRPC_VALIDATE_GFLAG(metaserver_task_scheduler_interval_ms, brpc::PassValidate);
DEFINE_uint64(metaserver_task_scheduler_max_inflight, 10, "max inflight doing task, 0 -> pause");
BRPC_VALIDATE_GFLAG(metaserver_task_scheduler_max_inflight, brpc::PassValidate);
DEFINE_uint64(metaserver_task_scheduler_max_postpone_time_ms, 5 * 60 * 1'000, "max delay time");
BRPC_VALIDATE_GFLAG(metaserver_task_scheduler_max_postpone_time_ms, brpc::PassValidate);
DEFINE_uint64(metaserver_task_scheduler_base_postpone_time_ms, 1'000, "base delay time");
BRPC_VALIDATE_GFLAG(metaserver_task_scheduler_base_postpone_time_ms, brpc::PassValidate);

DEFINE_bool(metaserver_placement_host_deduplicate, true,
            "placment rule: same host for same partition set");
BRPC_VALIDATE_GFLAG(metaserver_placement_host_deduplicate, brpc::PassValidate);
DEFINE_uint64(metaserver_placement_low_load_candidate_count, 2,
              "placment rule: select how many low load nodes");
BRPC_VALIDATE_GFLAG(metaserver_placement_low_load_candidate_count, brpc::PassValidate);
DEFINE_uint64(metaserver_convict_routine_interval_ms, 2 * 1'000,
              "convict routine interval in ms, default 3sec");
DEFINE_uint64(metaserver_balance_routine_interval_ms, 5 * 60 * 1000,
              "balance routine interval in ms, default 5min");
BRPC_VALIDATE_GFLAG(metaserver_convict_routine_interval_ms, brpc::PassValidate);
DEFINE_double(metaserver_phi_failure_threshold, 5.0, "phi failure threshold, default 7");
BRPC_VALIDATE_GFLAG(metaserver_phi_failure_threshold, brpc::PassValidate);
DEFINE_uint64(metaserver_phi_interpret_pause_time_us, 5 * 1'000 * 1'000,
              "phi pause time, default 5sec");
BRPC_VALIDATE_GFLAG(metaserver_phi_interpret_pause_time_us, brpc::PassValidate);
DEFINE_uint64(metaserver_convict_safe_mode_warning_ratio, 5, "ratio of safemode, 1-100");
BRPC_VALIDATE_GFLAG(metaserver_convict_safe_mode_warning_ratio, brpc::PassValidate);
DEFINE_uint64(metaserver_convict_safe_mode_critical_ratio, 8, "ratio of safemode, 1-100");
BRPC_VALIDATE_GFLAG(metaserver_convict_safe_mode_critical_ratio, brpc::PassValidate);
DEFINE_bool(metaserver_convict_safe_mode_enabled, true,
            "enable tag-level safe mode before convicting failed servers");
BRPC_VALIDATE_GFLAG(metaserver_convict_safe_mode_enabled, brpc::PassValidate);
DEFINE_bool(metaserver_convict_server_enabled, true,
            "enable freeze partition server and recover partition");
BRPC_VALIDATE_GFLAG(metaserver_convict_server_enabled, brpc::PassValidate);
DEFINE_bool(metaserver_convict_force_for_orphan_partition, true,
            "force freeze partition even if this is the only healthy one");
BRPC_VALIDATE_GFLAG(metaserver_convict_force_for_orphan_partition, brpc::PassValidate);
DEFINE_bool(metaserver_convict_proxy_enabled, true, "enable freeze proxy ");
BRPC_VALIDATE_GFLAG(metaserver_convict_proxy_enabled, brpc::PassValidate);

DEFINE_bool(metaserver_balance_enabled, true, "enable balance routine");
BRPC_VALIDATE_GFLAG(metaserver_balance_enabled, brpc::PassValidate);
DEFINE_uint64(metaserver_max_balance_partition_per_round, 10, "max balance partition per round");
BRPC_VALIDATE_GFLAG(metaserver_max_balance_partition_per_round, brpc::PassValidate);
DEFINE_uint64(metaserver_balance_partition_count_safe_gap, 0, "safe gap of partition count");
BRPC_VALIDATE_GFLAG(metaserver_balance_partition_count_safe_gap, brpc::PassValidate);

DEFINE_bool(metaserver_crash_on_fsm_failure, true, "coredump if applying raft fsm failed");

DEFINE_bool(metaserver_freeze_missing_partition_enabled, true,
            "auto freeze partition missing in partition server");
BRPC_VALIDATE_GFLAG(metaserver_freeze_missing_partition_enabled, brpc::PassValidate);
DEFINE_int64(metaserver_frozen_table_cool_down_time_sec, 6 * 3600,
             "frozen table would be dropped after this time");
BRPC_VALIDATE_GFLAG(metaserver_frozen_table_cool_down_time_sec, brpc::PassValidate);
DEFINE_int64(metaserver_frozen_partition_cool_down_time_sec, 600,
             "frozen partition would be dropped after this time");
BRPC_VALIDATE_GFLAG(metaserver_frozen_partition_cool_down_time_sec, brpc::PassValidate);
DEFINE_int64(metaserver_loading_partition_max_loading_time_sec, 15 * 60,
             "max loading time for partition, freeze it if exceed");
BRPC_VALIDATE_GFLAG(metaserver_loading_partition_max_loading_time_sec, brpc::PassValidate);
DEFINE_bool(metaserver_forbid_auto_register_for_convict_server, true,
            "frozen server by convicting can not auto register itself");
BRPC_VALIDATE_GFLAG(metaserver_forbid_auto_register_for_convict_server, brpc::PassValidate);

DEFINE_uint64(metaserver_table_default_evicter_max_memory_mb, 10 * 1024, "max memory size, in MB");
BRPC_VALIDATE_GFLAG(metaserver_table_default_evicter_max_memory_mb, brpc::PassValidate);

DEFINE_bool(metaserver_meta_query_allow_read_stale, true,
            "allow follower to serve meta query service");
BRPC_VALIDATE_GFLAG(metaserver_meta_query_allow_read_stale, brpc::PassValidate);
DEFINE_uint64(metaserver_meta_query_max_concurrency, 100, "max concurrency of meta query service");
BRPC_VALIDATE_GFLAG(metaserver_meta_query_max_concurrency, brpc::PassValidate);
DEFINE_uint64(metaserver_proxy_calibrate_interval_ms, 10'000,
              "interval to routine proxy pick and release");
BRPC_VALIDATE_GFLAG(metaserver_proxy_calibrate_interval_ms, brpc::PassValidate);

DEFINE_uint64(metaserver_meta_check_routine_interval_sec, 60, "interval of meta check");
BRPC_VALIDATE_GFLAG(metaserver_meta_check_routine_interval_sec, brpc::PassValidate);
DEFINE_uint64(metaserver_meta_check_max_freeze_partition_per_min, 10,
              "max count of partition which could be frozen");
BRPC_VALIDATE_GFLAG(metaserver_meta_check_max_freeze_partition_per_min, brpc::PassValidate);
DEFINE_uint64(metaserver_missing_partition_reboot_grace_sec, 30,
              "grace window before freezing missing partitions after a server reboot");
BRPC_VALIDATE_GFLAG(metaserver_missing_partition_reboot_grace_sec, brpc::PassValidate);

DEFINE_uint64(metaserver_raft_max_applied_log_bytes, 10 * 64 * 1024 * 1024,
              "applied log limit, default 10 files for 64mb");
DEFINE_bool(metaserver_raft_metrics_on, true, "enable byteraft metrics");
DEFINE_string(metaserver_raft_metrics_prefix, "bcache2_raft", "the prefix of byteraft metrics");
DEFINE_uint64(metaserver_raft_id, 1, "current raft node id");
DEFINE_string(metaserver_raft_peers, "1,127.0.0.1:7010,127.0.0.1:7020,0",
              "remote raft node address");
DEFINE_uint64(metaserver_raft_shard, 300, "the num of shard");
DEFINE_uint64(metaserver_raft_read_timeout_ms, 1000, "kv read timeout");
DEFINE_uint64(metaserver_raft_write_timeout_ms, 1000, "kv write timeout");
DEFINE_uint64(metaserver_raft_heartbeat_cycle_ms, 1000, "raft heartbeat cycle ms");
DEFINE_uint64(metaserver_raft_election_cycle_ms, 30'000, "raft election cycle ms");
DEFINE_uint64(metaserver_raft_segment_size, 1024 * 1024 * 64, "raft log segment size");
DEFINE_uint64(metaserver_raft_max_sync_log_size, 32 * 1024, "raft flush log batch size");
DEFINE_uint64(metaserver_raft_max_apply_log_size, 16 * 1024, "raft flush log batch size");
DEFINE_uint64(metaserver_raft_max_inflight_apply_task, 1, "raft max inflight apply task size");
DEFINE_uint64(metaserver_raft_max_log_buffer_size, 16 * 1024 * 1024, "raft log buffer size");
DEFINE_bool(metaserver_raft_enable_reorder_queue, true, "enable raft reorder queue");
DEFINE_uint64(metaserver_raft_reorder_queue_size, 12864, "reorder queue size");
DEFINE_uint64(metaserver_raft_reorder_cache_us, 1000, "reorder cache timeout us");
DEFINE_bool(metaserver_raft_wal_sync, true, "wal sync immediately");
DEFINE_uint64(metaserver_raft_worker_num, 9, "worker num");
DEFINE_uint64(metaserver_raft_reader_num, 1, "worker num");
DEFINE_uint64(metaserver_raft_flusher_num, 3, "worker num");
DEFINE_uint64(metaserver_raft_applier_num, 9, "worker num");
DEFINE_uint64(metaserver_raft_executor_num, 9, "worker num");
DEFINE_uint64(metaserver_raft_snapshot_num, 1, "worker num");
DEFINE_uint64(metaserver_raft_max_msgs_each_poll, 500, "worker num");
DEFINE_uint64(metaserver_raft_group_queue_max_depth, 25000, "worker num");
DEFINE_uint64(metaserver_raft_transport_timeout_ms, 1000, "raft transport timeout ms");
DEFINE_uint64(metaserver_snapshot_trigger_interval_sec, 0,
              "raft snapshot trigger interval in sec, default 0 off");
BRPC_VALIDATE_GFLAG(metaserver_snapshot_trigger_interval_sec, brpc::PassValidate);
DEFINE_uint64(metaserver_snapshot_trigger_index_gap, 10000, "raft snapshot trigger log index gap ");
BRPC_VALIDATE_GFLAG(metaserver_snapshot_trigger_index_gap, brpc::PassValidate);

}  // namespace metaserver
}  // namespace bcache2
