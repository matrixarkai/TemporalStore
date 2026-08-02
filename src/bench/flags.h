// Copyright (c) 2022-present);

#pragma once

#include <gflags/gflags.h>

DECLARE_uint64(bench_run_time);
DECLARE_uint64(bench_run_count);
DECLARE_uint64(bench_port);

// bench
DECLARE_string(bench_id);
DECLARE_uint64(bench_job_num);
DECLARE_uint64(bench_depth);
DECLARE_uint64(bench_key_ttl_ms);

// consistency checker
DECLARE_bool(bench_checker_enable);
DECLARE_uint64(bench_checker_worker_num);
DECLARE_bool(bench_checker_eventual_consistency_mode);
DECLARE_uint64(bench_checker_max_operation_per_round);
DECLARE_uint64(bench_checker_max_ambiguous_time_ms);
DECLARE_uint64(bench_checker_max_expire_ambiguous_time_ms);
DECLARE_uint64(bench_checker_reuse_key_count);
DECLARE_uint64(bench_checker_eventual_consistency_history_time_ms);
DECLARE_uint64(bench_checker_timeout_ms);

// log
DECLARE_uint64(bench_log_level);
// Due to conflict with glog log_dir flag
DECLARE_string(bench_log_dir);
DECLARE_uint64(bench_log_max_file_num);
DECLARE_uint64(bench_log_max_file_size);

// client
DECLARE_string(bench_client_type);

// thrift client
DECLARE_string(bench_thrift_client_server_addr);
DECLARE_string(bench_thrift_client_namespace_name);
DECLARE_string(bench_thrift_client_table_name);

// brpc client
DECLARE_string(bench_brpc_client_server_addr);
DECLARE_uint64(bench_brpc_client_partition_id);

// bcache2 client
DECLARE_string(bench_bcache2_client_idc);
DECLARE_string(bench_bcache2_client_table_uri);
DECLARE_bool(bench_bcache2_client_pin_primary);
DECLARE_uint64(bench_bcache2_client_meta_sync_interval_ms);
DECLARE_uint64(bench_bcache2_client_topo_error_retry_interval_ms);

// workloads bunch
DECLARE_uint64(bench_common_workload_freq);
DECLARE_uint64(bench_string_workload_freq);
DECLARE_uint64(bench_hash_workload_freq);
DECLARE_uint64(bench_ips_workload_freq);

// workload base
DECLARE_string(bench_workload_key_dis);
DECLARE_uint64(bench_workload_key_size);
DECLARE_uint64(bench_workload_key_count);
DECLARE_string(bench_workload_key_pattern);
DECLARE_double(bench_workload_zipfian_alpha);
DECLARE_uint64(bench_workload_value_count);
DECLARE_uint64(bench_workload_value_min_size);
DECLARE_uint64(bench_workload_value_max_size);

// common workload
DECLARE_uint64(bench_common_workload_freq_del);
DECLARE_uint64(bench_common_workload_freq_expire);
DECLARE_uint64(bench_common_workload_freq_ttl);
DECLARE_uint64(bench_common_workload_expire_min_ttl_ms);
DECLARE_uint64(bench_common_workload_expire_max_ttl_ms);

// hash workload
DECLARE_uint64(bench_hash_workload_freq_hset);
DECLARE_uint64(bench_hash_workload_freq_hget);
DECLARE_uint64(bench_hash_workload_freq_hdel);
DECLARE_uint64(bench_hash_workload_freq_hgetall);
DECLARE_uint64(bench_hash_workload_field_count);

// string workload
DECLARE_uint64(bench_string_workload_freq_set);
DECLARE_uint64(bench_string_workload_freq_setex);
DECLARE_uint64(bench_string_workload_freq_get);
DECLARE_uint64(bench_string_workload_setex_min_ttl_ms);
DECLARE_uint64(bench_string_workload_setex_max_ttl_ms);

// ips workload
DECLARE_uint64(bench_ips_workload_freq_add);
DECLARE_uint64(bench_ips_workload_freq_query);
