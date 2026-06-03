// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/flags.h"

DEFINE_uint64(bench_run_time, 0, "run time in seconds, 0 indicates forever");
DEFINE_uint64(bench_run_count, 0, "operation count, 0 indicates infinity");
DEFINE_uint64(bench_port, 9015, "bench port");

// bench
DEFINE_string(bench_id, "",
              "bench id, we'll fill bench_id in key, use current time if this flag is empty");
DEFINE_uint64(bench_job_num, 4, "bench job number");
DEFINE_uint64(bench_depth, 100, "bench worker depth");
DEFINE_uint64(bench_key_ttl_ms, 24 * 3600 * 1000, "bench all key expire time");

// consistency checker
DEFINE_bool(bench_checker_enable, false, "whther bench should check consistency");
DEFINE_uint64(bench_checker_worker_num, 4, "checker worker");
DEFINE_bool(bench_checker_eventual_consistency_mode, false,
            "client check eventual consistcy rather than linearizable consistency");
DEFINE_uint64(bench_checker_max_operation_per_round, 100000, "operation num per round");
DEFINE_uint64(bench_checker_max_ambiguous_time_ms, 3000, "max ambiguous timeout");
DEFINE_uint64(bench_checker_max_expire_ambiguous_time_ms, 200, "max expire ambiguous time");
DEFINE_uint64(bench_checker_reuse_key_count, 1000, "bench persistent key count");
DEFINE_uint64(bench_checker_eventual_consistency_history_time_ms, 5 * 1000,
              "time guarantees that all copys reach eventual consistency");
DEFINE_uint64(bench_checker_timeout_ms, 300 * 1000, "checker timeout");

// log
DEFINE_uint64(bench_log_level, 0, "A:0,D:1,I:2,W:3,E:4,F:5,N:100");
// Due to conflict with glog log_dir flag
DEFINE_string(bench_log_dir, "./", "log dir");
DEFINE_uint64(bench_log_max_file_num, 10, "max log file number");
DEFINE_uint64(bench_log_max_file_size, 1UL << 30, "max log file size");

// client
DEFINE_string(bench_client_type, "bcache2", "brpc/bcache2/thrift");

// thrift client
DEFINE_string(bench_thrift_client_server_addr, "127.0.0.1:8888", "server addr");
DEFINE_string(bench_thrift_client_namespace_name, "consistency_bench_ns", "table namespace");
DEFINE_string(bench_thrift_client_table_name, "consistency_bench", "table name");

// brpc client
DEFINE_string(bench_brpc_client_server_addr, "127.0.0.1:8888", "server addr");
DEFINE_uint64(bench_brpc_client_partition_id, 0, "request partition id");

// bcache2 client
DEFINE_string(bench_bcache2_client_idc, "", "bcache2 idc");
DEFINE_string(bench_bcache2_client_table_uri, "", "table uri");
DEFINE_bool(bench_bcache2_client_pin_primary, false, "pick primary");
DEFINE_uint64(bench_bcache2_client_meta_sync_interval_ms, 10 * 60 * 1000, "meta sync interval");
DEFINE_uint64(bench_bcache2_client_topo_error_retry_interval_ms,
    10 * 1000, "topo error retry interval");

// workloads bunch
DEFINE_uint64(bench_common_workload_freq, 1, "freq for common workload");
DEFINE_uint64(bench_string_workload_freq, 0, "freq for string workload");
DEFINE_uint64(bench_hash_workload_freq, 1, "freq for hash workload");
DEFINE_uint64(bench_ips_workload_freq, 0, "freq for ips workload");

// workload base
DEFINE_string(bench_workload_key_dis, "R", "R for Uniform Random, Z for Zipfian");
DEFINE_uint64(bench_workload_key_size, 16, "key size for random key");
DEFINE_uint64(bench_workload_key_count, 10000, "key count");
DEFINE_string(bench_workload_key_pattern, "S", "S for sequential");
DEFINE_double(bench_workload_zipfian_alpha, 0.75, "zipfian alpha");
DEFINE_uint64(bench_workload_value_count, 1000, "value count");
DEFINE_uint64(bench_workload_value_min_size, 32, "min value size");
DEFINE_uint64(bench_workload_value_max_size, 256, "max value size");

// common workload
DEFINE_uint64(bench_common_workload_freq_del, 1, "freq for del");
DEFINE_uint64(bench_common_workload_freq_expire, 1, "freq for expire");
DEFINE_uint64(bench_common_workload_freq_ttl, 1, "freq for ttl");
DEFINE_uint64(bench_common_workload_expire_min_ttl_ms, 1 * 1000, "random min expire time");
DEFINE_uint64(bench_common_workload_expire_max_ttl_ms, 60 * 1000, "random max expire time");

// hash workload
DEFINE_uint64(bench_hash_workload_freq_hset, 1, "freq for hset");
DEFINE_uint64(bench_hash_workload_freq_hget, 1, "freq for hget");
DEFINE_uint64(bench_hash_workload_freq_hgetall, 1, "freq for hgetall");
DEFINE_uint64(bench_hash_workload_freq_hdel, 1, "freq for hdel");
DEFINE_uint64(bench_hash_workload_field_count, 20, "hash field count");

// string workload
DEFINE_uint64(bench_string_workload_freq_set, 1, "freq for set");
DEFINE_uint64(bench_string_workload_freq_setex, 1, "freq for setex");
DEFINE_uint64(bench_string_workload_freq_get, 1, "freq for get");
DEFINE_uint64(bench_string_workload_setex_min_ttl_ms, 1 * 1000, "random min expire time");
DEFINE_uint64(bench_string_workload_setex_max_ttl_ms, 60 * 1000, "random max expire time");

// ips workload
DEFINE_uint64(bench_ips_workload_freq_add, 1, "freq for add");
DEFINE_uint64(bench_ips_workload_freq_query, 1, "freq for query");
