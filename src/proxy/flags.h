// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <gflags/gflags.h>

DECLARE_int32(port);
DECLARE_string(idc);
DECLARE_string(proxy_log_dir);
DECLARE_uint64(log_max_file_num);
DECLARE_uint64(log_max_file_size);
DECLARE_string(master_consul);
DECLARE_string(master_endpoint);
DECLARE_uint64(proxy_log_level);
DECLARE_string(register_consul);
DECLARE_uint64(register_ttl_s);
DECLARE_uint64(heartbeat_interval_ms);
DECLARE_string(proxy_cluster_name);
DECLARE_string(proxy_vregion);
DECLARE_string(proxy_vdc);
DECLARE_string(proxy_vau);
DECLARE_uint64(proxy_heartbeat_timeout_ms);
DECLARE_bool(proxy_auto_register);
DECLARE_string(proxy_ingestion_account);
DECLARE_bool(proxy_ingestion_enforce_account);
DECLARE_uint64(proxy_ingestion_max_inflight);
DECLARE_uint64(proxy_ingestion_max_write_inflight);
DECLARE_bool(proxy_pin_primary_reads);
DECLARE_int64(proxy_backend_io_timeout_ms);
DECLARE_int64(proxy_backend_connect_timeout_ms);
