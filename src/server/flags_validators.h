// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <gflags/gflags.h>

#include "brpc/reloadable_flags.h"
#include "common/logging.h"

static bool PassLogLevel(const char* flag, uint64_t new_log_level) {
    byte::SetMinLogLevel(byte::LogLevel(new_log_level));
    return true;
}

DEFINE_string(host, "", "host");
DEFINE_string(host_v6, "", "ipv6 host");
DEFINE_int32(port, 0, "listen port");
DEFINE_string(server_log_dir, "./", "default log dir");
DEFINE_int32(server_log_num, 10, "default log num");
DEFINE_int32(server_log_size, 1 * 1024 * 1024 * 1024, "default log size");
DEFINE_string(master_consul, "", "master consul");
DEFINE_string(master_endpoint, "", "default master endpoint");
DEFINE_string(table_name, "", "table name");
DEFINE_uint64(server_log_level, 2, "A:0,D:1,I:2,W:3,E:4,F:5,N:100");
BRPC_VALIDATE_GFLAG(server_log_level, PassLogLevel);
DEFINE_string(cluster_name, "dev", "cluster name");
DEFINE_uint64(matrixobjectstore_client_max_write_size, 1024 * 1024 * 1024, "matrixobjectstore max size to write");
DEFINE_uint64(matrixobjectstore_log_file_num, 5, "matrixobjectstore client log file num per log level");
DEFINE_uint64(matrixobjectstore_log_file_size_mb, 1024, "matrixobjectstore client log file size");
DEFINE_uint64(worker_num, 4, "worker num");
