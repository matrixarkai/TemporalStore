// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "proxy/flags.h"

#include <string>

#include "brpc/reloadable_flags.h"
#include "common/logging.h"

static bool PassLogLevel(const char* flag, uint64_t new_log_level) {
    byte::SetMinLogLevel(byte::LogLevel(new_log_level));
    return true;
}

DEFINE_int32(port, 0, "listen port");
DEFINE_string(proxy_log_dir, "./", "default log dir");
DEFINE_uint64(log_max_file_num, 10, "log max file num");
DEFINE_uint64(log_max_file_size, 1 * 1024 * 1024 * 1024, "log max file size");
DEFINE_string(master_consul, "", "master consul");
DEFINE_string(master_endpoint, "", "default master endpoint");
DEFINE_string(idc, "", "proxy idc");
DEFINE_uint64(proxy_log_level, 2, "A:0,D:1,I:2,W:3,E:4,F:5,N:100");
BRPC_VALIDATE_GFLAG(proxy_log_level, PassLogLevel);
DEFINE_string(register_consul, "", "register consul");
DEFINE_uint64(register_ttl_s, 10, "service register ttl in seconds");
BRPC_VALIDATE_GFLAG(register_ttl_s, brpc::PassValidate);
DEFINE_uint64(heartbeat_interval_ms, 3000, "heartbeat interval in milliseconds");
BRPC_VALIDATE_GFLAG(heartbeat_interval_ms, brpc::PassValidate);

DEFINE_string(proxy_cluster_name, "", "cluster name");
DEFINE_string(proxy_vregion, "", "vregion");
DEFINE_string(proxy_vdc, "", "vdc");
DEFINE_string(proxy_vau, "", "vau");
DEFINE_uint64(proxy_heartbeat_timeout_ms, 5000, "heartbeat timeout in milliseconds");
BRPC_VALIDATE_GFLAG(proxy_heartbeat_timeout_ms, brpc::PassValidate);
DEFINE_bool(proxy_auto_register, true, "auto register to metaserver");
