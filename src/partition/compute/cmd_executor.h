// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/flat_hash_map.h>
#include <byte/base/closure.h>
#include <byte/include/macros.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/controller.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "common/module_config.h"
#include "model/ips/ips_table_manager.h"
#include "model/schema.h"
#include "partition/cmd_context.h"
#include "partition/metrics.h"
#include "partition/quota_manager.h"
#include "protocol/config.pb.h"
#include "protocol/server.pb.h"

namespace bcache2 {

class ExecuteEnv;

namespace partition {

class Partition;
class ObjectManager;

class CmdExecutor {
 public:
    CmdExecutor(Partition* partition, ObjectManager* object_manager, OpLogger* op_logger,
                MetricsManager* metrics_manager);
    ~CmdExecutor() = default;

    void Execute(uint16_t module_id, uint64_t function_id, const google::protobuf::Message* request,
                 google::protobuf::Message* response, CmdContext* cmd_ctx, Closure<void>* callback);
    void UpdateLimitConfig(const LimitConfig& limit_config);
    Status LoadModuleCustomConfig(const Config& config);
    Status UpdateModuleCustomConfig(const Config& config);

    void ReapMetrics();

 private:
    Status PrepareCmd(uint16_t module_id, uint64_t function_id);
    void ExecuteCmd(uint16_t module_id, uint64_t function_id,
                    const google::protobuf::Message* request, google::protobuf::Message* response,
                    CmdContext* cmd_ctx, Closure<void>* callback);

    Partition* partition_ = nullptr;
    ObjectManager* object_manager_ = nullptr;
    OpLogger* op_logger_ = nullptr;

    std::vector<std::unique_ptr<ModuleCustomConfig>> module_configs_;
    std::unique_ptr<QuotaManager> quota_manager_;
    CmdExecutorMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(CmdExecutor);
};

}  // namespace partition
}  // namespace bcache2
