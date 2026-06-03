// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/cmd_manager.h"
#include "common/metrics.h"

namespace bcache2 {
namespace client {

const char kMetricsCmdRequest[] = "cmd";
const char kMetricsMetaSyncerRequest[] = "getmeta";
const char kMetricsTopomUpdate[] = "topom.updated";
const char kMetricsBatchRequest[] = "batch";
const char kMetricsBatchSize[] = "batch.size";

struct CmdMetrics {
    std::vector<std::vector<std::unique_ptr<RequestMetrics>>> cmd_metrics;

    void Init(MetricsManager* metrics_manager) {
        auto& cmd_manager = CmdManager::Instance();
        const std::vector<CmdManager::ModuleInfo>& modules_info = cmd_manager.GetModuleInfos();
        for (size_t i = 0; i < modules_info.size(); i++) {
            const CmdManager::ModuleInfo& module_info = modules_info[i];
            if (Module_IsValid(i)) {
                if (i >= cmd_metrics.size()) {
                    cmd_metrics.resize(i + 1);
                }
                std::string module_name = module_info.name;
                for (size_t j = 0; j < module_info.cmd_executors.size(); j++) {
                    cmd_metrics[i].emplace_back(new RequestMetrics(
                        metrics_manager, kMetricsCmdRequest,
                        {{"module", module_name}, {"cmd", module_info.cmd_executors[j].name}}));
                }
            }
        }
    }
};

}  // namespace client
}  // namespace bcache2
