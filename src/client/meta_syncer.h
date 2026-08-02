// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <bthread/timer_thread.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "client/client_impl.h"
#include "common/consul.h"
#include "common/status.h"
#include "protocol/master.pb.h"

namespace bcache2 {
namespace client {

class MetaSyncer {
 public:
    struct Options {
        std::string host;
        std::string client_version;
        std::string idc;
        std::string consul;
        std::string endpoint;
        int64_t timer_interval_ms = 1000 * 60 * 10;
        int64_t standalone_interval_delta_ms = 1000 * 5;
        int max_redirect_times = 3;
        int64_t meta_fetch_timeout_ms = 1000;
    };

    explicit MetaSyncer(const Options& options) : options_(options) {}
    virtual ~MetaSyncer() { timer_thread_.stop_and_join(); }

    Status Init();
    Status OpenTable(TableCore* table);
    Status CloseTable(TableCore* table);
    Status StandaloneMode(TableCore* table);

 private:
    struct TableNode {
        TableCore* table = nullptr;
        std::string master_addr;
        GetTableTopoResponse topo;
        bool is_standalone = false;
        int64_t standalone_interval_ms = 0;
        int64_t last_update_time_ms = 0;
        std::unique_ptr<RequestMetrics> request_metrics;
        std::unique_ptr<MetricsEnv::CounterHolder> update_metrics;
    };

    void StandaloneSchedule(const std::string& table_name, int64_t interval_ms);
    void StandaloneMetaSync(const std::string& table_name);
    void MetaSyncSchedule();
    void RefreshMasterPool();
    Status UpdateTableMeta(TableNode* table_node, int redirect_times);
    Status HandleTableMetaResponse(TableNode* table_node, GetTableTopoResponse response);

    Options options_;

    bthread::Mutex mutex_;

    std::unordered_map<std::string, TableNode> tables_;
    std::unique_ptr<BackendServerPool> master_pool_;
    std::unique_ptr<MetricsManager> metrics_manager_;
    service_discovery::Consul consul_;
    bthread::TimerThread timer_thread_;

    DISALLOW_COPY_AND_ASSIGN(MetaSyncer);
};

}  // namespace client
}  // namespace bcache2
