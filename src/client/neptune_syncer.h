// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <bthread/timer_thread.h>
#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>

#include <cstdlib>

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <map>
#include <random>

#include "common/status.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "client/client_impl.h"
#include "client/client.h"
#include "common/ratio_dice.h"
#include "common/consul.h"

namespace bcache2 {
namespace client {

class NeptuneSyncer {
 public:
    struct Options {
        int64_t timer_interval_ms = 1000 * 60;
    };

    explicit NeptuneSyncer(const Options& options,
        const ClientOptions* client_opts)
        : options_(options),
        client_opts_(client_opts) {
        drop_pct_ = 0;
    }
    virtual ~NeptuneSyncer() {}

    Status Init();
    std::string RefreshNeptuneServer();
    std::string next(const std::string& caller_dc) {
        butil::DoublyBufferedData<std::map<std::string,
            std::unique_ptr<RatioDice<std::string>>,
            IgnoreCaseLess>>::ScopedPtr dc_weight_ptr;
        int rc = dc_weight_.Read(&dc_weight_ptr);
        if (rc != 0) {
            LOG_WARNING("Read dc_weight_ failed").put("Caller DC", caller_dc);
            return caller_dc;
        }
        const auto& dc_weight_map = *dc_weight_ptr;
        auto iter = dc_weight_map.find(caller_dc);
        if (iter != dc_weight_map.end()) {
            return iter->second->Roll();
        } else {
            return caller_dc;
        }
    }

    uint64_t Get_Drop_PCT() {
        return drop_pct_;
    }
    struct IgnoreCaseLess {
    bool operator()(const std::string& str1, const std::string& str2) const {
        return std::lexicographical_compare(
            str1.begin(), str1.end(),
            str2.begin(), str2.end(),
            [](char c1, char c2) { return std::tolower(c1) < std::tolower(c2); });
      }
    };

 private:
    void NeptuneSyncSchedule();
    void Refresh(const std::string& addr);
    bool check_table_valid(const rapidjson::Value& data);

    Options options_;

    bthread::TimerThread timer_thread_;
    service_discovery::Consul consul_;
    std::string neptune_consul_name_ = "toutiao.mesh.cp_http_cache";
    std::string neptune_consul_cluster_ = "bcache";
    std::string neptune_consul_default_cluster_ = "default";

    const ClientOptions* client_opts_;

    butil::DoublyBufferedData<std::map<std::string,
        std::unique_ptr<RatioDice<std::string>>,
        IgnoreCaseLess>> dc_weight_;

    uint64_t drop_pct_;

    DISALLOW_COPY_AND_ASSIGN(NeptuneSyncer);
};

}  // namespace client
}  // namespace bcache2
