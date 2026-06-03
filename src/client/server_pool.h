// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "brpc/channel.h"
#include "butil/fast_rand.h"

#include "common/logging.h"
#include "common/status.h"
#include "protocol/master.pb.h"
#include "protocol/server.pb.h"

namespace bcache2 {
namespace client {

class BackendServerPool {
 public:
    enum class ServerType {
        kBCache2,
        kRedis,
        kMaster,
    };

    struct Options {
        ServerType type = ServerType::kBCache2;
        brpc::ChannelOptions channel_options;
    };

    struct ChannelContext {
        std::atomic<int64_t> first_failed_time_ms{0};
        std::atomic<int64_t> failed_counter{0};
        brpc::Channel channel;
    };

    BackendServerPool() {}
    virtual ~BackendServerPool() {}

    void Init(const Options& options) { options_ = options; }

    std::shared_ptr<ChannelContext> GetServer(const std::string& endpoint) const {
        auto iter = server_pool_.find(endpoint);
        if (iter == server_pool_.end()) {
            return nullptr;
        }
        return iter->second;
    }

    std::shared_ptr<ChannelContext> GetServerByRR() {
        if (server_pool_.size() < 1) {
            LOG_WARNING("Backend server empty").put("Type", static_cast<int>(options_.type));
            return nullptr;
        }
        round_robin_index_ = (round_robin_index_ + 1) % server_pool_.size();
        auto random_it =
            std::next(std::begin(server_pool_),
                      round_robin_index_);  // TODO(zkwu): this is linear time complexity
        return random_it->second;
    }

    std::shared_ptr<ChannelContext> GetServerByRandom() {
        const size_t size = server_pool_.size();
        if (size < 1) {
            return nullptr;
        }
        int idx = butil::fast_rand() % size;
        return std::next(std::begin(server_pool_), idx)->second;
    }

    Status AddServer(const std::string& endpoint) {
        if (GetServer(endpoint) != nullptr) {
            LOG_DEBUG("Backend server already exists")
                .put("Type", static_cast<int>(options_.type))
                .put("Endpoint", endpoint);
            return Status::AlreadyExists("Backend server already exists");
        }

        std::shared_ptr<ChannelContext> channel_context;
        channel_context.reset(new ChannelContext());
        if (channel_context->channel.Init(endpoint.c_str(), &options_.channel_options) != 0) {
            LOG_WARNING("Backend server init failed").put("endpoint", endpoint);
            return Status::Internal("Backend server init failed: channel init error");
        }
        server_pool_.insert(std::make_pair(endpoint, std::move(channel_context)));

        return Status::OK();
    }

    Status AddServers(const std::set<std::string>& endpoints) {
        for (const auto& endpoint : endpoints) {
            Status status = AddServer(endpoint);
            if (!status.ok() && !status.IsAlreadyExists()) {
                return status;
            }
        }
        return Status::OK();
    }

    // cleanup endpoints not in the parame
    void CleanupPool(const std::set<std::string>& endpoints) {
        for (auto iter = server_pool_.begin(); iter != server_pool_.end();) {
            if (endpoints.find(iter->first) != endpoints.end()) {
                ++iter;
                continue;
            }
            iter = server_pool_.erase(iter);
        }
    }

    Status ResetPoolOptions(const Options& options) {
        options_ = options;

        for (auto iter = server_pool_.begin(); iter != server_pool_.end(); ++iter) {
            auto channel_context = std::make_shared<ChannelContext>();
            if (channel_context->channel.Init(iter->first.c_str(), &options_.channel_options) !=
                0) {
                LOG_WARNING("Backend server init failed").put("endpoint", iter->first);
                return Status::Internal("Backend server init failed: channel init error");
            }
            iter->second = std::move(channel_context);
        }
        return Status::OK();
    }

 private:
    Options options_;
    int round_robin_index_ = 0;

    // will be used with brpc DoublyBufferedData, unique_ptr not supported
    // TODO(zhangfucheng.0): use unique_ptr
    std::unordered_map<std::string, std::shared_ptr<ChannelContext>> server_pool_;
};

}  // namespace client
}  // namespace bcache2
