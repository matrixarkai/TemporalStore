// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "brpc/channel.h"
#include "butil/endpoint.h"
#include "butil/strings/string_util.h"

#include "common/consul.h"
#include "common/loop_thread.h"
#include "common/status.h"
#include "protocol/metaserver.pb.h"

namespace bcache2 {

class MetaServerTracker : public LoopThread {
 public:
    explicit MetaServerTracker(std::string cluster_name);
    MetaServerTracker(std::string cluster_name, std::string uri);
    ~MetaServerTracker();

    Status Start();

    bool IsLeader(const butil::EndPoint& ep);
    std::vector<butil::EndPoint> GetEndpoints();
    Status GetLeaderEndpoint(butil::EndPoint* endpoint);

 private:
    void DoLoop() override;
    uint64_t LoopIntervalMs() override;
    Status TrackMetaServer(std::set<butil::EndPoint> endpoints);
    Status ParseUri(std::set<butil::EndPoint>* endpoints, bool strict_mode = false);
    Status QueryLeader(const butil::EndPoint& ep, butil::EndPoint* hint);

 private:
    const std::string cluster_name_;
    const std::string uri_;

    service_discovery::Consul sd_;

    std::mutex mu_;
    butil::EndPoint leader_;
    std::set<butil::EndPoint> endpoints_{};
};

}  // namespace bcache2

