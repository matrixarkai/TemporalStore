// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>

#include <memory>
#include <string>
#include <thread>

#include "brpc/channel.h"
#include "butil/endpoint.h"
#include "butil/time.h"

#include "common/metaserver_tracker.h"
#include "protocol/info.pb.h"

namespace bcache2 {
namespace proxy {

class Proxy;

class HeartBeat {
 public:
    HeartBeat(std::string cluster_name, Endpoint ep, Location loc,
              std::shared_ptr<MetaServerTracker> ms_tracker, Proxy* proxy);

    ~HeartBeat() {
        Stop();
        Join();
    }

    void Start();
    void Stop();
    void Join();

 private:
    void LoopWorker();
    void RegisterService();

    void SendHeartbeat();
    void InitHeartbeatRequest(metaserver::ProxyHeartbeatRequest* request);
    void HandleHeartbeatResponse(const metaserver::ProxyHeartbeatResponse& response);
    void SendStopSignal();

    void MaybeAutoRegister(Status status, const metaserver::ProxyHeartbeatResponse& response);
    void AutoRegisterInternal();

 private:
    std::thread loop_thread_;
    bool started_ = false;
    bool stopped_ = true;

    const std::string cluster_name_;
    const Endpoint self_endpoint_;
    const Location self_location_;
    const int64_t boot_time_us_{};

    std::shared_ptr<MetaServerTracker> ms_tracker_;
    Proxy* const proxy_{nullptr};

    bool registered_{false};
    int64_t last_register_timestamp_ms_{0};

    DISALLOW_COPY_AND_ASSIGN(HeartBeat);
};

}  // namespace proxy
}  // namespace bcache2
