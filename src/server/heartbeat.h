// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>
#include <string>

#include "common/loop_thread.h"
#include "common/metaserver_tracker.h"
#include "protocol/base.pb.h"
#include "protocol/host_spec.pb.h"

namespace bcache2 {
namespace server {

class PartitionManager;

class Heartbeat : public LoopThread {
 public:
    Heartbeat(std::string cluster_name, HostSpec host_spec,
              std::shared_ptr<MetaServerTracker> ms_tracker, PartitionManager* partition_manager);
    ~Heartbeat();

    void Stop();

 private:
    void DoLoop() override;
    uint64_t LoopIntervalMs() override;

    void SendHeartbeat();
    void InitHeartbeatRequest(metaserver::ServerHeartbeatRequest* request);
    void SendStopSignal();

    void MaybeAutoRegister(Status status, const metaserver::ServerHeartbeatResponse& response);
    Status TryToDropLegacyMe();
    void AutoRegisterInternal();

 private:
    const std::string cluster_name_;
    const HostSpec host_spec_;
    const int64_t boot_time_us_;
    std::shared_ptr<MetaServerTracker> ms_tracker_;

    int64_t fallback_end_timepoint_ms_{0};
    size_t round_{0};

    bool registered_{false};
    int64_t last_register_timestamp_ms_{0};

    uint64_t last_heartbeat_elapse_ms_{0};

    PartitionManager* partition_manager_{nullptr};
};

}  // namespace server
}  // namespace bcache2

