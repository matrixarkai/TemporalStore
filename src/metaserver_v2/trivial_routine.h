// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include "common/consul.h"
#include "common/loop_thread.h"
#include "metaserver_v2/raft_server.h"

namespace bcache2 {
namespace metaserver {

class TrivialRoutine : public LoopThread {
 public:
    explicit TrivialRoutine(RaftServer* raft_server);
    ~TrivialRoutine() { Stop(); }

 private:
    void DoLoop() override;
    uint64_t LoopIntervalMs() override { return kLoopIntervalMs; }

    void ConsulAnnounce();
    void MaybeTriggerSnapshot();

 private:
    static constexpr uint64_t kLoopIntervalMs = 5'000;

    RaftServer* const raft_server_{nullptr};
    service_discovery::Consul sd_;

    int64_t last_snapshot_timestamp_{0};
    uint64_t last_snapshot_index_{0};  // not accurate
};

}  // namespace metaserver
}  // namespace bcache2

