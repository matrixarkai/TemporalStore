// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <atomic>
#include <memory>
#include <set>
#include <unordered_set>
#include <vector>

#include "bthread/bthread.h"

#include "common/proto_enhance.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/raft_connector.h"

namespace bcache2 {
namespace metaserver {

class ProxyCalibrateRoutine {
 public:
    struct Options {
        Metabase* metabase{nullptr};
        RaftConnector* raft_connector{nullptr};
    };

 public:
    ProxyCalibrateRoutine() = default;
    ~ProxyCalibrateRoutine();

    Status Start(const Options& opts);
    void Stop();

    static void* RunRoutine(void* arg);

 private:
    void Routine();
    void Calibrate();

    size_t AcquireInstance(const ProxyGroupPtr& group, int delta);
    size_t ReleaseInstance(const ProxyGroupPtr& group, int delta);

    Status DetachProxy(const ProxyPtr& proxy);
    Status AttachProxy(const ProxyPtr& proxy, const ProxyGroupInfo& info);

 private:
    std::atomic<bool> running_{false};
    Metabase* metabase_{nullptr};
    RaftConnector* raft_connector_{nullptr};
    bthread_t routine_thd_;
};

}  // namespace metaserver
}  // namespace bcache2

