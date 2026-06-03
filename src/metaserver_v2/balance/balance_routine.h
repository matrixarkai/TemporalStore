// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

#include "bthread/bthread.h"

#include "common/proto_enhance.h"
#include "metaserver_v2/meta/metabase.h"
#include "metaserver_v2/scheduler/scheduler_manager.h"

namespace bcache2 {
namespace metaserver {

class BalanceRoutine {
 public:
    struct Options {
        Metabase* metabase{nullptr};
        SchedulerManager* scheduler_manager{nullptr};
    };

 public:
    BalanceRoutine();
    ~BalanceRoutine();

    Status Start(const Options& opts);
    void Stop();

    static void* RunRoutine(void* arg);

 private:
    void Routine();
    void BalanceByTable();

 private:
    std::atomic<bool> running_{false};
    bthread_t routine_thd_;

    Metabase* metabase_{nullptr};
    SchedulerManager* scheduler_mgr_{nullptr};
};

}  // namespace metaserver
}  // namespace bcache2

