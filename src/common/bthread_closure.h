// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include "bthread/countdown_event.h"
#include "common/function_closure.h"

#define BTHREAD_SYNC_CALL(method, ...) \
    do {                               \
        BthreadSyncClosure sync;       \
        method(__VA_ARGS__, &sync);    \
        sync.Wait();                   \
    } while (false);

namespace bcache2 {

class BthreadSyncClosure : public Closure<void> {
 public:
    BthreadSyncClosure() {}

    void Run() override { count_event_.signal(); }

    void Wait() { count_event_.wait(); }

    bool IsSelfDelete() const override { return false; }

 private:
    bthread::CountdownEvent count_event_{1};

    DISALLOW_COPY_AND_ASSIGN(BthreadSyncClosure);
};

}  // namespace bcache2
