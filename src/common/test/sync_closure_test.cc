// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/sync_closure.h"

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gtest/gtest.h>

#include <thread>  // NOLINT(build/c++11)

namespace bcache2 {

TEST(SyncClosureTest, WaitAfterRun) {
    SyncClosure sync;
    sync.Run();
    sync.Wait();
}

TEST(SyncClosureTest, RunAfterWait) {
    SyncClosure sync;
    std::thread t([&sync] {
        sleep(5);
        sync.Run();
    });
    sync.Wait();
    t.join();
}

}  // namespace bcache2
