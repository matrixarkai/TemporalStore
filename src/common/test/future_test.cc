// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/future.h"

#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <gtest/gtest.h>

#include <thread>  // NOLINT(build/c++11)

namespace bcache2 {

TEST(FutureTest, WaitAfterRun) {
    Future<int> future;
    future.Post(10);
    future.Run();
    int re = future.Get();
    ASSERT_EQ(10, re);
}

TEST(FutureTest, RunAfterWait) {
    Future<int> future;
    std::thread t([&future] {
        sleep(5);
        future.Post(100);
        future.Run();
    });
    int re = future.Get();
    ASSERT_EQ(100, re);
    t.join();
}

}  // namespace bcache2
