// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <byte/byte_log/byte_log_impl.h>
#include <byte/include/assert.h>
#include <byte/thread/async_thread.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <stdio.h>

#include "common/coclosure.h"
#include "common/fiu_local.h"
#include "common/metrics.h"
#include "server/server.h"

void CoroutineMain(int argc, char** argv, int* result, bcache2::CoSyncClosure* callback) {
    testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    BYTE_ASSERT(bcache2::IsCoContext());
    *result = RUN_ALL_TESTS();
    callback->Run();
}

void CleanUpThread(bcache2::CoSyncClosure* sync) {
    coroutine::Destroy();
    if (sync) {
        sync->Run();
    }
}

GTEST_API_ int main(int argc, char** argv) {
    printf("Running main() from %s\n", __FILE__);

    if (fiu_init(0)) {
        return -1;
    }

    byte::SetByteLogDir("./");
    byte::SetByteLogMaxFileNum(10);
    byte::SetByteLogMaxFileSize(1UL << 30);
    byte::SetMinLogLevel(byte::LOG_LEVEL_ALL);

    bcache2::MetricsEnv metrics_env;
    bcache2::MetricsEnv::Options metrics_env_option;
    metrics_env_option.prefix = "bcache2.test";
    metrics_env.Init(metrics_env_option);

    byte::AsyncThreadPool pool;
    byte::AsyncThreadPoolOptions options;
    BYTE_ASSERT(pool.Init(options));
    BYTE_ASSERT(pool.Start());
    byte::AsyncThread* thread = pool.KthThread(0);
    bcache2::CoSyncClosure sync;
    int result = 0;
    thread->Invoke(NewCoClosure(&CoroutineMain, argc, argv, &result, &sync));
    sync.Wait();

    for (int i = 0; i < pool.ThreadNum(); ++i) {
        bcache2::CoSyncClosure sync;
        pool.KthThread(i)->Invoke(NewClosure(&CleanUpThread, &sync));
        sync.Wait();
    }
    metrics_env.Stop();
    pool.Stop();
    CleanUpThread(nullptr);
    return result;
}
