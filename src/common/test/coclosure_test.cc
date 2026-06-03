// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/coclosure.h"

#include <byte/base/closure.h>
#include <byte/include/assert.h>
#include <byte/include/macros.h>
#include <byte/thread/this_thread.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <thread>  // NOLINT(build/c++11)

#include "common/function_closure.h"
#include "common/time.h"

namespace bcache2 {

void CleanUpThread(bcache2::CoSyncClosure* sync) {
    coroutine::Destroy();
    if (sync) {
        sync->Run();
    }
}

void CleanUpThreadPool(byte::AsyncThreadPool* pool) {
    for (int i = 0; i < pool->ThreadNum(); ++i) {
        bcache2::CoSyncClosure sync;
        pool->KthThread(i)->Invoke(NewClosure(&CleanUpThread, &sync));
        sync.Wait();
    }
}

class TClosure {
 public:
    TClosure() : id(0) {}
    void Foo(bcache2::CoSyncClosure* sync) {
        id = byte::ThisThread::GetId();
        sync->Run();
    }
    int id;
};

class TAsyncWriteClosure {
 public:
    struct AsyncWriteArgs {
        CoSyncClosure* sync;
        // 其它需要从callback函数接收的字段
        int written;
    };

 public:
    explicit TAsyncWriteClosure(const char* str) : str(str), state(0) {}
    void Write(bcache2::CoSyncClosure* sync) {
        CoSyncClosure new_sync;
        AsyncWriteArgs args;
        args.written = 0;
        args.sync = &new_sync;
        async_write(str, TAsyncWriteClosure::callback, &args);
        new_sync.Wait();
        state = args.written;
        sync->Run();
    }
    static void async_write(const char* str, void (*callback)(int written, void* args),
                            void* args) {
        std::thread t([str, callback, args] {
            sleep(2);
            int written = strlen(str);
            callback(written, args);
        });
        t.detach();
    }
    static void callback(int written, void* args) {
        AsyncWriteArgs* asyncWriteArgs = static_cast<AsyncWriteArgs*>(args);
        asyncWriteArgs->written = written;
        asyncWriteArgs->sync->Run();
    }
    const char* str;
    int state;
};

TEST(CoClosureTest, Run) {
    TClosure closure;
    std::thread t([&closure] {
        byte::AsyncThreadPool pool;
        byte::AsyncThreadPoolOptions options;
        options.thread_num_ = 1;
        options.aio_enable_ = false;
        pool.Init(options);
        pool.Start();
        bcache2::CoSyncClosure sync;
        pool.KthThread(0)->Invoke(NewCoClosure(&closure, &TClosure::Foo, &sync));
        sync.Wait();
        CleanUpThreadPool(&pool);
        pool.Stop();
    });
    t.join();
    EXPECT_NE(0, closure.id);
}

TEST(CoClosureTest, Wait) {
    const char* str = "hello";
    TAsyncWriteClosure closure(str);
    std::thread t([&closure] {
        byte::AsyncThreadPool pool;
        byte::AsyncThreadPoolOptions options;
        options.thread_num_ = 1;
        options.aio_enable_ = false;
        pool.Init(options);
        pool.Start();
        bcache2::CoSyncClosure sync;
        pool.KthThread(0)->Invoke(NewCoClosure(&closure, &TAsyncWriteClosure::Write, &sync));
        sync.Wait();
        CleanUpThreadPool(&pool);
        pool.Stop();
    });
    t.join();
    EXPECT_EQ(static_cast<int>(strlen(str)), closure.state);
}

class CorutineTest : public testing::Test {
 public:
    void SetUp() override {
        byte::AsyncThreadPoolOptions options;
        ASSERT_TRUE(pool_.Init(options));
        ASSERT_TRUE(pool_.Start());
        thread_ = pool_.KthThread(0);
    }
    void TearDown() override {
        CleanUpThreadPool(&pool_);
        pool_.Stop();
    }

 protected:
    byte::AsyncThreadPool pool_;
    byte::AsyncThread* thread_ = nullptr;
};

TEST_F(CorutineTest, ThreadWaitBeforeSignal) {
    CoSyncClosure thread_sync;
    int value = 1;
    thread_->Invoke(NewCoFuncClosure([&] {
        value = 2;
        thread_sync.Run();
    }));
    ASSERT_EQ(1, value);
    thread_sync.Wait();
    ASSERT_EQ(2, value);
}

TEST_F(CorutineTest, ThreadWaitAfterSignal) {
    CoSyncClosure thread_sync;
    int value = 1;
    NewFuncClosure([&] {
        value = 2;
        thread_sync.Run();
    })->Run();
    ASSERT_EQ(2, value);
    thread_sync.Wait();
    ASSERT_EQ(2, value);
}

TEST_F(CorutineTest, CorutineWaitBeforeSignal) {
    CoSyncClosure thread_sync;
    thread_->Invoke(NewCoFuncClosure([&] {
        CoSyncClosure co_sync;
        int value = 1;
        byte::InvokeInCurrentThread(NewFuncClosure([&] {
            value = 2;
            co_sync.Run();
        }));
        ASSERT_EQ(1, value);
        co_sync.Wait();
        ASSERT_EQ(2, value);
        thread_sync.Run();
    }));
    thread_sync.Wait();
}

TEST_F(CorutineTest, CorutineWaitAfterSignal) {
    CoSyncClosure thread_sync;
    thread_->Invoke(NewCoFuncClosure([&] {
        CoSyncClosure co_sync;
        int value = 1;
        NewFuncClosure([&] {
            value = 2;
            co_sync.Run();
        })->Run();
        ASSERT_EQ(2, value);
        co_sync.Wait();
        ASSERT_EQ(2, value);
        thread_sync.Run();
    }));
    thread_sync.Wait();
}

TEST_F(CorutineTest, IsCoContext) {
    {
        CoSyncClosure thread_sync;
        thread_->Invoke(NewCoFuncClosure([&] {
            ASSERT_TRUE(IsCoContext());
            thread_sync.Run();
        }));
        thread_sync.Wait();
    }
    {
        CoSyncClosure thread_sync;
        thread_->Invoke(NewFuncClosure([&] {
            ASSERT_FALSE(IsCoContext());
            thread_sync.Run();
        }));
        thread_sync.Wait();
    }
}

TEST_F(CorutineTest, CoSleep) {
    uint64_t start = GetCurrentTimeInUs();
    CoSyncClosure thread_sync1;
    thread_->Invoke(NewCoFuncClosure([&] {
        CoSleep(2000000);  // sleep 2 seconds
        thread_sync1.Run();
    }));

    CoSyncClosure thread_sync2;
    thread_->Invoke(NewCoFuncClosure([&] {
        CoSleep(1000000);  // sleep 1 seconds
        thread_sync2.Run();
    }));

    thread_sync2.Wait();
    uint64_t time1 = GetCurrentTimeInUs();
    ASSERT_LT(500000, time1 - start);
    ASSERT_GT(1500000, time1 - start);

    thread_sync1.Wait();
    uint64_t time2 = GetCurrentTimeInUs();
    ASSERT_LT(1500000, time2 - start);
    ASSERT_GT(2500000, time2 - start);
}

TEST_F(CorutineTest, CoCountDownLatchInThread) {
    CoCountDownLatch latch(10);
    int value = 0;
    for (int i = 0; i < 10; ++i) {
        thread_->Invoke(NewCoFuncClosure([&] {
            value++;
            latch.CountDown();
        }));
    }
    latch.Wait();
    ASSERT_EQ(10, value);
}

TEST_F(CorutineTest, CoCountDownLatchInCoroutine) {
    CoSyncClosure thread_sync;
    thread_->Invoke(NewCoFuncClosure([&] {
        CoCountDownLatch latch(10);
        int value = 0;
        for (int i = 0; i < 10; ++i) {
            thread_->Invoke(NewFuncClosure([&] {
                value++;
                latch.CountDown();
            }));
        }
        latch.Wait();
        ASSERT_EQ(10, value);

        thread_sync.Run();
    }));
    thread_sync.Wait();
}

}  // namespace bcache2
