// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/concurrent/cond.h>
#include <byte/concurrent/mutex.h>
#include <byte/include/assert.h>
#include <byte/thread/async_thread.h>
#include "thirdparty/libco/coroutine.h"

#include <chrono>  // NOLINT(build/c++11)
#include <thread>  // NOLINT(build/c++11)

#include "common/function_closure.h"
#include "common/logging.h"

#define SYNC_CALL(method, ...)      \
    do {                            \
        CoSyncClosure sync;         \
        method(__VA_ARGS__, &sync); \
        sync.Wait();                \
    } while (false);

#define SYNC_CALL0(method)  \
    do {                    \
        CoSyncClosure sync; \
        method(&sync);      \
        sync.Wait();        \
    } while (false);

namespace bcache2 {

class CoClosure : public Closure<void> {
 public:
    explicit CoClosure(Closure<void>* closure) : closure_(closure) {}

    virtual void Run() {
        coroutine::Run(&CoroutineFunc, closure_, coroutine::Options());
        delete this;
    }

    virtual bool IsSelfDelete() const { return true; }

 private:
    static void CoroutineFunc(void* args) {
        Closure<void>* closure = static_cast<Closure<void>*>(args);
        closure->Run();
    }

    Closure<void>* closure_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(CoClosure);
};

template <typename... Args>
inline Closure<void>* NewCoClosure(Args... args) {
    return new CoClosure(NewClosure(args...));
}

template <typename... Args>
inline Closure<void>* NewCoFuncClosure(Args... args) {
    return new CoClosure(NewFuncClosure(args...));
}

inline bool IsCoContext() { return coroutine::Current() != nullptr; }

inline void CoSleep(uint64_t delay_us) {
    coroutine::Coroutine* corutine = coroutine::Current();
    if (LIKELY(corutine != nullptr)) {
        byte::InvokeLaterInCurrentThread(delay_us, NewClosure(&coroutine::Resume, corutine));
        coroutine::Yield();
    } else {
        std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
    }
}

class CoCountDownLatch {
 public:
    explicit CoCountDownLatch(int count)
        : thread_(byte::GetCurrentThread()),
          coroutine_(coroutine::Current()),
          cond_(&mutex_),
          count_(count) {
        LOG_DEBUG("CoCountDownLatch Construct").put("CoCountDownLatch", this).put("Count", count_);
    }
    ~CoCountDownLatch() {
        LOG_DEBUG("CoCountDownLatch Destruct").put("CoCountDownLatch", this).put("Count", count_);
    }

    void CountDown() {
        if (LIKELY(coroutine_ != nullptr)) {
            CoSignal();
        } else {
            ThreadSignal();
        }
    }

    void Wait() {
        if (LIKELY(coroutine_ != nullptr)) {
            CoWait();
        } else {
            ThreadWait();
        }
    }

 private:
    void CoWait() {
        LOG_DEBUG("CoCountDownLatch CoWait").put("CoCountDownLatch", this).put("Count", count_);
        BYTE_ASSERT(coroutine::Current() == coroutine_);
        if (LIKELY(count_ > 0)) {
            coroutine::Yield();
        }
    }

    void CoSignal() {
        LOG_DEBUG("CoCountDownLatch CoSignal").put("CoCountDownLatch", this).put("Count", count_);
        BYTE_ASSERT(thread_ != nullptr);
        if (byte::GetCurrentThread() != thread_) {
            thread_->Invoke(NewClosure(this, &CoCountDownLatch::CoSignal));
            return;
        }

        BYTE_ASSERT(count_ > 0);
        count_--;
        if (LIKELY(coroutine::Current() != coroutine_ && count_ == 0)) {
            coroutine::Resume(coroutine_);
        }
    }

    void ThreadWait() {
        byte::MutexLocker lock(&mutex_);
        if (count_ > 0) {
            cond_.Wait();
        }
    }

    void ThreadSignal() {
        byte::MutexLocker lock(&mutex_);
        BYTE_ASSERT(count_ > 0);
        count_--;
        if (count_ == 0) {
            cond_.Broadcast();
        }
    }

    byte::AsyncThread* thread_ = nullptr;
    coroutine::Coroutine* coroutine_ = nullptr;

    mutable byte::Mutex mutex_;
    byte::ConditionVariable cond_;

    int count_ = 0;

    DISALLOW_COPY_AND_ASSIGN(CoCountDownLatch);
};

class CoSyncClosure : public Closure<void> {
 public:
    CoSyncClosure() {}

    void Run() override { count_latch_.CountDown(); }

    void Wait() { count_latch_.Wait(); }

    bool IsSelfDelete() const override { return false; }

 private:
    CoCountDownLatch count_latch_{1};

    DISALLOW_COPY_AND_ASSIGN(CoSyncClosure);
};

}  // namespace bcache2
