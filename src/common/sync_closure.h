// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/concurrent/cond.h>
#include <byte/concurrent/mutex.h>

namespace bcache2 {

class SyncClosure : public Closure<void> {
 public:
    SyncClosure() : cond_(&mutex_) {}
    void Run() override {
        byte::MutexLocker lock(&mutex_);
        if (!completed_) {
            completed_ = true;
            cond_.Broadcast();
        }
    }

    bool IsSelfDelete() const override { return false; }

    void Wait() {
        byte::MutexLocker lock(&mutex_);
        if (!completed_) {
            cond_.Wait();
        }
    }

 private:
    mutable byte::Mutex mutex_;
    byte::ConditionVariable cond_;
    bool completed_ = false;

    DISALLOW_COPY_AND_ASSIGN(SyncClosure);
};

}  // namespace bcache2
