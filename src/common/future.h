// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/concurrent/cond.h>
#include <byte/concurrent/mutex.h>

namespace bcache2 {

template <typename T>
class Future : public Closure<void> {
 public:
    Future() : cond_(&mutex_) {}

    void Run() override {
        byte::MutexLocker lock(&mutex_);
        if (!completed_) {
            completed_ = true;
            cond_.Broadcast();
        }
    }

    bool IsSelfDelete() const override { return false; }

    void Post(const T& value) {
        byte::MutexLocker lock(&mutex_);
        value_ = value;
    }

    T Get() {
        byte::MutexLocker lock(&mutex_);
        if (!completed_) {
            cond_.Wait();
        }
        return value_;
    }

 private:
    mutable byte::Mutex mutex_;
    byte::ConditionVariable cond_;
    T value_ = T();
    bool completed_ = false;

    DISALLOW_COPY_AND_ASSIGN(Future);
};

}  // namespace bcache2
