// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/include/macros.h>
#include <byte/thread/async_thread.h>

namespace bcache2 {

class ScopedInvoker {
 public:
    explicit ScopedInvoker(Closure<void>* callback) : ScopedInvoker(callback, nullptr) {}
    ScopedInvoker(Closure<void>* callback, byte::AsyncThread* thread)
        : callback_(callback), thread_(thread) {}

    ~ScopedInvoker() {
        if (LIKELY(callback_ != nullptr)) {
            if (UNLIKELY(thread_ == nullptr)) {
                callback_->Run();
            } else {
                thread_->Invoke(callback_);
            }
        }
    }

    void Release() { callback_ = nullptr; }

 private:
    Closure<void>* callback_ = nullptr;
    byte::AsyncThread* thread_ = nullptr;
};

class ScopedCallback {
 public:
    explicit ScopedCallback(Closure<void>* callback) : callback_(callback) {}

    ~ScopedCallback() {
        if (LIKELY(callback_ != nullptr)) {
            callback_->Run();
        }
    }

    void Release() { callback_ = nullptr; }

 private:
    Closure<void>* callback_ = nullptr;
};

}  // namespace bcache2
