// Copyright (c) 2020-present, ByteDance Inc. All rights reserved.
#include "common/loop_thread.h"

#include <unistd.h>

#include <chrono>
#include <memory>

namespace bcache2 {

Status LoopThread::Start() {
    std::unique_lock<std::mutex> lock_guard(mutex_);
    if (!is_stop_) {
        return Status::Internal("already running");
    }

    is_stop_ = false;
    thread_ = std::thread(&LoopThread::Run, this);
    return Status::OK();
}

void LoopThread::Stop() {
    std::unique_lock<std::mutex> lock_guard(mutex_);
    if (!is_stop_) {
        is_stop_ = true;
        lock_guard.unlock();
        cond_.notify_one();
        thread_.join();
    }
}

void LoopThread::Run() {
    std::unique_lock<std::mutex> lock_guard(mutex_);
    while (!is_stop_) {
        DoLoop();
        cond_.wait_for(lock_guard, std::chrono::milliseconds(LoopIntervalMs()),
                       [this]() { return is_stop_; });
    }
}

}  // namespace bcache2
