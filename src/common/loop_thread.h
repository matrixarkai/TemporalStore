// Copyright (c) 2020-present, ByteDance Inc. All rights reserved.
#pragma once

#include <stdlib.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include "common/status.h"

namespace bcache2 {

//// LoopThread is from ByteBase(Abase2)
class LoopThread {
 public:
    LoopThread() = default;
    virtual ~LoopThread() = default;
    Status Start();
    void Stop();

 private:
    void Run();
    virtual void DoLoop() = 0;
    virtual uint64_t LoopIntervalMs() = 0;

    bool is_stop_ = true;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cond_;
};

}  // namespace bcache2
