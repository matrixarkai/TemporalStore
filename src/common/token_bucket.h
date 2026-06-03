// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <chrono>
#include <memory>
#include <thread>

#include "butil/time.h"

namespace bcache2 {

// Not Thread Safe
class SimpleTokenBucket {
 public:
    SimpleTokenBucket(uint64_t rate, uint64_t burst);
    ~SimpleTokenBucket() = default;

    uint64_t Rate() { return rate_; }
    uint64_t Burst() { return burst_; }

    static void PosixUSleep(uint64_t us) {
        std::this_thread::sleep_for(std::chrono::microseconds(us));
    }

    template <void SleepUS(uint64_t) = SimpleTokenBucket::PosixUSleep>
    void Consume(uint64_t tokens, uint64_t now = butil::cpuwide_time_ns()) {
        uint64_t new_time = CalcFutureTime(tokens, now);
        if (new_time > now) {
            SleepUS((new_time - now) / 1000);
        }

        last_time_ = new_time;
    }

    bool ConsumeWithoutWait(uint64_t tokens, uint64_t now = butil::cpuwide_time_ns());

    uint64_t GetLeftToken(uint64_t now = butil::cpuwide_time_ns());

 private:
    uint64_t CalcFutureTime(uint64_t tokens, uint64_t now);

 private:
    const uint64_t rate_ = 0;
    const uint64_t burst_ = 0;

    uint64_t last_time_{0};
    uint64_t time_per_token_{0};  // unit: NanoSecond/token
    uint64_t time_per_burst_{0};  // unit: NanoSecond/token
};
}  // namespace bcache2
