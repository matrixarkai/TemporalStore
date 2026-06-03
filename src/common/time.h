// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <cstdint>
#include <ctime>

namespace bcache2 {

inline uint64_t GetCurrentTimeInNs() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000UL + ts.tv_nsec;
}

inline uint64_t GetCurrentTimeInUs() { return GetCurrentTimeInNs() / 1000UL; }

inline uint64_t GetCurrentTimeInMs() { return GetCurrentTimeInNs() / 1000000UL; }

inline uint32_t GetCurrentTimeInSec() { return GetCurrentTimeInNs() / 1000000000UL; }

// Measures the elapsed time
class TimeCost {
 public:
    TimeCost() { start_ = GetCurrentTimeInNs(); }

    uint64_t GetElapsedInNs() const {
        auto now = GetCurrentTimeInNs();
        return now - start_;
    }

    uint64_t GetElapsedInMs() const { return GetElapsedInNs() / 1000000UL; }

    uint64_t GetElapsedInUs() const { return GetElapsedInNs() / 1000UL; }

    void Reset() { start_ = GetCurrentTimeInNs(); }

 private:
    uint64_t start_ = 0;
};

}  // namespace bcache2
