// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <ctime>

namespace bcache2 {
namespace ips {
class TimeCost {
 public:
    TimeCost() { start_ = std::chrono::steady_clock::now(); }

    int GetElapsed() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now - start_).count();
    }

    void Reset() { start_ = std::chrono::steady_clock::now(); }

 private:
    std::chrono::steady_clock::time_point start_;
};
}  // namespace ips
}  // namespace bcache2
