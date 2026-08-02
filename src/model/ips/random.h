// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <cassert>
#include <random>

namespace bcache2 {
namespace ips {
class Rand {
 public:
    // [left, right]
    static int64_t randi64(int64_t left, int64_t right) {
        assert(left <= right);
        // safe to use [left, right]
        static thread_local std::mt19937_64 gen(std::random_device /* cpp lint hack */ {}());

        std::uniform_int_distribution<int64_t> dist(left, right);
        return dist(gen);
    }

    static int64_t randi64(int64_t right) { return randi64(1, right); }
};

}  // namespace ips
}  // namespace bcache2
