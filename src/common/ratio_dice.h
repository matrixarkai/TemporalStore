// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/assert.h>
#include <byte/include/macros.h>

#include <algorithm>
#include <random>
#include <utility>
#include <vector>

template <typename T>
class RatioDice {
 public:
    RatioDice() : rng_(dev_()) {}
    ~RatioDice() {}

    void AddProperty(uint64_t ratio, T property) {
        BYTE_ASSERT(ratio > 0);
        total_ratio_ += ratio;
        properties_.emplace_back(std::make_pair(ratio, property));
        std::sort(properties_.begin(), properties_.end(),
                  [](const std::pair<uint64_t, T>& lhs, const std::pair<uint64_t, T>& rhs) {
                      return lhs.first < rhs.first;
                  });
    }

    T Roll() const {
        BYTE_ASSERT(total_ratio_ > 0);
        std::uniform_int_distribution<uint64_t> letter_dist(1, total_ratio_);
        uint64_t dice = letter_dist(rng_);

        uint64_t current_ratio = 1;
        for (const auto& property : properties_) {
            if (dice >= current_ratio && dice < current_ratio + property.first) {
                return property.second;
            }
            current_ratio += property.first;
        }

        BYTE_ASSERT(false) << "bug";
        return T();
    }

 private:
    mutable std::random_device dev_;
    mutable std::mt19937 rng_;

    std::vector<std::pair<uint64_t, T>> properties_;
    uint64_t total_ratio_ = 0;

    // 这里有什么disallow copy的必要？
    DISALLOW_COPY_AND_ASSIGN(RatioDice);
};
