// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <stdint.h>

namespace bcache2 {

inline bool IsBitSet(uint64_t bits, uint32_t bit_pos) {
    if (bit_pos >= 64) {
        return false;
    }
    return bits & (1UL << bit_pos);
}

inline uint64_t SetBit(uint64_t bits, uint32_t bit_pos) { return bits |= (1UL << bit_pos); }

}  // namespace bcache2
