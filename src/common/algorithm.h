// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <stdint.h>

namespace bcache2 {

inline uint64_t NextPowerOfTwo(uint64_t value) {
    --value;
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    value |= value >> 32;
    return ++value;
}

inline uint32_t NextPowerOfTwo(uint32_t value) {
    --value;
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    return ++value;
}

}  // namespace bcache2
