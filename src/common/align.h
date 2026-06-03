// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <inttypes.h>
#include <stddef.h>

namespace bcache2 {

template <typename T>
inline T* Align(size_t align, T* ptr) {
    const auto intptr = reinterpret_cast<uintptr_t>(ptr);
    const auto aligned = (intptr + align - 1u) & -align;
    return reinterpret_cast<T*>(aligned);
}

}  // namespace bcache2
