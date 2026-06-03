// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>

#include "byte/algorithm/crc64.h"

namespace bcache2 {

inline uint64_t CallHash(const char* data, size_t size) {
    return byte::crc64_signed(0, data, size);
}

inline uint64_t CallHash(const std::string& str) { return CallHash(str.data(), str.size()); }

typedef uint64_t(HashFunc)(const char* data, size_t size);
extern HashFunc* hash_func;

inline void SetHashFunc(HashFunc* func) { hash_func = func; }

}  // namespace bcache2
