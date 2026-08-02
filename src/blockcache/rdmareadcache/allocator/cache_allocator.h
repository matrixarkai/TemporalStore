// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

namespace bcache2 {

class CacheAllocator {
 public:
    CacheAllocator() = default;
    virtual ~CacheAllocator() {}

    virtual char* allocate(size_t sz) = 0;
    virtual void free(char* addr, size_t sz) = 0;
};

}  // namespace bcache2
