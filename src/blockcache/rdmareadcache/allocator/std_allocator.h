// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

#include "blockcache/rdmareadcache/allocator/cache_allocator.h"

namespace bcache2 {

class StdAllocator : public CacheAllocator {
    std::allocator<char> alloc_;

 public:
    StdAllocator() {}
    virtual ~StdAllocator() {}

    char* allocate(size_t sz) override;
    void free(char* addr, size_t sz) override;
};
}  // namespace bcache2
