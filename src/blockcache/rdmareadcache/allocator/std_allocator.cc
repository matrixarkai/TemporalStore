// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/rdmareadcache/allocator/std_allocator.h"

namespace bcache2 {

char* StdAllocator::allocate(size_t sz) { return alloc_.allocate(sz); }

void StdAllocator::free(char* addr, size_t sz) { alloc_.deallocate(addr, sz); }

}  // namespace bcache2
