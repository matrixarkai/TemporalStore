// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include "blockcache/rdmareadcache/common/macro.h"

/* persistent functions used on PMEM only */
inline void clwb(void* p) { asm volatile("clwb (%0)" ::"r"(p)); }
inline void sfence() { asm volatile("sfence"); }
inline void flush(char* addr, size_t sz) {
    for (char* curr = addr;
         curr <= reinterpret_cast<char*>(((reinterpret_cast<size_t>(addr) + sz) | CACHE_LINE_MASK));
         curr += CACHE_LINE_SIZE) {
        clwb(curr);
    }
}
