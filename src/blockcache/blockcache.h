// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>

#include "blockcache/blockcache_impl.h"
#include "blockcache/flags.h"

namespace bcache2 {
namespace blockcache {

#ifdef ENABLE_MTCACHE

class BlockCache : public BlockCacheImpl {
 public:
    BlockCache();
};

#else

class BlockCache {
 public:
    Status Start() { return Status::Unimplemented(""); }
    Status Stop() { return Status::Unimplemented(""); }
    Status Get(const std::string& key, partition::PageInfo* page_info) {
        return Status::Unimplemented("");
    }
    Status Put(const std::string& key, partition::PageInfo* page_info) {
        return Status::Unimplemented("");
    }
    Status Get(const std::string& key, std::string* value) { return Status::Unimplemented(""); }
    Status Put(const std::string& key, const std::string& value) {
        return Status::Unimplemented("");
    }
};

#endif

}  // namespace blockcache
}  // namespace bcache2
