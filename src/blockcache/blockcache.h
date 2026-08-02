// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

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
    Status Start() { return Status::OK(); }
    Status Stop() {
        std::lock_guard<std::mutex> lock(mu_);
        cache_.clear();
        return Status::OK();
    }
    Status Get(const std::string& key, partition::PageInfo* page_info) {
        return Status::Unimplemented("");
    }
    Status Put(const std::string& key, partition::PageInfo* page_info) {
        return Status::Unimplemented("");
    }
    Status Get(const std::string& key, std::string* value) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = cache_.find(key);
        if (it == cache_.end()) {
            return Status::NotFound("");
        }
        *value = it->second;
        return Status::OK();
    }
    Status Put(const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lock(mu_);
        cache_[key] = value;
        return Status::OK();
    }

 private:
    std::mutex mu_;
    std::unordered_map<std::string, std::string> cache_;
};

#endif

}  // namespace blockcache
}  // namespace bcache2
