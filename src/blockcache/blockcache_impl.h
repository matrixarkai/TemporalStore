// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "partition/index/index.h"

namespace mtcache {

class UnifiedCache;

}  // namespace mtcache

namespace bcache2 {

namespace partition {
class PageInfo;
}  // namespace partition

namespace blockcache {

struct CacheOptions {
    size_t dram_capacity{0};
    size_t pmem_capacity{0};
    size_t ssd_capacity{0};
    std::vector<std::string> pmem_paths;
    std::vector<std::string> ssd_paths;
    std::string cache_dram_replacement_policy;
    std::string cache_pmem_replacement_policy;
    std::string cache_ssd_replacement_policy;
    std::string cache_dram_pmem_data_placement_type;
    size_t cache_dram_pmem_data_placement_threshold;
    std::string cache_metric_id_prefix;
    std::map<std::string, std::string> cache_registry_tags;
    bool cache_ssd_instance_only;
    bool enable_cache_metrics;
    bool blockcache_clear_ssd_folder;
};

class BlockCacheImpl {
 public:
    BlockCacheImpl();
    ~BlockCacheImpl();
    void Init(const CacheOptions& options);
    bcache2::Status Start();
    bcache2::Status Stop();
    bcache2::Status Get(const std::string& key, partition::PageInfo* page_info);
    bcache2::Status Put(const std::string& key, partition::PageInfo* page_info);
    bcache2::Status Get(const std::string& key, std::string* value);
    bcache2::Status Put(const std::string& key, const std::string& value);

 private:
    bool initialized_{false};
    std::unique_ptr<mtcache::UnifiedCache> unified_cache_;
    std::vector<std::string> ssd_paths_;
    bool blockcache_clear_ssd_folder_{false};

    DISALLOW_COPY_AND_ASSIGN(BlockCacheImpl);
};

}  // namespace blockcache
}  // namespace bcache2
