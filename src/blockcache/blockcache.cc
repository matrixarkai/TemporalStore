// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "blockcache/blockcache.h"

#include <absl/strings/str_split.h>

#include <map>
#include <string>
#include <vector>

namespace bcache2 {
namespace blockcache {

static std::string GetCacheMetricIdPrefix() {
    std::string cache_metric_id_prefix = FLAGS_blockcache_metric_id_prefix;
    if (!cache_metric_id_prefix.empty()) {
        cache_metric_id_prefix += ".unifiedcache";
    } else {
        cache_metric_id_prefix = "unifiedcache";
    }
    return cache_metric_id_prefix;
}

static std::map<std::string, std::string> GetCacheMetricTags() {
    std::vector<std::string> tag_keys = absl::StrSplit(FLAGS_blockcache_metric_tag_keys, ';');
    std::vector<std::string> tag_values = absl::StrSplit(FLAGS_blockcache_metric_tag_values, ';');
    std::map<std::string, std::string> tags;
    if (tag_keys.size() != tag_values.size()) {
        return tags;
    }
    for (size_t i = 0; i < tag_keys.size(); i++) {
        tags[tag_keys[i]] = tag_values[i];
    }
    return tags;
}

BlockCache::BlockCache() {
    std::vector<std::string> pmem_paths = absl::StrSplit(FLAGS_blockcache_pmem_path, ';');
    std::vector<std::string> ssd_paths = absl::StrSplit(FLAGS_blockcache_ssd_path, ';');
    CacheOptions options{
        .dram_capacity = FLAGS_blockcache_dram_capacity,
        .pmem_capacity = FLAGS_blockcache_pmem_capacity,
        .ssd_capacity = FLAGS_blockcache_ssd_capacity,
        .pmem_paths = pmem_paths,
        .ssd_paths = ssd_paths,
        .cache_dram_replacement_policy = FLAGS_blockcache_dram_replacement_policy,
        .cache_pmem_replacement_policy = FLAGS_blockcache_pmem_replacement_policy,
        .cache_ssd_replacement_policy = FLAGS_blockcache_ssd_replacement_policy,
        .cache_dram_pmem_data_placement_type = FLAGS_blockcache_dram_pmem_data_placement_type,
        .cache_dram_pmem_data_placement_threshold =
            FLAGS_blockcache_dram_pmem_data_placement_threshold,
        .cache_metric_id_prefix = GetCacheMetricIdPrefix(),
        .cache_registry_tags = GetCacheMetricTags(),
        .cache_ssd_instance_only = FLAGS_blockcache_ssd_instance_only,
        .enable_cache_metrics = FLAGS_blockcache_enable_metrics,
        .blockcache_clear_ssd_folder = FLAGS_blockcache_clear_ssd_folder};
    Init(options);
}

}  // namespace blockcache
}  // namespace bcache2
