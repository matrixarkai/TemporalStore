// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "blockcache/blockcache_impl.h"

#include <unified_cache.h>

#include <filesystem>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "protocol/storage.pb.h"

using bcache2::partition::PageInfo;

namespace bcache2 {
namespace blockcache {

BlockCacheImpl::BlockCacheImpl() {}

BlockCacheImpl::~BlockCacheImpl() {
    if (initialized_) {
        Stop();
    }
}

void BlockCacheImpl::Init(const CacheOptions& options) {
    mtcache::CacheOptions opts{
        .dram_capacity = options.dram_capacity,
        .pmem_capacity = options.pmem_capacity,
        .ssd_capacity = options.ssd_capacity,
        .pmem_paths = options.pmem_paths,
        .ssd_paths = options.ssd_paths,
        .cache_dram_replacement_policy = options.cache_dram_replacement_policy,
        .cache_pmem_replacement_policy = options.cache_pmem_replacement_policy,
        .cache_ssd_replacement_policy = options.cache_ssd_replacement_policy,
        .cache_dram_pmem_data_placement_type = options.cache_dram_pmem_data_placement_type,
        .cache_dram_pmem_data_placement_threshold =
            options.cache_dram_pmem_data_placement_threshold,
        .metric_id_prefix = options.cache_metric_id_prefix,
        .metric_registry_tags = options.cache_registry_tags,
        .cache_ssd_instance_only = options.cache_ssd_instance_only};
    unified_cache_.reset(new mtcache::UnifiedCache(opts));
    ssd_paths_ = opts.ssd_paths;
    blockcache_clear_ssd_folder_ = options.blockcache_clear_ssd_folder;
}

bcache2::Status BlockCacheImpl::Start() {
    // Clear data for abnormal exit as well
    if (blockcache_clear_ssd_folder_) {
        for (auto path : ssd_paths_) {
            std::filesystem::remove_all(path);
        }
    }
    auto res = unified_cache_->Start();
    if (!res) {
        return bcache2::Status::Unknown("Unified cache start failed!");
    }
    initialized_ = true;
    return bcache2::Status::OK();
}

bcache2::Status BlockCacheImpl::Stop() {
    if (!initialized_) {
        LOG_WARNING("BlockCacheImpl uninitialized.");
        return bcache2::Status::Unavailable("BlockCacheImpl not running.");
    }
    auto res = unified_cache_->Stop();
    if (!res) {
        return bcache2::Status::Unknown("Unified cache stop failed!");
    }
    if (blockcache_clear_ssd_folder_) {
        for (auto path : ssd_paths_) {
            std::filesystem::remove_all(path);
        }
    }
    initialized_ = false;
    return bcache2::Status::OK();
}

bcache2::Status BlockCacheImpl::Get(const std::string& key, PageInfo* page_info) {
    if (!initialized_) {
        LOG_WARNING("BlockCacheImpl uninitialized.");
        return bcache2::Status::Unavailable("BlockCacheImpl not running.");
    }
    LOG_CALL_DEBUG()
        .put("ObjectId", static_cast<uint16_t>(page_info->header.object_id()))
        .put("PageId", page_info->header.page_id())
        .put("Address", page_info->address)
        .put("Size", page_info->size)
        .put("Data", page_info->data);
    auto handle = unified_cache_->Acquire(key);
    if (handle == nullptr) {
        LOG_DEBUG("BlockCacheImpl Get miss").put("key: ", key).put("Size: ", page_info->size);
        return bcache2::Status::NotFound("Page not found in blockcache.");
    } else {
        auto data = reinterpret_cast<const char*>(handle->value().data());
        auto size = handle->value().length();
        auto header_size = *reinterpret_cast<const uint16_t*>(&data[0]);
        LOG_CALL_DEBUG().put("buffer: ", std::string(data, size)).put("header size:", header_size);
        if (size < sizeof(uint16_t) + header_size) {
            LOG_ERROR("Blockcache entry size too short")
                .put("Size", size)
                .put("HeaderSize", header_size);
            unified_cache_->Release(handle);
            unified_cache_->Remove(key);
            return bcache2::Status::NotFound("Page corrupted in blockcache.");
        }
        storage::PageHeader header;
        if (!header.ParseFromArray(&data[sizeof(uint16_t)], header_size)) {
            LOG_ERROR("Blockcache parse header failed")
                .put("Size", size)
                .put("HeaderSize", header_size);
            unified_cache_->Release(handle);
            unified_cache_->Remove(key);
            return bcache2::Status::NotFound("Page corrupted in blockcache.");
        }
        uint32_t actual_page_size = sizeof(uint16_t) + header_size + header.data_size();
        if (actual_page_size != size) {
            LOG_ERROR("Blockcache check page_size failed")
                .put("Size", size)
                .put("HeaderSize", header_size);
            unified_cache_->Release(handle);
            unified_cache_->Remove(key);
            return bcache2::Status::NotFound("Page corrupted in blockcache.");
        }
        page_info->header = std::move(header);
        // TODO(xiao.liu): might use further optimization with zero copy APIs
        page_info->data =
            std::string(&data[sizeof(uint16_t) + header_size], page_info->header.data_size());
    }
    unified_cache_->Release(handle);
    return bcache2::Status::OK();
}

bcache2::Status BlockCacheImpl::Put(const std::string& key, PageInfo* page_info) {
    LOG_CALL_DEBUG()
        .put("ObjectId", static_cast<uint16_t>(page_info->header.object_id()))
        .put("PageId", page_info->header.page_id())
        .put("Address", page_info->address)
        .put("Size", page_info->size)
        .put("Data", page_info->data);
    if (!initialized_) {
        LOG_WARNING("BlockCacheImpl uninitialized.");
        return bcache2::Status::Unavailable("BlockCacheImpl not running.");
    }
    BYTE_ASSERT(page_info->header.data_size() > 0) << page_info->header.ShortDebugString();
    // TODO(xiao.liu): needs further optimization, but oplogger and page_store parse page header in
    // a different way.
    std::string buffer;
    buffer.resize(sizeof(uint16_t) + page_info->header.ByteSize());
    *reinterpret_cast<uint16_t*>(&buffer[0]) = page_info->header.ByteSize();
    BYTE_ASSERT(page_info->header.SerializeToArray(&buffer[0] + sizeof(uint16_t),
                                                   page_info->header.ByteSize()));
    buffer = buffer + page_info->data;
    auto iobuf = folly::IOBuf::copyBuffer(buffer.c_str(), buffer.size());

    unified_cache_->Insert(key, *iobuf, buffer.size());
    LOG_DEBUG("BlockCacheImpl Put done").put("key:", key).put("Size: ", page_info->size);
    return bcache2::Status::OK();
}

bcache2::Status BlockCacheImpl::Get(const std::string& key, std::string* value) {
    if (!initialized_) {
        LOG_WARNING("BlockCacheImpl uninitialized.");
        return bcache2::Status::Unavailable("BlockCacheImpl not running.");
    }
    LOG_CALL_DEBUG().put("Key", key);
    auto handle = unified_cache_->Acquire(key);
    if (handle == nullptr) {
        LOG_DEBUG("BlockCacheImpl Get miss").put("key: ", key);
        return bcache2::Status::NotFound("Page not found in blockcache.");
    }
    value->assign(reinterpret_cast<const char*>(handle->value().data()), handle->value().length());
    unified_cache_->Release(handle);
    LOG_DEBUG("BlockCacheImpl Get hit").put("key: ", key).put("Size: ", value->size());
    return bcache2::Status::OK();
}

bcache2::Status BlockCacheImpl::Put(const std::string& key, const std::string& value) {
    LOG_CALL_DEBUG().put("Key", key).put("Value size", value.length());
    if (!initialized_) {
        LOG_WARNING("BlockCacheImpl uninitialized.");
        return bcache2::Status::Unavailable("BlockCacheImpl not running.");
    }
    auto iobuf = folly::IOBuf::copyBuffer(value.c_str(), value.size());
    unified_cache_->Insert(key, *iobuf, value.size());
    return bcache2::Status::OK();
}

}  // namespace blockcache
}  // namespace bcache2
