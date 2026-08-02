// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <memory>

#include "blockcache/rdmareadcache/index/hash_table.h"
#include "blockcache/rdmareadcache/storage/storage_engine.h"
#include "blockcache/rdmareadcache/storage/storage_engine_dram.h"
#include "blockcache/rdmareadcache/storage/storage_engine_pmem.h"
#include "blockcache/rdmareadcache/storage/storage_engine_ssd.h"

namespace bcache2 {

/**
 * @brief TODO(mingzhe.du): different data migration policy
 */
enum class ReplacementPolicyType : uint8_t { FIFO = 0, LRU = 1, OTHER = 2 };

template <typename K, typename V>
class RDMACache {
    size_t dram_capacity_;
    size_t pmem_capacity_;
    size_t ssd_capacity_;

    AllocatorType dram_alloc_type_;
    AllocatorType pmem_alloc_type_;
    AllocatorType ssd_alloc_type_;

    HashTable<K, char*>* cache_index_ = nullptr;
    StorageEngineDram<K, V>* dram_engine_ = nullptr;
    StorageEnginePmem<K, V>* pmem_engine_ = nullptr;
    StorageEngineSSD<K, V>* ssd_engine_ = nullptr;

    ReplacementPolicyType policy_type_;
    ReplacementPolicy* replacement_policy_ = nullptr;

 public:
    RDMACache() {}
    RDMACache(size_t dram_cap, AllocatorType dram_alloc_type, size_t pmem_cap,
              AllocatorType pmem_alloc_type, size_t ssd_cap, AllocatorType ssd_alloc_type,
              ReplacementPolicyType replace_policy) {
        cache_index_ = new HashTable<K, char*>();
        if (dram_cap > 0) {
            InitStorageEngine(StorageEngineType::DRAM, dram_cap, dram_alloc_type);
        }
        if (pmem_cap > 0) {
            InitStorageEngine(StorageEngineType::PMEM, pmem_cap, pmem_alloc_type);
        }
        if (ssd_cap > 0) {
            InitStorageEngine(StorageEngineType::SSD, ssd_cap, ssd_alloc_type);
        }
        SetReplacementPolicy(replace_policy);
    }

    ~RDMACache() {
        if (cache_index_) delete (cache_index_);
        if (dram_engine_) delete (dram_engine_);
        if (pmem_engine_) delete (pmem_engine_);
        if (ssd_engine_) delete (ssd_engine_);
        if (replacement_policy_) delete (replacement_policy_);
    }

    /**
     * @brief search for the value associated with 'key' in the cache, value is stored in resp if
     * found
     *
     * @param key is the target key
     * @param resp stores the value associated with the key if found
     * @return status code indicating different operation results
     */
    int Lookup(const K& key, RDMAResponse* resp);

    /**
     * @brief insert a key value pair into the DRAM
     *
     * @param key is the target key
     * @param val is the target value
     * @return status code indicating different operation results
     */
    int Insert(const K& key, const V& val);

    /**
     * @brief insert a key value pair into the storage of type 'type'
     *
     * @param type indicate the storage engine type
     * @param key is the target key
     * @param val is the target value
     * @return status code indicating different operation results
     */
    int Insert(StorageEngineType type, const K& key, const V& val);

    /**
     * @brief remove a key value pair from a storage engine
     *
     * @param key is the target key
     * @return status code indicating different operation results
     */
    int Remove(const K& key);

    /**
     * @brief Get the Capacity of cache storage engine of type 'type'
     *
     * @param type is the  storage engine type
     * @return the capacity of the storage engine
     */
    size_t GetCapacity(StorageEngineType type);

    /**
     * @brief Set the Capacity of storage engine of the cache with type 'type'
     *
     * @param type is the storage engine type
     * @param capacity is the desired capacity
     */
    void InitStorageEngine(StorageEngineType type, size_t capacity, AllocatorType alloc_type);

    /**
     * @brief Get the Replacement Policy Type object
     *
     * @return ReplacementPolicyType
     */
    ReplacementPolicyType GetReplacementPolicyType();

    /**
     * @brief Set the Replacement Policy Type object
     */
    void SetReplacementPolicy(ReplacementPolicyType policy);
};
}  // namespace bcache2
