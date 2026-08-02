// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once
#include <memory>
#include <string>

#include "blockcache/rdmareadcache/allocator/cache_allocator.h"
#include "blockcache/rdmareadcache/allocator/std_allocator.h"
#include "blockcache/rdmareadcache/common/macro.h"
#include "blockcache/rdmareadcache/common/response.h"
#include "blockcache/rdmareadcache/index/utils.h"
#include "blockcache/rdmareadcache/replacement_policy/replacement_policy_fifo.h"
#include "blockcache/rdmareadcache/replacement_policy/replacement_policy_lru.h"

#define FAIL_ALLOC -1
#define CRC_MISMATCH -2

namespace bcache2 {

enum class AllocatorType : uint8_t { Standard = 0, LogBasedAlloc = 1, PoolBasedAlloc = 2 };
enum class StorageEngineType : uint8_t { DRAM = 0, PMEM = 1, SSD = 2, INVALID = 3 };

template <typename K, typename V>
class StorageEngine {
 public:
    StorageEngine() = default;
    virtual ~StorageEngine() {}

    /**
     * @brief get the value associated with query key, store the value (excluding key, header and
     * CRC) in the resp field
     *
     * @param key is the query key
     * @param sz is the size of the data block (including key, value, header and CRC) to be loaded
     * from the storage engine
     * @param resp is a buffer used for storing the value associated with the query key
     * @param addr is the address of the data block in the storage engine
     * @return int represents the status code of Get operation
     */
    virtual int Get(const K& key, size_t sz, RDMAResponse* resp, char* addr) = 0;

    /**
     * @brief put a key value pair into the storage engine
     *
     * @param key
     * @param val
     * @return int indicates the operation status
     */
    virtual char* Put(const K& key, const V& val) = 0;

    /**
     * @brief delete a data block (including a key, value, header and CRC) from the storage engine
     *
     * @param addr is the address of the data block in the storage engine
     * @return int indicates the operation results
     */
    virtual int Del(char* addr, size_t len) = 0;

    /**
     * @brief collect and report storage engine status (used space, free space, etc.,)
     */
    virtual void Stats() = 0;
};

template class StorageEngine<int, int>;
template class StorageEngine<std::string, std::string>;
}  // namespace bcache2
