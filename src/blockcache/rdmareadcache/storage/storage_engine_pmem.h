// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>

#include "blockcache/rdmareadcache/common/persist.h"
#include "blockcache/rdmareadcache/storage/storage_engine.h"

namespace bcache2 {

template <typename K, typename V>
class StorageEnginePmem : public StorageEngine<K, V> {
    AllocatorType alloc_type_;
    CacheAllocator* allocator_;

 public:
    StorageEnginePmem(AllocatorType type, size_t cap);
    virtual ~StorageEnginePmem();

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
    int Get(const K& key, size_t sz, RDMAResponse* resp, char* addr) override;

    /**
     * @brief put a key value pair into the storage engine
     *
     * @param key
     * @param val
     * @return int indicates the operation status
     */
    char* Put(const K& key, const V& val) override;

    /**
     * @brief delete a data block (including a key, value, header and CRC) from the storage engine
     *
     * @param addr is the address of the data block in the storage engine
     * @return int indicates the operation results
     */
    int Del(char* addr, size_t len) override;

    /**
     * @brief query the states of the storage engine
     *
     */
    void Stats() override;
};

}  // namespace bcache2
