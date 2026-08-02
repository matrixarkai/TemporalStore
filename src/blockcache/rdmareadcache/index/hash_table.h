// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <cassert>
#include <cstring>
#include <functional>
#include <thread>
#include <vector>

#include "blockcache/rdmareadcache/index/entry.h"

namespace bcache2 {

template <typename K, typename V>
class HashTable {
    Bucket<K, V>* Buckets;

 public:
    HashTable() {
        Buckets = (Bucket<K, V>*)malloc(TABLE_SIZE);
        for (uint64_t i = 0; i < BUCKET_NUM; ++i) {
            Buckets[i] = Bucket<K, V>();
        }
    }
    ~HashTable() { free(Buckets); }

    // main APIs
    /**
     * @brief given a key, return the address of the associated data block if the key exists,
     * otherwise return nullptr, it also return the size of the data block and the storage engine
     * type
     * @param key is the search key, sz stores the data block size in the storage engine, type
     * stores the corresponding storage engine type
     * @return the address of the corresponding data block
     */
    char* Get(const K& key, size_t* sz, uint8_t* type);

    /**
     * @brief store a key value pair index into the hash table (always in DRAM)
     * @param key is the key that user intends to insert
     * @param val is the value that user intends to insert
     * @param kv_size is the total size of key and value
     * @param type is the storage engine type
     * @param old_addr is the address of the old data (if put updates an existing key
     * block to points to a new value, we need to record the old address for memory reclamtion)
     * @param old_sz is the old data block size
     * @param old_type is the storage type where data block is stored
     * @return operation code that represents the operation status
     */
    int Put(const K& key, V val, size_t kv_size, StorageEngineType type, char** old_addr,
            size_t* old_sz, uint8_t* old_type);

    /**
     * @brief delete a key value pair index from the hash table if it exists
     * @param key is the to-be-deleted key
     * @param addr stores the data block address associated with this key in the storage engine
     * @param type stores the storage engine type
     * @return status code that represents the operation result
     */
    int Del(const K& key, char** addr, size_t* sz, uint8_t* type);

    /**
     * @brief resize the hash table when hash collision is too frequent
     *
     */
    void Resize() { /* TODO(mingzhe.du) unimplemented */
    }

    /**
     * @brief get the starting Addr of hash table for RDMA access
     *
     * @return the starting address of the hash table
     */
    char* GetStartingAddr();

    /**
     * @brief Get the i'th Bucket for RDMA access
     *
     * @param i represents the index of the target bucket
     * @return Bucket<K, V>* pointer
     */
    Bucket<K, V>* GetBucket(uint64_t i);

    // helper functions for checking the correctness of the hash table
    size_t GetSize();
    uint64_t GetNumEntries();
    bool AllBucketsUnlocked();
};
}  // namespace bcache2
