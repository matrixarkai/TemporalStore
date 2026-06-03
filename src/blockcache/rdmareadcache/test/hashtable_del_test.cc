// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <gtest/gtest.h>

#include "blockcache/rdmareadcache/index/hash_table.h"

/*
    This test is for testing that a key will be deleted if it exists,
    otherwise NOT_FOUND will return

    Every delete operation should return the corresponding data block address,
    length, and the storage type so that rdma cache system knows where to
    reclaim the deleted data block
*/

class HashTableDelTest : public testing::Test {
 public:
    void SetUp() override { ht = new bcache2::HashTable<int, char*>(); }
    void TearDown() override { delete (ht); }

 protected:
    bcache2::HashTable<int, char*>* ht;
};

TEST_F(HashTableDelTest, DeleteTest) {
    ASSERT_EQ(ht->GetNumEntries(), 0ul);

    int key = 1;
    uint64_t data_size = sizeof(int) + sizeof(int);
    char* val_1 = static_cast<char*>(malloc(data_size + DATA_HEADER + CRC_LEN));
    *reinterpret_cast<uint64_t*>(val_1) = sizeof(int);
    *reinterpret_cast<uint64_t*>((val_1 + 8)) = sizeof(int);
    *reinterpret_cast<int*>((val_1 + 16)) = key;
    *reinterpret_cast<int*>(val_1 + 20) = key;
    char* old_addr = nullptr;
    size_t old_sz = 0;
    uint8_t old_type = 3;
    ASSERT_EQ(ht->Put(key, val_1, data_size, bcache2::StorageEngineType::DRAM, &old_addr, &old_sz,
                      &old_type),
              OP_SUCCESS);

    // delete an existing key should be a OP_SUCCESS operation,
    // and return the data block address, length and type
    char* addr = nullptr;
    uint64_t size = 0;
    uint8_t type = static_cast<uint8_t>(bcache2::StorageEngineType::INVALID);
    ASSERT_EQ(ht->Del(1, &addr, &size, &type), OP_SUCCESS);
    ASSERT_EQ(addr, val_1);
    ASSERT_EQ(size, data_size + DATA_HEADER + CRC_LEN);
    ASSERT_EQ(type, static_cast<uint8_t>(bcache2::StorageEngineType::DRAM));
    ASSERT_EQ(ht->GetBucket(1)->GetOccupiedEntryNum(), 0ul);
    ASSERT_EQ(ht->GetNumEntries(), 0ul);

    // delete a non-existent key returns NOT_FOUND
    addr = nullptr;
    size = 0;
    type = static_cast<uint8_t>(bcache2::StorageEngineType::INVALID);
    ASSERT_EQ(ht->Del(1, &addr, &size, &type), NOT_FOUND);
    ASSERT_EQ(addr, nullptr);
    ASSERT_EQ(size, 0ul);
    ASSERT_EQ(type, static_cast<uint8_t>(bcache2::StorageEngineType::INVALID));
    // delete a non-existent key does not cause underflow
    ASSERT_EQ(ht->GetBucket(1)->GetOccupiedEntryNum(), 0ul);
    ASSERT_EQ(ht->GetNumEntries(), 0ul);

    free(val_1);
}
