// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <gtest/gtest.h>

#include "blockcache/rdmareadcache/index/hash_table.h"

/*
    This test is used for testing that a key value pair will be
    inserted into the appropriate bucket and entry

    When a bucket has available empty slots, entries will be inserted directly.

    When a key already exists, the old value will be replaced by the new value
    if they are different

    When a bucket is full, an entry will be evicted to make room for the
    to-be-inserted KV pair, now we implement a random eviction policy, which will
    be replaced later
*/

class HashTableInsertTest : public testing::Test {
 public:
    void SetUp() override { ht = new bcache2::HashTable<int, char*>(); }
    void TearDown() override { delete (ht); }

 protected:
    bcache2::HashTable<int, char*>* ht;
};

TEST_F(HashTableInsertTest, InsertTest) {
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
    ASSERT_EQ(ht->GetBucket(1)->GetOccupiedEntryNum(), 1ul);
    ASSERT_EQ(ht->GetNumEntries(), 1ul);

    key = 2;
    ASSERT_EQ(ht->Put(key, val_1, data_size, bcache2::StorageEngineType::DRAM, &old_addr, &old_sz,
                      &old_type),
              OP_SUCCESS);
    ASSERT_EQ(ht->GetBucket(2)->GetOccupiedEntryNum(), 1ul);
    ASSERT_EQ(ht->GetNumEntries(), 2ul);

    key = 3;
    ASSERT_EQ(ht->Put(key, val_1, data_size, bcache2::StorageEngineType::DRAM, &old_addr, &old_sz,
                      &old_type),
              OP_SUCCESS);
    ASSERT_EQ(ht->GetBucket(3)->GetOccupiedEntryNum(), 1ul);
    ASSERT_EQ(ht->GetNumEntries(), 3ul);

    key = 4;
    ASSERT_EQ(ht->Put(key, val_1, data_size, bcache2::StorageEngineType::DRAM, &old_addr, &old_sz,
                      &old_type),
              OP_SUCCESS);
    ASSERT_EQ(ht->GetBucket(4)->GetOccupiedEntryNum(), 1ul);
    ASSERT_EQ(ht->GetNumEntries(), 4ul);

    key = 5;
    ASSERT_EQ(ht->Put(key, val_1, data_size, bcache2::StorageEngineType::DRAM, &old_addr, &old_sz,
                      &old_type),
              OP_SUCCESS);
    ASSERT_EQ(ht->GetBucket(5)->GetOccupiedEntryNum(), 1ul);
    ASSERT_EQ(ht->GetNumEntries(), 5ul);

    key = BUCKET_NUM + 1;
    ASSERT_EQ(ht->Put(key, val_1, data_size, bcache2::StorageEngineType::DRAM, &old_addr, &old_sz,
                      &old_type),
              OP_SUCCESS);
    ASSERT_EQ(ht->GetBucket(1)->GetOccupiedEntryNum(), 2ul);
    ASSERT_EQ(ht->GetNumEntries(), 6ul);

    // in-place update test
    key = 1;
    char* val_2 = static_cast<char*>(malloc(data_size + DATA_HEADER + CRC_LEN));
    *reinterpret_cast<uint64_t*>(val_2) = sizeof(int);
    *reinterpret_cast<uint64_t*>((val_2 + 8)) = sizeof(int);
    *reinterpret_cast<int*>((val_2 + 16)) = key;
    *reinterpret_cast<int*>(val_2 + 20) = key;
    ASSERT_EQ(ht->Put(key, val_2, data_size, bcache2::StorageEngineType::DRAM, &old_addr, &old_sz,
                      &old_type),
              OP_SUCCESS);
    ASSERT_EQ(ht->GetBucket(1)->GetOccupiedEntryNum(), 2ul);
    ASSERT_EQ(ht->GetNumEntries(), 6ul);

    // bucket overflow test
    for (size_t i = 1; i < 32; ++i) {
        ASSERT_EQ(ht->Put((BUCKET_NUM * i + 6), val_2, data_size, bcache2::StorageEngineType::DRAM,
                          &old_addr, &old_sz, &old_type),
                  OP_SUCCESS);
        if (i < 16) {
            ASSERT_EQ(ht->GetBucket(6)->GetOccupiedEntryNum(), i);
            ASSERT_EQ(ht->GetNumEntries(), (i + 6));
        } else {
            ASSERT_EQ(ht->GetBucket(6)->GetOccupiedEntryNum(), 15ul);
            ASSERT_EQ(ht->GetNumEntries(), 21ul);
        }
    }

    free(val_1);
    free(val_2);
}
