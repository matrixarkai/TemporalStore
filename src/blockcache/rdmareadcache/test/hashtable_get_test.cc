// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <gtest/gtest.h>

#include "blockcache/rdmareadcache/index/hash_table.h"

/*
    This test is for testing that a hash table Get() operation
    will always return a pointer that points the data block
    in the storage engine, its length and storage type

    If a key is inserted, Get() should return the associated value (i.e., data block address),
    the data block length and storage type
    If a key is updated, Get() should return new values
    If a key does not exist, Get() should return nullptr
*/

class HashTableReadTest : public testing::Test {
 public:
    void SetUp() override { ht = new bcache2::HashTable<int, char*>(); }
    void TearDown() override { delete (ht); }

 protected:
    bcache2::HashTable<int, char*>* ht;
};

TEST_F(HashTableReadTest, ReadTest) {
    /*
        emulate how storage engine allocates a data block and store kv pairs.
        assuming K and V are integers
        CRC is ignored for UT
    */
    int key = 1;
    uint64_t data_size = sizeof(int) + sizeof(int);
    char* val_1 = static_cast<char*>(malloc(data_size + DATA_HEADER + CRC_LEN));
    *reinterpret_cast<uint64_t*>(val_1) = sizeof(int);
    *reinterpret_cast<uint64_t*>((val_1 + 8)) = sizeof(int);
    *reinterpret_cast<int*>((val_1 + 16)) = key;
    *reinterpret_cast<int*>(val_1 + 20) = key;

    size_t len = 0;
    uint8_t type;
    char* old_addr = nullptr;
    size_t old_sz = 0;
    uint8_t old_type = 3;

    ASSERT_EQ(ht->Put(key, val_1, data_size, bcache2::StorageEngineType::DRAM, &old_addr, &old_sz,
                      &old_type),
              OP_SUCCESS);
    // test a GET operation can read the correct ptr to the data block stored in storate engine
    ASSERT_EQ(ht->Get(key, &len, &type), val_1);
    // test a GET operation can read the correct length of the data block
    ASSERT_EQ(len, data_size + DATA_HEADER + CRC_LEN);
    // test a GET operation can read the correct storage type of the data block
    ASSERT_EQ(type, static_cast<uint8_t>(bcache2::StorageEngineType::DRAM));

    char* val_2 = static_cast<char*>(malloc(data_size + DATA_HEADER + CRC_LEN));
    *reinterpret_cast<uint64_t*>(val_2) = sizeof(int);
    *reinterpret_cast<uint64_t*>(val_2 + 8) = sizeof(int);
    *reinterpret_cast<int*>(val_2 + 16) = key;
    *reinterpret_cast<int*>(val_2 + 20) = key;
    len = 0;
    ASSERT_EQ(ht->Put(key, val_2, data_size, bcache2::StorageEngineType::DRAM, &old_addr, &old_sz,
                      &old_type),
              OP_SUCCESS);
    // if the data block is replaced by a new block, a GET operation should
    // read the ptr to the new data block
    ASSERT_EQ(ht->Get(key, &len, &type), val_2);
    ASSERT_EQ(len, data_size + DATA_HEADER + CRC_LEN);
    ASSERT_EQ(type, static_cast<uint8_t>(bcache2::StorageEngineType::DRAM));
    // old address, old size and old type should be filled if update-in-place happens for a PUT
    // operation
    ASSERT_EQ(old_addr, val_1);
    ASSERT_EQ(old_sz, data_size + DATA_HEADER + CRC_LEN);
    ASSERT_EQ(old_type, static_cast<uint8_t>(bcache2::StorageEngineType::DRAM));

    key = 2;
    len = 0;
    // GET() should return nullptr if key does not exist
    ASSERT_EQ(ht->Get(key, &len, &type), nullptr);
    ASSERT_EQ(len, 0ul);
    ASSERT_EQ(type, static_cast<uint8_t>(bcache2::StorageEngineType::INVALID));

    free(val_1);
    free(val_2);
}
