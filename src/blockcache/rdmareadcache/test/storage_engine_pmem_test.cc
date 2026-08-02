// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/rdmareadcache/storage/storage_engine_pmem.h"

#include <gtest/gtest.h>

/*
    This test aims at testing that the PMEM storage engine can
    correctly Put, Get and Delele a given key value pair
*/

class StorageEnginePmemTest : public testing::Test {
 public:
    void SetUp() override {
        storage_engine_ = new bcache2::StorageEnginePmem<int, int>(bcache2::AllocatorType::Standard,
                                                                   storage_engine_cap);
    }
    void TearDown() override { delete (storage_engine_); }

 protected:
    size_t storage_engine_cap = 1024 * 1024;
    bcache2::StorageEnginePmem<int, int>* storage_engine_;
};

TEST_F(StorageEnginePmemTest, InsertTest) {
    // Put operation should OP_SUCCESS and return a pointer that points
    // to the inserted kv block
    char* addr_1 = storage_engine_->Put(1, 1);
    ASSERT_NE(addr_1, nullptr);

    // Get operation to an existed key should return OP_SUCCESS
    // the corresponding value should be stored in the resp field
    bcache2::RDMAResponse resp;
    size_t sz = DATA_HEADER + 2 * sizeof(int) + CRC_LEN;
    int return_code = storage_engine_->Get(1, sz, &resp, addr_1);
    ASSERT_EQ(return_code, OP_SUCCESS);
    ASSERT_EQ(*reinterpret_cast<int*>(resp.GetResponse()), 1);
    resp.Clear();

    // Get should return CRC_MISMATCH since the value does not match
    ASSERT_EQ(storage_engine_->Get(2, sz, &resp, addr_1), NOT_FOUND);
    resp.Clear();

    // Get should return CRC_MISMATCH since the data block size is wrong,
    // the resulting data block CRC is different from the stored CRC
    size_t wrong_sz = 2 * sizeof(int);
    ASSERT_EQ(storage_engine_->Get(1, wrong_sz, &resp, addr_1), CRC_MISMATCH);

    // Del operation to an existed key should return OP_SUCCESS.
    // We do not test deleting a non-existent key because
    // whether a key exists or not is checked by the index
    // engine. If a key exists, kv block address returned by
    // the index engine will be passed to the storage engine
    // for deletion; Otherwise, the index engine returns NOT_FOUND.
    // Therefore, storage engine just frees the space pointed by the
    // addr parameter without checking if the address is valid or not
    ASSERT_EQ(storage_engine_->Del(addr_1, sz), OP_SUCCESS);
}
