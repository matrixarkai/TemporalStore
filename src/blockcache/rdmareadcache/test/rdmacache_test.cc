// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <gtest/gtest.h>

#include "blockcache/rdmareadcache/rdma_cache.h"

class RDMACacheTest : public testing::Test {
 public:
    void SetUp() override {
        rdma_cache = new bcache2::RDMACache<int, int>(
            dram_cap, dram_alloc_type, pmem_cap, pmem_alloc_type, ssd_cap, ssd_alloc_type, policy);
    }

    void TearDown() override { delete (rdma_cache); }

 protected:
    size_t dram_cap = 512 * 1024 * 1024;
    size_t pmem_cap = 0;
    size_t ssd_cap = 0;

    bcache2::AllocatorType dram_alloc_type = bcache2::AllocatorType::Standard;
    bcache2::AllocatorType pmem_alloc_type = bcache2::AllocatorType::Standard;
    bcache2::AllocatorType ssd_alloc_type = bcache2::AllocatorType::Standard;

    bcache2::ReplacementPolicyType policy = bcache2::ReplacementPolicyType::FIFO;

    bcache2::RDMACache<int, int>* rdma_cache;
};

TEST_F(RDMACacheTest, FuncTest) {
    bcache2::RDMAResponse resp;
    // Insert a non-existent key must succeed unless out-of-memory
    ASSERT_EQ(rdma_cache->Insert(1, 1), OP_SUCCESS);
    ASSERT_EQ(rdma_cache->Lookup(1, &resp), OP_SUCCESS);
    ASSERT_EQ(*reinterpret_cast<int*>(resp.GetResponse()), 1);
    resp.Clear();

    // Insert an existing key triggers an in-place update
    ASSERT_EQ(rdma_cache->Insert(1, 2), OP_SUCCESS);
    ASSERT_EQ(rdma_cache->Lookup(1, &resp), OP_SUCCESS);
    ASSERT_EQ(*reinterpret_cast<int*>(resp.GetResponse()), 2);
    resp.Clear();

    // Removing an existing key should succeed
    ASSERT_EQ(rdma_cache->Remove(1), OP_SUCCESS);

    // Lookup a deleted/non-existent key returns NOT_FOUND
    ASSERT_EQ(rdma_cache->Lookup(1, &resp), NOT_FOUND);
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
