// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/blockcache.h"

#include <byte/base/closure.h>
#include <gflags/gflags.h>
#include <google/protobuf/util/message_differencer.h>
#include <gtest/gtest.h>

#include "common/coclosure.h"
#include "common/controller.h"

using bcache2::partition::PageInfo;

namespace bcache2 {
namespace blockcache {

class BlockCacheTest : public testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {
        LOG_DEBUG("BlockCacheTest TearDown called.");
        auto stop_status = blockcache_->Stop();
        blockcache_.reset();
    }

 protected:
    std::unique_ptr<BlockCache> blockcache_;
};

TEST_F(BlockCacheTest, SimplePutGet) {
    blockcache_ = std::make_unique<BlockCache>();
    auto start_status = blockcache_->Start();
    ASSERT_TRUE(start_status.ok());
    std::string key = "abc";
    std::string data = "123123";
    std::string value = std::string(data);
    auto put_status = blockcache_->Put(key, value);
    ASSERT_TRUE(put_status.ok());
    std::string new_data;
    auto get_status = blockcache_->Get(key, &new_data);
    ASSERT_TRUE(get_status.ok()) << get_status;
    ASSERT_EQ(data, new_data);
}

TEST_F(BlockCacheTest, GetPutNonUninitialized) {
    blockcache_ = std::make_unique<BlockCache>();
    std::string key = "abc";
    std::string data = "123123";
    std::string value = std::string(data);
    auto put_status = blockcache_->Put(key, value);
    ASSERT_FALSE(put_status.ok());
    ASSERT_EQ(put_status.errorcode(), Code::kUnavailable);
    std::string new_data;
    auto get_status = blockcache_->Get(key, &new_data);
    ASSERT_FALSE(get_status.ok());
    ASSERT_EQ(get_status.errorcode(), Code::kUnavailable);
}

TEST_F(BlockCacheTest, GetNonExist) {
    blockcache_ = std::make_unique<BlockCache>();
    auto start_status = blockcache_->Start();
    ASSERT_TRUE(start_status.ok());
    std::string key = "abc";
    std::string data;
    auto status = blockcache_->Get(key, &data);
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.errorcode(), Code::kNotFound);
}

TEST_F(BlockCacheTest, CacheEvictionVerify) {
    // Only use DRAM
    FLAGS_blockcache_dram_capacity = 100;
    FLAGS_blockcache_pmem_capacity = 0;
    FLAGS_blockcache_ssd_capacity = 0;
    FLAGS_blockcache_dram_replacement_policy = "FIFO";
    blockcache_ = std::make_unique<BlockCache>();
    auto start_status = blockcache_->Start();
    ASSERT_TRUE(start_status.ok());

    std::string data = std::string(64, 'a');
    // Insert one key and check if it exist
    {
        std::string key = "1111";
        auto put_status = blockcache_->Put(key, data);
        std::string get_data;
        auto status = blockcache_->Get(key, &get_data);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(data, get_data);
    }

    // Insert 10 KVs to reach capacity
    for (int i = 0; i < 10; i++) {
        std::string address = std::to_string(i * 1111);
        blockcache_->Put(address, data);
    }

    // The first key should be evicted.
    std::shared_ptr<PageInfo> first_page(new PageInfo);
    std::string first = "0";
    std::string first_data;
    auto first_status = blockcache_->Get(first, &first_data);
    ASSERT_FALSE(first_status.ok());
    ASSERT_EQ(first_status.errorcode(), Code::kNotFound);

    // The last key should still exist.
    std::string last = "9999";
    std::string last_data;
    auto last_status = blockcache_->Get(last, &last_data);
    ASSERT_TRUE(last_status.ok());
    ASSERT_EQ(data, last_data);
    FLAGS_blockcache_dram_capacity = 32LLU * 1024 * 1024 * 1024;
    FLAGS_blockcache_dram_replacement_policy = "SLRU";
}

}  // namespace blockcache
}  // namespace bcache2
