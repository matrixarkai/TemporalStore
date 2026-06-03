// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <gtest/gtest.h>

#include "blockcache/rdmareadcache/index/hash_table.h"

/*
    This test is for verifying that the hash table is properly initialized
    Buckets, header and entries in each bucket should be zero-filled
*/

class HashTableInitTest : public testing::Test {
 public:
    void SetUp() override { ht = new bcache2::HashTable<int, char*>(); }
    void TearDown() override { delete (ht); }

 protected:
    bcache2::HashTable<int, char*>* ht;
};

TEST_F(HashTableInitTest, InitStatus) {
    // access the first bucket and verify that the content is zero-filled
    ASSERT_EQ(ht->GetSize(), TABLE_SIZE);
    ASSERT_NE(ht->GetBucket(0)->GetMetadata(), nullptr);
    ASSERT_NE(ht->GetBucket(0)->GetEntry(0), nullptr);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(0)->GetPtr(), nullptr);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(0)->GetCRC(), 0);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(0)->GetType(), DefaultDRAM);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(0)->GetOverflowFlag(), 0);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(0)->GetSignature128b().to_ulong(), 0ul);
    ASSERT_NE(ht->GetBucket(0)->GetEntry(BUCKET_CAP - 1), nullptr);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(BUCKET_CAP - 1)->GetPtr(), nullptr);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(BUCKET_CAP - 1)->GetCRC(), 0);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(BUCKET_CAP - 1)->GetType(), DefaultDRAM);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(BUCKET_CAP - 1)->GetOverflowFlag(), 0);
    ASSERT_EQ(ht->GetBucket(0)->GetEntry(BUCKET_CAP - 1)->GetSignature128b().to_ulong(), 0ul);
    ASSERT_EQ(ht->GetBucket(0)->GetEmptyEntry(), 0);

    // access the bucket in the middle and verify that the content is zero-filled
    ASSERT_NE(ht->GetBucket(BUCKET_NUM / 2)->GetMetadata(), nullptr);
    ASSERT_NE(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(0), nullptr);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(0)->GetPtr(), nullptr);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(0)->GetCRC(), 0);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(0)->GetType(), DefaultDRAM);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(0)->GetOverflowFlag(), 0);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(0)->GetSignature128b().to_ulong(), 0ul);
    ASSERT_NE(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(BUCKET_CAP - 1), nullptr);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(BUCKET_CAP - 1)->GetPtr(), nullptr);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(BUCKET_CAP - 1)->GetCRC(), 0);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(BUCKET_CAP - 1)->GetType(), DefaultDRAM);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEntry(BUCKET_CAP - 1)->GetOverflowFlag(), 0);
    ASSERT_EQ(
        ht->GetBucket(BUCKET_NUM / 2)->GetEntry(BUCKET_CAP - 1)->GetSignature128b().to_ulong(),
        0ul);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM / 2)->GetEmptyEntry(), 0);

    // access the last bucket and verify that the content is zero-filled
    ASSERT_NE(ht->GetBucket(BUCKET_NUM - 1)->GetMetadata(), nullptr);
    ASSERT_NE(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(0), nullptr);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(0)->GetPtr(), nullptr);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(0)->GetCRC(), 0);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(0)->GetType(), DefaultDRAM);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(0)->GetOverflowFlag(), 0);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(0)->GetSignature128b().to_ulong(), 0ul);
    ASSERT_NE(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(BUCKET_CAP - 1), nullptr);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(BUCKET_CAP - 1)->GetPtr(), nullptr);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(BUCKET_CAP - 1)->GetCRC(), 0);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(BUCKET_CAP - 1)->GetType(), DefaultDRAM);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEntry(BUCKET_CAP - 1)->GetOverflowFlag(), 0);
    ASSERT_EQ(
        ht->GetBucket(BUCKET_NUM - 1)->GetEntry(BUCKET_CAP - 1)->GetSignature128b().to_ulong(),
        0ul);
    ASSERT_EQ(ht->GetBucket(BUCKET_NUM - 1)->GetEmptyEntry(), 0);
}
