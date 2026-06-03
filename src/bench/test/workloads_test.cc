// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/workloads/workloads.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include "bench/workloads/common_workload.h"
#include "bench/workloads/hash_workload.h"
#include "bench/workloads/string_workload.h"
#include "bench/workloads/ips_workload.h"

namespace bcache2 {
namespace bench {
namespace test {

TEST(Workloads, CommonWorkloadDel) {
    CommonWorkload workloads;
    CommonWorkload::Options opts;
    opts.freq_del = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::COMMON);
    ASSERT_EQ(op.function_id(), common2::Function::DEL_OBJECT);
}

TEST(Workloads, CommonWorkloadExpire) {
    CommonWorkload workloads;
    CommonWorkload::Options opts;
    opts.freq_expire = 1;
    opts.expire_min_ttl_ms = 1000;
    opts.expire_max_ttl_ms = 1200;
    workloads.Init(opts);
    for (int i = 0; i < 1000; ++i) {
        Operation op = workloads.NextOperation("key", "value");
        ASSERT_EQ(op.module_id(), Module::COMMON);
        ASSERT_EQ(op.function_id(), common2::Function::EXPIRE);
        common2::ExpireRequest req;
        ASSERT_TRUE(req.ParseFromString(op.request_bytes()));
        ASSERT_TRUE(req.ttl_ms() >= 1000 && req.ttl_ms() <= 1200);
    }
}

TEST(Workloads, CommonWorkloadTtl) {
    CommonWorkload workloads;
    CommonWorkload::Options opts;
    opts.freq_ttl = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::COMMON);
    ASSERT_EQ(op.function_id(), common2::Function::TTL);
}

TEST(Workloads, StringWorkloadSet) {
    StringWorkload workloads;
    StringWorkload::Options opts;
    opts.freq_set = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::STRING);
    ASSERT_EQ(op.function_id(), str2::Function::SET);
}

TEST(Workloads, StringWorkloadSetex) {
    StringWorkload workloads;
    StringWorkload::Options opts;
    opts.freq_setex = 1;
    opts.setex_min_ttl_ms = 1000;
    opts.setex_max_ttl_ms = 1200;
    workloads.Init(opts);
    for (int i = 0; i < 1000; ++i) {
        Operation op = workloads.NextOperation("key", "value");
        ASSERT_EQ(op.module_id(), Module::STRING);
        ASSERT_EQ(op.function_id(), str2::Function::SETEX);
        str2::SetexRequest req;
        ASSERT_TRUE(req.ParseFromString(op.request_bytes()));
        ASSERT_TRUE(req.ttl_ms() >= 1000 && req.ttl_ms() <= 1200);
    }
}

TEST(Workloads, StringWorkloadGet) {
    StringWorkload workloads;
    StringWorkload::Options opts;
    opts.freq_get = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::STRING);
    ASSERT_EQ(op.function_id(), str2::Function::GET);
}

TEST(Workloads, HashWorkloadHGet) {
    HashWorkload workloads;
    HashWorkload::Options opts;
    opts.freq_hget = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::HASH);
    ASSERT_EQ(op.function_id(), hash2::Function::GET);
}

TEST(Workloads, HashWorkloadHGetAll) {
    HashWorkload workloads;
    HashWorkload::Options opts;
    opts.freq_hgetall = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::HASH);
    ASSERT_EQ(op.function_id(), hash2::Function::GETALL);
}

TEST(Workloads, HashWorkloadHSet) {
    HashWorkload workloads;
    HashWorkload::Options opts;
    opts.freq_hset = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::HASH);
    ASSERT_EQ(op.function_id(), hash2::Function::SET);
}

TEST(Workloads, HashWorkloadHDel) {
    HashWorkload workloads;
    HashWorkload::Options opts;
    opts.freq_hdel = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::HASH);
    ASSERT_EQ(op.function_id(), hash2::Function::DEL);
}

TEST(Workloads, IpsWorkloadAdd) {
    IpsWorkload workloads;
    IpsWorkload::Options opts;
    opts.freq_add = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::IPS);
    ASSERT_EQ(op.function_id(), ips::Function::ADD);
}

TEST(Workloads, IpsWorkloadQuery) {
    IpsWorkload workloads;
    IpsWorkload::Options opts;
    opts.freq_query = 1;
    workloads.Init(opts);
    Operation op = workloads.NextOperation("key", "value");
    ASSERT_EQ(op.module_id(), Module::IPS);
    ASSERT_EQ(op.function_id(), ips::Function::BATCH_QUERY);
}

TEST(Workloads, WorkloadsBunch) {
    WorkloadsBunch workloads;
    WorkloadsBunch::Options opts;
    opts.key_count = 10;
    opts.key_size = 128;
    opts.value_count = 20;
    opts.value_min_size = 128;
    opts.value_max_size = 256;
    workloads.Init(opts);
    ASSERT_EQ(workloads.random_raw_keys_.size(), 10);
    ASSERT_EQ(workloads.random_raw_keys_.front().size(), 128);
    ASSERT_EQ(workloads.random_values_.size(), 20);
    for (auto& value : workloads.random_values_) {
        ASSERT_TRUE(value.size() >= 128 && value.size() <= 256);
    }

    std::unique_ptr<StringWorkload> string_workloads(new StringWorkload());
    StringWorkload::Options string_opts;
    string_opts.freq_get = 1;
    string_workloads->Init(string_opts);
    workloads.RegisterWorkload(string_workloads.release(), 1);

    std::unique_ptr<HashWorkload> hash_workloads(new HashWorkload());
    HashWorkload::Options hash_opts;
    hash_opts.freq_hget = 1;
    hash_workloads->Init(hash_opts);
    workloads.RegisterWorkload(hash_workloads.release(), 1);

    int64_t hash_count = 0;
    int64_t string_count = 0;
    for (int i = 0; i < 10000; ++i) {
        Operation op = workloads.NextOperation();
        if (op.module_id() == Module::HASH) {
            ++hash_count;
        } else if (op.module_id() == Module::STRING) {
            ++string_count;
        } else {
            ASSERT_FALSE(true);
        }
    }

    ASSERT_LE(std::abs(hash_count - string_count), 500UL) << hash_count << "\n" << string_count;
}

}  // namespace test
}  // namespace bench
}  // namespace bcache2
