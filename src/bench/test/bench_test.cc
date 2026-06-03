// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "bench/bench.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "bench/client/bcache2_client.h"
#include "byte/algorithm/crc64.h"
#include "bench/client/brpc_client.h"
#include "bench/flags.h"
#include "bench/workloads/string_workload.h"
#include "bench/workloads/workloads.h"
#include "bvar/latency_recorder.h"
#include "bytestore/bytestore.h"
#include "common/controller.h"
#include "common/logging.h"
#include "common/partition_id_type.h"
#include "common/sync_closure.h"
#include "include/byte_log.h"
#include "protocol/bench.pb.h"
#include "test/common/bench.h"
#include "test/common/temp_dir.h"
#include "test/mini_cluster/mini_cluster.h"

namespace bcache2 {
namespace bench {
namespace test {

class BenchmarkTest : public ::testing::Test {
 public:
    BenchmarkTest() {}
    virtual ~BenchmarkTest() {}

    void SetUp() override {
        bytestore_init();
        // init bcache2 mini cluster
        MiniCluster::Options options;
        options.server_count = 1;
        options.cluster_uri = "file://" + temp_dir_.GetDir() + "/cluster/public";
        options.master_port = 0;
        options.work_dir = temp_dir_.GetDir();

        cluster_.Init(options);
        Status status = cluster_.Start();
        ASSERT_TRUE(status.ok()) << status;
        server_port_ = cluster_.GetFirstServer()->GetServerPort();

        auto master = cluster_.GetMaster();
        status = master->CreateSimpleTable(namespace_, table_name_);
        ASSERT_TRUE(status.ok()) << status;

        master_port_ = master->GetMasterPort();

        FLAGS_bench_run_time = 10;
    }
    void TearDown() override {
        cluster_.Stop();
        bytestore_shutdown();
    }

 protected:
    std::string namespace_ = "ut";
    std::string table_name_ = "test_table";
    uint32_t server_port_ = 0;
    uint32_t master_port_ = 0;

    TempDir temp_dir_;
    MiniCluster cluster_;

    DISALLOW_COPY_AND_ASSIGN(BenchmarkTest);
};

TEST_F(BenchmarkTest, Loop) {
    byte::SetMinLogLevel(byte::LOG_LEVEL_DEBUG);

    BCache2Client client;
    BCache2Client::Options client_opts;
    client_opts.table_uri =
        "tcp://127.0.0.1:" + std::to_string(master_port_) + "/" + namespace_ + "/" + table_name_;
    Status status = client.Init(client_opts);
    ASSERT_TRUE(status.ok()) << status;

    WorkloadsBunch workloads;
    WorkloadsBunch::Options workloads_opts;
    workloads_opts.key_count = 100;
    workloads_opts.key_size = 100;
    workloads_opts.value_count = 100;
    workloads_opts.value_min_size = 100;
    workloads_opts.value_max_size = 200;
    workloads.Init(workloads_opts);
    workloads.SetReusedKeys({"key1", "key2", "key3"});

    StringWorkload* string_workload = new StringWorkload();
    StringWorkload::Options string_opts;
    string_opts.freq_set = 1;
    string_workload->Init(string_opts);
    workloads.RegisterWorkload(string_workload, 1);

    Bench bench;
    Bench::Options opts;
    opts.client = &client;
    opts.workloads = &workloads;
    opts.jobs = 2;
    opts.depth = 5;
    opts.key_ttl_ms = 1000;
    opts.stay_operations = true;
    bench.Init(opts);
    bench.Start();
    for (size_t i = 0; i < 20; i++) {
        bench.PrintStats();
        sleep(1);
    }
    bench.Stop();
    ASSERT_GT(bench.ExtractOperations()[0].size(), 1000);
}

}  // namespace test
}  // namespace bench
}  // namespace bcache2
