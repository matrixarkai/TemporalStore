// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <cassert>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <vector>

#include "bench/bench.h"
#include "bench/client/bcache2_client.h"
#include "bench/client/brpc_client.h"
#include "bench/client/thrift_client.h"
#include "bench/consistency_checker.h"
#include "bench/flags.h"
#include "bench/service.h"
#include "bench/workloads/common_workload.h"
#include "bench/workloads/hash_workload.h"
#include "bench/workloads/string_workload.h"
#include "bench/workloads/ips_workload.h"
#include "brpc/server.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/time.h"

namespace bench = bcache2::bench;

using Status = bcache2::Status;

std::atomic<bool> g_stop_flag;
void SignalHandler(int /*signal*/) { g_stop_flag.store(true); }

std::unique_ptr<bench::WorkloadsBunch> InitWorkloads() {
    std::unique_ptr<bench::WorkloadsBunch> workloads(new bench::WorkloadsBunch());
    bench::WorkloadsBunch::Options base_opts;
    base_opts.id =
        FLAGS_bench_id.empty() ? std::to_string(bcache2::GetCurrentTimeInNs()) : FLAGS_bench_id;
    base_opts.key_count = FLAGS_bench_workload_key_count;
    base_opts.key_size = FLAGS_bench_workload_key_size;

    base_opts.value_count = FLAGS_bench_workload_value_count;
    base_opts.value_min_size = FLAGS_bench_workload_value_min_size;
    base_opts.value_max_size = FLAGS_bench_workload_value_max_size;

    if (FLAGS_bench_workload_key_pattern == "S") {
        base_opts.key_pattern = bench::WorkloadsBunch::KeyPattern::kSequential;
    }

    if (FLAGS_bench_workload_key_dis == "R") {
        base_opts.key_dis = bench::WorkloadsBunch::KeyDist::kUniform;
    }
    if (FLAGS_bench_workload_key_dis == "Z") {
        base_opts.key_dis = bench::WorkloadsBunch::KeyDist::kZipfian;
        base_opts.zipfian_alpha = FLAGS_bench_workload_zipfian_alpha;
    }
    workloads->Init(base_opts);

    if (FLAGS_bench_hash_workload_freq > 0) {
        std::unique_ptr<bench::HashWorkload> hash_workloads(new bench::HashWorkload());
        bench::HashWorkload::Options opts;
        opts.freq_hset = FLAGS_bench_hash_workload_freq_hset;
        opts.freq_hget = FLAGS_bench_hash_workload_freq_hget;
        opts.freq_hgetall = FLAGS_bench_hash_workload_freq_hgetall;
        opts.freq_hdel = FLAGS_bench_hash_workload_freq_hdel;
        opts.field_count = FLAGS_bench_hash_workload_field_count;
        hash_workloads->Init(std::move(opts));
        workloads->RegisterWorkload(hash_workloads.release(), FLAGS_bench_hash_workload_freq);
    }

    if (FLAGS_bench_string_workload_freq > 0) {
        std::unique_ptr<bench::StringWorkload> string_workloads(new bench::StringWorkload());
        bench::StringWorkload::Options opts;
        opts.freq_set = FLAGS_bench_string_workload_freq_set;
        opts.freq_setex = FLAGS_bench_string_workload_freq_setex;
        opts.freq_get = FLAGS_bench_string_workload_freq_get;
        opts.setex_min_ttl_ms = FLAGS_bench_string_workload_setex_min_ttl_ms;
        opts.setex_max_ttl_ms = FLAGS_bench_string_workload_setex_max_ttl_ms;
        string_workloads->Init(std::move(opts));
        workloads->RegisterWorkload(string_workloads.release(), FLAGS_bench_string_workload_freq);
    }

    if (FLAGS_bench_common_workload_freq > 0) {
        std::unique_ptr<bench::CommonWorkload> common_workloads(new bench::CommonWorkload());
        bench::CommonWorkload::Options opts;
        opts.freq_del = FLAGS_bench_common_workload_freq_del;
        opts.freq_expire = FLAGS_bench_common_workload_freq_expire;
        opts.freq_ttl = FLAGS_bench_common_workload_freq_ttl;
        opts.expire_min_ttl_ms = FLAGS_bench_common_workload_expire_min_ttl_ms;
        opts.expire_max_ttl_ms = FLAGS_bench_common_workload_expire_max_ttl_ms;
        common_workloads->Init(std::move(opts));
        workloads->RegisterWorkload(common_workloads.release(), FLAGS_bench_common_workload_freq);
    }

    if (FLAGS_bench_ips_workload_freq > 0) {
        std::unique_ptr<bench::IpsWorkload> ips_workloads(new bench::IpsWorkload());
        bench::IpsWorkload::Options opts;
        opts.freq_add = FLAGS_bench_ips_workload_freq_add;
        opts.freq_query = FLAGS_bench_ips_workload_freq_query;
        ips_workloads->Init(std::move(opts));
        workloads->RegisterWorkload(ips_workloads.release(), FLAGS_bench_ips_workload_freq);
    }

    return workloads;
}

std::unique_ptr<bench::Client> InitClient() {
    std::unique_ptr<bench::Client> client;

    if (FLAGS_bench_client_type == "brpc") {
        std::unique_ptr<bench::BrpcClient> brpc_client(new bench::BrpcClient());
        bench::BrpcClient::Options opts;
        opts.partition_id = FLAGS_bench_brpc_client_partition_id;
        opts.server_addr = FLAGS_bench_brpc_client_server_addr;
        Status status = brpc_client->Init(opts);
        BYTE_ASSERT(status.ok()) << status;
        client.reset(brpc_client.release());
    }

    if (FLAGS_bench_client_type == "bcache2") {
        std::unique_ptr<bench::BCache2Client> bcache2_client(new bench::BCache2Client());
        bench::BCache2Client::Options opts;
        opts.idc = FLAGS_bench_bcache2_client_idc;
        opts.table_uri = FLAGS_bench_bcache2_client_table_uri;
        if (FLAGS_bench_bcache2_client_pin_primary) {
            opts.pin_primary = true;
        }

        Status status = bcache2_client->Init(opts);
        BYTE_ASSERT(status.ok()) << status;
        client.reset(bcache2_client.release());
    }

    if (FLAGS_bench_client_type == "thrift") {
        std::unique_ptr<bench::ThriftClient> thrift_client(new bench::ThriftClient());
        bench::ThriftClient::Options opts;
        opts.server_addr = FLAGS_bench_thrift_client_server_addr;
        opts.namespace_name = FLAGS_bench_thrift_client_namespace_name;
        opts.table_name = FLAGS_bench_thrift_client_table_name;
        Status status = thrift_client->Init(opts);
        BYTE_ASSERT(status.ok()) << status;
        client.reset(thrift_client.release());
    }

    return std::move(client);
}

int main(int args, char** argv) {
    gflags::ParseCommandLineFlags(&args, &argv, true);

    g_stop_flag = false;
    signal(SIGINT, &SignalHandler);
    signal(SIGTERM, &SignalHandler);

    if (FLAGS_bench_run_time == 0) {
        FLAGS_bench_run_time = UINT64_MAX;
    }
    if (FLAGS_bench_run_count == 0) {
        FLAGS_bench_run_count = UINT64_MAX;
    }

    std::unique_ptr<bench::WorkloadsBunch> workloads = InitWorkloads();
    std::unique_ptr<bench::Client> client = InitClient();
    bench::Bench bench;
    bench::Bench::Options opts;
    opts.client = client.get();
    opts.workloads = workloads.get();
    opts.jobs = FLAGS_bench_job_num;
    opts.depth = FLAGS_bench_depth;
    opts.key_ttl_ms = FLAGS_bench_key_ttl_ms;
    opts.stay_operations = FLAGS_bench_checker_enable;
    if (FLAGS_bench_checker_eventual_consistency_mode) {
        opts.delay_start_ms = FLAGS_bench_checker_eventual_consistency_history_time_ms * 2;
    } else {
        opts.delay_start_ms = 0;
    }
    bench.Init(opts);
    bench.Start();

    bench::ConsistencyChecker checker;
    bench::ConsistencyChecker::Options checker_opts;
    checker_opts.worker_num = FLAGS_bench_checker_worker_num;
    checker_opts.eventual_consistency_mode = FLAGS_bench_checker_eventual_consistency_mode;
    checker_opts.eventual_consistency_history_time_us =
        FLAGS_bench_checker_eventual_consistency_history_time_ms * 1000;
    checker_opts.max_ambiguous_time_ms = FLAGS_bench_checker_max_ambiguous_time_ms;
    checker_opts.max_expire_ambiguous_time_ms = FLAGS_bench_checker_max_expire_ambiguous_time_ms;
    checker_opts.timeout_ms = FLAGS_bench_checker_timeout_ms;
    checker.Init(checker_opts);

    brpc::Server server;
    brpc::ServiceOptions service_opts;
    service_opts.ownership = brpc::SERVER_OWNS_SERVICE;
    server.AddService(new bench::ServiceImpl(&bench, workloads.get(), &checker), service_opts);
    if (server.Start(FLAGS_bench_port, nullptr) != 0) {
        LOG_ERROR("failed to start server");
        return 1;
    }

    std::thread print_thread([&bench, &checker]() {
        while (true) {
            bench.PrintStats();
            checker.PrintStats();
            fflush(stdout);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    byte::SetByteLogDir(FLAGS_bench_log_dir);
    byte::SetByteLogMaxFileNum(FLAGS_bench_log_max_file_num);
    byte::SetByteLogMaxFileSize(FLAGS_bench_log_max_file_size);
    byte::SetMinLogLevel(byte::LogLevel(FLAGS_bench_log_level));

    uint64_t start_time_sec = bcache2::GetCurrentTimeInSec();
    while (true) {
        if (workloads->GetTotalCount() > FLAGS_bench_run_count) {
            break;
        }
        if (bcache2::GetCurrentTimeInSec() - start_time_sec > FLAGS_bench_run_time) {
            break;
        }
        if (!checker.Consistency()) {
            break;
        }
        if (g_stop_flag) {
            break;
        }

        if (bench.GetTotalOperationsNum() < FLAGS_bench_checker_max_operation_per_round) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        // 1. stop bench
        bench.Stop();

        // 2. pick some key to reuse in next round,
        std::unordered_set<std::string> reused_keys;
        // 80% from origin reused_keys
        std::vector<std::string> origin_reused_keys = workloads->GetReusedKeys();
        std::random_shuffle(origin_reused_keys.begin(), origin_reused_keys.end());
        for (uint64_t i = 0; i < origin_reused_keys.size(); ++i) {
            if (i > FLAGS_bench_checker_reuse_key_count * 4 / 5) {
                break;
            }
            reused_keys.emplace(origin_reused_keys[i]);
        }
        // last 20% from this round
        std::vector<std::vector<bench::Operation>> ops = bench.ExtractOperations();
        for (auto& worker_ops : ops) {
            for (auto& op : worker_ops) {
                if (reused_keys.size() >= FLAGS_bench_checker_reuse_key_count) {
                    break;
                }
                reused_keys.emplace(op.key());
            }
            if (reused_keys.size() >= FLAGS_bench_checker_reuse_key_count) {
                break;
            }
        }
        workloads->SetReusedKeys(std::vector<std::string>(reused_keys.begin(), reused_keys.end()));

        // 3. pass operations to checker and restart bench
        checker.CheckConsistency(std::move(ops));
        workloads->SetRound(workloads->GetRound() + 1);
        bench.Start();
    }

    bench.Stop();
    _Exit(0);
}
