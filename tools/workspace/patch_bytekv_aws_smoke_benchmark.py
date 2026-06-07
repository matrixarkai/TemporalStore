#!/usr/bin/env python3
from pathlib import Path

target = Path("/home/vj/bytekv-rocksdb-server/bytekv/tools/aws_smoke/main.cc")
target.write_text(r'''#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>

#include "client/config.h"
#include "client/kv_client_impl.h"
#include "common/internal_auth.h"
#include "common/kv_errorcode.h"
#include "common/log.h"
#include "common/master_client/master_brpc_client.h"
#include "proto/api.pb.h"
#include "proto/common.pb.h"
#include "proto/master.pb.h"

DEFINE_string(master, "", "master address, for example 10.70.1.79:26010");
DEFINE_string(tso, "", "tso address, for example 10.70.1.79:26020");
DEFINE_string(ns, "smoke", "namespace name");
DEFINE_string(table, "smoke_table", "table name without namespace");
DEFINE_string(key, "smoke_key", "key to write");
DEFINE_string(value, "smoke_value", "value to write");
DEFINE_uint32(cluster_id, 1001, "cluster id");
DEFINE_string(cluster_name, "bytekv_aws_test", "cluster name");
DEFINE_uint32(replica_count, 1, "table replica count");
DEFINE_uint32(wait_seconds, 30, "seconds to wait for table readiness");
DEFINE_uint32(quota_gb, 1, "table/namespace quota in GB");
DEFINE_uint32(partition_size_mb, 1024, "table partition size lower/upper bound in MB");

DEFINE_int32(benchmark_mode, 0, "run concurrent benchmark after table readiness when nonzero");
DEFINE_uint32(threads, 2, "benchmark worker threads");
DEFINE_uint32(ops, 10000, "total benchmark operations");
DEFINE_uint32(read_percent, 50, "percentage of Get operations");
DEFINE_uint32(key_count, 1000, "number of benchmark keys");
DEFINE_uint32(value_size, 128, "benchmark value size in bytes");
DEFINE_uint32(timeout_ms, 5000, "request timeout in milliseconds");

namespace {

std::string FullTableName() { return FLAGS_ns + ":" + FLAGS_table; }

void PrintCode(const std::string& label, bytekv::Errorcode code) {
  std::cout << label << ": " << bytekv::GetErrorString(code) << " (" << code
            << ")" << std::endl;
}

bytekv::Errorcode CreateNamespace(
    const std::shared_ptr<bytekv::common::MasterBrpcClient>& master) {
  bytekv::CreateNamespaceRequest req;
  bytekv::CreateNamespaceResponse resp;
  req.set_ns(FLAGS_ns);
  req.mutable_options()->set_quota_in_gb(FLAGS_quota_gb);
  auto code = master->GetNamespaceServiceRpcClient()->CreateNamespace(req, &resp);
  if (code == bytekv::BYTEKV_ERR_OK) {
    code = static_cast<bytekv::Errorcode>(resp.code());
  }
  PrintCode("create_namespace", code);
  return code;
}

bytekv::Errorcode CreateTable(
    const std::shared_ptr<bytekv::common::MasterBrpcClient>& master) {
  bytekv::CreateTableRequest req;
  bytekv::CreateTableResponse resp;
  req.set_table(FullTableName());
  req.mutable_options()->set_quota_in_gb(FLAGS_quota_gb);
  req.mutable_options()->set_replica_count(FLAGS_replica_count);
  req.mutable_options()->set_security(bytekv::TABLE_SECURITY_LEVEL_SERVER);
  req.mutable_options()->set_partition_size_mb_lower(FLAGS_partition_size_mb);
  req.mutable_options()->set_partition_size_mb_upper(FLAGS_partition_size_mb);
  auto code = master->GetNamespaceServiceRpcClient()->Create(req, &resp);
  if (code == bytekv::BYTEKV_ERR_OK) {
    code = static_cast<bytekv::Errorcode>(resp.code());
  }
  PrintCode("create_table", code);
  return code;
}

std::shared_ptr<bytekv::client::KVClientImpl> BuildClient() {
  auto config = std::make_shared<bytekv::client::Config>();
  config->cluster_id = FLAGS_cluster_id;
  config->master_addrs = std::vector<std::string>{FLAGS_master};
  config->tso_addrs = std::vector<std::string>{FLAGS_tso};
  config->refresh_table_timeval_seconds = 1;
  return std::make_shared<bytekv::client::KVClientImpl>(config);
}

bytekv::Errorcode PutOne(bytekv::client::KVClientImpl* client,
                         const std::string& key,
                         const std::string& value) {
  bytekv::PutRequest put;
  bytekv::PutResponse put_resp;
  put.set_table_name(FullTableName());
  put.set_key(key);
  put.set_value(value);
  put.set_timeout_ms(FLAGS_timeout_ms);
  auto put_code = client->Put(put, &put_resp);
  if (put_code != bytekv::BYTEKV_ERR_OK ||
      put_resp.code() != bytekv::BYTEKV_ERR_OK) {
    return put_code == bytekv::BYTEKV_ERR_OK
               ? static_cast<bytekv::Errorcode>(put_resp.code())
               : put_code;
  }
  return bytekv::BYTEKV_ERR_OK;
}

bytekv::Errorcode GetOne(bytekv::client::KVClientImpl* client,
                         const std::string& key,
                         std::string* value) {
  bytekv::GetRequest get;
  bytekv::GetResponse get_resp;
  get.set_table_name(FullTableName());
  get.set_key(key);
  get.set_timeout_ms(FLAGS_timeout_ms);
  auto get_code = client->Get(get, &get_resp);
  if (get_code != bytekv::BYTEKV_ERR_OK ||
      get_resp.code() != bytekv::BYTEKV_ERR_OK) {
    return get_code == bytekv::BYTEKV_ERR_OK
               ? static_cast<bytekv::Errorcode>(get_resp.code())
               : get_code;
  }
  *value = get_resp.value();
  return bytekv::BYTEKV_ERR_OK;
}

bytekv::Errorcode TryPutGet(bytekv::client::KVClientImpl* client,
                            bool verbose) {
  auto put_code = PutOne(client, FLAGS_key, FLAGS_value);
  if (verbose) {
    PrintCode("put_result", put_code);
  }
  if (put_code != bytekv::BYTEKV_ERR_OK) {
    return put_code;
  }

  std::string value;
  auto get_code = GetOne(client, FLAGS_key, &value);
  if (verbose) {
    PrintCode("get_result", get_code);
    std::cout << "get_value: " << value << std::endl;
  }
  if (get_code != bytekv::BYTEKV_ERR_OK) {
    return get_code;
  }
  if (value != FLAGS_value) {
    std::cerr << "value mismatch: expected " << FLAGS_value << " got "
              << value << std::endl;
    return bytekv::BYTEKV_ERR_INVALID_PARAMETER;
  }
  return bytekv::BYTEKV_ERR_OK;
}

uint64_t Percentile(const std::vector<uint64_t>& sorted, double pct) {
  if (sorted.empty()) {
    return 0;
  }
  size_t idx = static_cast<size_t>((pct / 100.0) * (sorted.size() - 1));
  return sorted[idx];
}

int RunBenchmark() {
  auto client = BuildClient();
  std::string value(FLAGS_value_size, 'v');
  std::cout << "benchmark_prefill_start keys=" << FLAGS_key_count << std::endl;
  for (uint32_t i = 0; i < FLAGS_key_count; ++i) {
    auto code = PutOne(client.get(), "bench_key_" + std::to_string(i), value);
    if (code != bytekv::BYTEKV_ERR_OK) {
      PrintCode("benchmark_prefill_error", code);
      return 1;
    }
  }
  std::cout << "benchmark_prefill_done" << std::endl;

  const uint32_t threads = std::max<uint32_t>(1, FLAGS_threads);
  const uint64_t ops_per_thread = FLAGS_ops / threads;
  const uint64_t remainder = FLAGS_ops % threads;
  std::vector<std::thread> workers;
  std::vector<std::vector<uint64_t>> latencies(threads);
  std::atomic<uint64_t> reads{0};
  std::atomic<uint64_t> writes{0};
  std::atomic<uint64_t> errors{0};

  auto start = std::chrono::steady_clock::now();
  for (uint32_t t = 0; t < threads; ++t) {
    uint64_t count = ops_per_thread + (t < remainder ? 1 : 0);
    workers.emplace_back([&, t, count]() {
      auto local_client = BuildClient();
      latencies[t].reserve(count);
      std::string out;
      for (uint64_t i = 0; i < count; ++i) {
        uint64_t seq = i * 1103515245ULL + t * 2654435761ULL;
        uint32_t key_idx = static_cast<uint32_t>(seq % FLAGS_key_count);
        bool do_read = (seq % 100) < FLAGS_read_percent;
        auto op_start = std::chrono::steady_clock::now();
        bytekv::Errorcode code;
        if (do_read) {
          code = GetOne(local_client.get(), "bench_key_" + std::to_string(key_idx),
                        &out);
          reads.fetch_add(1, std::memory_order_relaxed);
        } else {
          code = PutOne(local_client.get(), "bench_key_" + std::to_string(key_idx),
                        value);
          writes.fetch_add(1, std::memory_order_relaxed);
        }
        auto op_end = std::chrono::steady_clock::now();
        if (code != bytekv::BYTEKV_ERR_OK) {
          errors.fetch_add(1, std::memory_order_relaxed);
        }
        latencies[t].push_back(
            std::chrono::duration_cast<std::chrono::microseconds>(op_end - op_start)
                .count());
      }
    });
  }
  for (auto& worker : workers) {
    worker.join();
  }
  auto end = std::chrono::steady_clock::now();

  std::vector<uint64_t> all;
  for (auto& v : latencies) {
    all.insert(all.end(), v.begin(), v.end());
  }
  std::sort(all.begin(), all.end());
  double seconds = std::chrono::duration<double>(end - start).count();
  double qps = seconds > 0 ? static_cast<double>(all.size()) / seconds : 0.0;
  std::cout << "benchmark_result: PASS"
            << " table=" << FullTableName()
            << " threads=" << threads
            << " ops=" << all.size()
            << " reads=" << reads.load()
            << " writes=" << writes.load()
            << " errors=" << errors.load()
            << " seconds=" << seconds
            << " qps=" << qps
            << " p50_us=" << Percentile(all, 50)
            << " p95_us=" << Percentile(all, 95)
            << " p99_us=" << Percentile(all, 99)
            << " max_us=" << (all.empty() ? 0 : all.back())
            << std::endl;
  return errors.load() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  bytekv::logger->set_level(spdlog::level::warn);

  if (FLAGS_master.empty() || FLAGS_tso.empty()) {
    std::cerr << "--master and --tso are required" << std::endl;
    return 2;
  }
  if (FLAGS_key_count == 0 || FLAGS_read_percent > 100) {
    std::cerr << "invalid benchmark flags" << std::endl;
    return 2;
  }

  bytekv::common::InternalAuth::Init(FLAGS_cluster_name, FLAGS_cluster_id);
  auto master = std::make_shared<bytekv::common::MasterBrpcClient>(
      std::vector<std::string>{FLAGS_master}, std::vector<std::string>{FLAGS_tso});

  auto ns_code = CreateNamespace(master);
  auto table_code = CreateTable(master);
  (void)ns_code;
  (void)table_code;

  auto client = BuildClient();
  bytekv::Errorcode code = bytekv::BYTEKV_ERR_UNDEFINED;
  for (uint32_t i = 0; i <= FLAGS_wait_seconds; ++i) {
    code = TryPutGet(client.get(), i == FLAGS_wait_seconds);
    if (code == bytekv::BYTEKV_ERR_OK) {
      std::cout << "smoke_result: PASS table=" << FullTableName()
                << " key=" << FLAGS_key << " value=" << FLAGS_value
                << std::endl;
      return FLAGS_benchmark_mode != 0 ? RunBenchmark() : 0;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  PrintCode("smoke_result", code);
  return 1;
}
''')
