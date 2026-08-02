#include "buffer/iobuf_buffer.h"
#include "cache_instance.h"

#include <folly/Benchmark.h>
#include <folly/io/IOBuf.h>
#include <gflags/gflags.h>

#include <random>
#include <time.h>
#include <vector>

DECLARE_int32(used_num_numa_nodes);
DECLARE_bool(cache_enable_pmem_data_recovery);
DECLARE_int32(num_threads_recover_pmem);

DEFINE_int32(num_records, 800000, "num records");

namespace mtcache {

static folly::IOBuf CreateRandIOBuf(size_t len) {
  std::vector<int32_t> nums;
  size_t num = len / sizeof(int32_t);
  nums.reserve(num);
  std::srand(std::time(nullptr));
  for (size_t i = 0; i < num; i++) {
    nums.push_back(std::rand());
  }
  return *folly::IOBuf::copyBuffer(reinterpret_cast<const void*>(nums.data()),
                                   nums.size() * sizeof(int32_t));
}

BENCHMARK(pmem_recover_bench) {
  std::shared_ptr<noodle::MetricRegistry> registry;
  std::unique_ptr<CacheInstance> pmem_cache;
  constexpr size_t pmem_capacity = 800LLU * 1024 * 1024 * 1024;
  const ReplacementPolicyType replace_type = ReplacementPolicyType::kSLRU;
  const std::vector<std::string> pmem_paths = {
      "/mnt/pmem0/pmem_recovery_bench", "/mnt/pmem1/pmem_recovery_bench"};
  constexpr size_t n_buf = 1000;
  constexpr size_t val_len = 512 * 1024;
  std::vector<folly::IOBuf> values;
  struct timespec time_start;
  struct timespec time_end;

  BENCHMARK_SUSPEND {
    FLAGS_used_num_numa_nodes = 2;
    FLAGS_cache_enable_pmem_data_recovery = false;
    registry = noodle::GetMetricRegistry("ti.mtcache.pmem_cache_bench");

    values.reserve(n_buf);
    for (size_t i = 0; i < n_buf; i++) {
      values.emplace_back(CreateRandIOBuf(val_len));
    }
    pmem_cache = std::make_unique<CacheInstance>(
        pmem_capacity, replace_type, StorageEngineType::kPMEMStorageEngine,
        pmem_paths);
    pmem_cache->SetMetricRegistry(registry);
    pmem_cache->Start();
    LOG(INFO) << "Start to async write data into PMEM.";
    for (size_t i = 0; i < FLAGS_num_records; i++) {
      auto iobuf_buf = std::make_shared<IOBufBuffer>(values[i % values.size()]);
      iobuf_buf->SetKey("key" + std::to_string(i));
      pmem_cache->AsyncPut(std::move(iobuf_buf), "[PmemRecoverBench]");
    }
    // Wait all async put tasks to complete.
    pmem_cache->TEST_JoinPmemWriteExecutor();
    LOG(INFO) << "Finish to async write data into PMEM.";
    pmem_cache->Stop();
    pmem_cache.reset();
    noodle::GetGlobalMetricRegistry()->Deregister(
        "ti.mtcache.pmem_cache_bench");
  }

  FLAGS_cache_enable_pmem_data_recovery = true;
  pmem_cache = std::make_unique<CacheInstance>(
      pmem_capacity, replace_type, StorageEngineType::kPMEMStorageEngine,
      pmem_paths);
  pmem_cache->SetMetricRegistry(registry);

  clock_gettime(CLOCK_REALTIME, &time_start);
  pmem_cache->Start();
  clock_gettime(CLOCK_REALTIME, &time_end);

  BENCHMARK_SUSPEND {
    size_t micro_time = (time_end.tv_sec - time_start.tv_sec) * 1000000 +
                        (time_end.tv_nsec - time_start.tv_nsec) / 1000;
    size_t total_bytes = val_len * FLAGS_num_records;
    LOG(INFO) << "Total recover bytes: " << total_bytes
              << "\nTotal time(us): " << micro_time
              << "\nRecover num threads: " << FLAGS_num_threads_recover_pmem
              << "\nRecover Throughput(MB/s): " << total_bytes / micro_time;
    pmem_cache->Stop();
  }
}

}  // namespace mtcache

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  folly::runBenchmarks();
  return 0;
}
