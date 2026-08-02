#include "common/thread_pool/cpu_numa_thread_pool_executor.h"
#include "unified_cache.h"

#include <folly/Benchmark.h>
#include <folly/io/IOBuf.h>
#include <gflags/gflags.h>

#include <random>
#include <time.h>
#include <vector>

DECLARE_int32(used_num_numa_nodes);
DECLARE_bool(cache_enable_eviction_handler);
DEFINE_int32(num_threads, 12, "num threads");
DEFINE_int32(num_tasks, 12000, "num tasks");
DEFINE_bool(warm_dram, false, "warm dram to de-affect page fault");

namespace mtcache {

static CacheOptions CreateCacheOptions() {
  CacheOptions opts{.dram_capacity = 40LLU * 1024 * 1024 * 1024,
                    .pmem_capacity = 40LLU * 1024 * 1024 * 1024,
                    .ssd_capacity = 0,
                    .pmem_paths = {"/mnt/pmem0/pmem_cache_bench",
                                   "/mnt/pmem1/pmem_cache_bench"},
                    .ssd_paths = {},
                    .cache_dram_replacement_policy = "FIFO",
                    .cache_pmem_replacement_policy = "FIFO",
                    .cache_ssd_replacement_policy = "FIFO",
                    .cache_dram_pmem_data_placement_type = "SideBySide",
                    .cache_dram_pmem_data_placement_threshold = 256,
                    .metric_id_prefix = "mtcache.async_pmem_bench",
                    .cache_ssd_instance_only = false};
  return opts;
}

static std::unique_ptr<UnifiedCache> CreateCache() {
  auto opts = CreateCacheOptions();
  FLAGS_cache_enable_eviction_handler = false;
  auto unified_cache = std::make_unique<UnifiedCache>(opts);
  unified_cache->Start();
  return unified_cache;
}

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

BENCHMARK(write_pmem_bench) {
  FLAGS_used_num_numa_nodes = 2;
  constexpr size_t kNumWarmThreads = 8;
  std::unique_ptr<CPUNumaThreadPoolExecutor> executor;
  std::unique_ptr<CPUNumaThreadPoolExecutor> warm_executor;
  std::unique_ptr<UnifiedCache> cache;
  constexpr size_t n_buf = 500;
  constexpr size_t val_len = 512 * 1024;
  std::vector<folly::IOBuf> values;
  struct timespec time_start;
  struct timespec time_end;
  std::atomic<uint64_t> fly_write_tasks{0};

  BENCHMARK_SUSPEND {
    executor = std::make_unique<CPUNumaThreadPoolExecutor>(
        FLAGS_num_threads,
        false,  // enable dynamicNumThreads because dram worker_ does not need
        // numa binding
        std::make_shared<folly::NamedThreadFactory>("AsyncPmemBench"));
    warm_executor = std::make_unique<CPUNumaThreadPoolExecutor>(
        kNumWarmThreads,
        false,  // enable dynamicNumThreads because dram worker_ does not need
        // numa binding
        std::make_shared<folly::NamedThreadFactory>("WarmThreads"));
    cache = CreateCache();
    values.reserve(n_buf);
    for (size_t i = 0; i < n_buf; i++) {
      values.emplace_back(CreateRandIOBuf(val_len));
    }
  }

  if (FLAGS_warm_dram) {
    for (size_t i = 0; i < FLAGS_num_tasks; i++) {
      folly::via(warm_executor.get(), [i, &values, &cache, &fly_write_tasks]() {
        cache->Insert("key" + std::to_string(i), values[i % values.size()],
                      val_len);
        fly_write_tasks.fetch_sub(1, std::memory_order_release);
      });
      fly_write_tasks.fetch_add(1, std::memory_order_release);
    }

    while (fly_write_tasks.load(std::memory_order_acquire) != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    for (size_t i = 0; i < FLAGS_num_tasks; i++) {
      folly::via(warm_executor.get(), [i, &cache, &fly_write_tasks]() {
        cache->Remove("key" + std::to_string(i));
        fly_write_tasks.fetch_sub(1, std::memory_order_release);
      });
      fly_write_tasks.fetch_add(1, std::memory_order_release);
    }

    while (fly_write_tasks.load(std::memory_order_acquire) != 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  clock_gettime(CLOCK_REALTIME, &time_start);
  for (size_t i = 0; i < FLAGS_num_tasks; i++) {
    folly::via(executor.get(), [i, &values, &cache]() {
      cache->Insert("key" + std::to_string(i), values[i % values.size()],
                    val_len);
    });
  }

  executor->join();
  cache->TEST_JoinPmemWriteExecutor();

  clock_gettime(CLOCK_REALTIME, &time_end);

  BENCHMARK_SUSPEND {
    // compute write throughput
    size_t micro_time = (time_end.tv_sec - time_start.tv_sec) * 1000000 +
                        (time_end.tv_nsec - time_start.tv_nsec) / 1000;
    size_t total_bytes = val_len * FLAGS_num_tasks;
    LOG(INFO) << "num_threads: " << FLAGS_num_threads
              << "\nTotal write bytes: " << total_bytes
              << "\nTotal time(us): " << micro_time
              << "\nThroughput(MB/s): " << total_bytes / micro_time;
  }
}

}  // namespace mtcache

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  folly::runBenchmarks();
  return 0;
}
