#include "cache_bench.h"

#include "common/logging.h"
#include "debug_utils.h"
#include "simple_lru_cache.h"
#include "tools/stats.h"

#include <folly/String.h>
#include <gflags/gflags.h>

#include <chrono>
#include <cstdio>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <xxhash.h>

// We want to use `assertion` in our benchmarking tool anyway.
#undef NDEBUG
#include <cassert>

namespace mtcache {

template <typename Key, typename Value>
void CacheBench<Key, Value>::read(bool cache_miss) {
  // TODO
}

template <typename Key, typename Value>
void CacheBench<Key, Value>::write() {
  // TODO
}

template <typename Key, typename Value>
void CacheBench<Key, Value>::generate_rand_keys() {
  total_rand_key_cnt_ = (total_ops_ * (100 - read_ratio_)) / 100 + preload_ +
                        1; /* avoid read_ratio is 100 */
  rand_keys_.reserve(total_rand_key_cnt_);
  for (uint64_t i = 0; i < total_rand_key_cnt_; ++i) {
    rand_keys_.emplace_back(get_rand_str(32));
    // rand_keys_.emplace_back(std::to_string(i));
    if (i % 500000 == 0) {
      printf("%lu hashed key generated, sample: %s\n", i,
             rand_keys_[i].c_str());
    }
  }
  printf("Hashed keys generate finished, total = %lu\n", total_rand_key_cnt_);
}

template <typename Key, typename Value>
uint64_t CacheBench<Key, Value>::get_random_key_from_inserted_keys(
    uint64_t key_range, uint64_t hotkey_ratio, uint64_t hotkey_hit_ratio) {
  uint64_t rand_num = fast_rand64();
  uint64_t min_key_idx = 0;
  // determine if it is a hotkey testing
  if (hotkey_ratio && hotkey_hit_ratio) {
    if (rand_num % 100 < hotkey_hit_ratio) {
      // read a hotkey
      key_range = key_range * hotkey_ratio / 100;
    } else {
      // read a non-hotkey
      uint64_t default_key_range = key_range;
      key_range = key_range * (100 - hotkey_ratio) / 100;
      min_key_idx = default_key_range - key_range;
    }
  }
  return (rand_num % key_range) + min_key_idx;
}

template <typename Key, typename Value>
void CacheBench<Key, Value>::preload() {
  std::vector<std::thread> threads;

  // Preload certain amount of entries to the cache. If user does not specify
  // preload flag in pure read test, we force to preload 1/100 of total
  // operations entries to avoid reading from empty cache.
  if ((read_ratio_ == 100) && (preload_ == 0)) {
    preload_ = 1 + total_ops_ / 100;
  }
  std::atomic<uint64_t> preload_key_count{0};
  const int preload_workers = 12;
  for (int i = 0; i < preload_workers; ++i) {
    threads.emplace_back([&, i]() {
      const int load_per_thread =
          (i > 0) ? preload_ / preload_workers
                  : preload_ / preload_workers + preload_ % preload_workers;
      for (int j = 0; j < load_per_thread; ++j) {
        uint64_t idx =
            preload_key_count.fetch_add(1, std::memory_order_relaxed);
        std::string key = rand_keys_[idx];
        // std::string key = get_hashed_key(data);
        int vsize = (fast_rand16() %
                     (max_vsize_ - min_vsize_ + 1 /* avoid zero div */)) +
                    min_vsize_;
        char* value;
        rand_string(&value, vsize, false);
        cache_->Insert(key, std::string(value, vsize));
      }
    });
  }

  // Wait till all workers finish preloading.
  for (auto& thread : threads) {
    thread.join();
  }

  printf("Preload %ld keys finished.\n", preload_);
}

template <typename Key, typename Value>
void CacheBench<Key, Value>::run(
    Cache<Key, Value>* cache, const std::string& msg,
    const std::function<void(void)>& space_amplification_cb) {
  cache_ = cache;

  preload();

  if (space_amplification_cb) {
    space_amplification_cb();
  }

  // Init all threads Histgrams (avoid using locks during the test)
  // Since we are not going to change any key in the `map`, concurrent
  // access to their values should be safe.
  std::map<CacheBenchStatsType, std::vector<HistStats>> hists;
  for (int i = 0; i < workers_; ++i) {
    hists[CacheBenchStatsType::READ_INSERTED].emplace_back();
    hists[CacheBenchStatsType::READ_MISS].emplace_back();
    hists[CacheBenchStatsType::WRITE].emplace_back();
  }
#ifdef CACHE_BENCH_DEBUG
  std::atomic<uint64_t> long_reads_count{0};
  std::atomic<uint64_t> long_writes_count{0};
#endif
  // Used for calculate total qps
  auto t0 = absl::GetCurrentTimeNanos();

  // Benchmarking
  std::vector<std::thread> threads;
  key_count_ = preload_ + 1;
  std::atomic<uint64_t> read_hits{0};
  std::atomic<uint64_t> progress_count{0};
  LOG(INFO) << "key_count_ = " << key_count_ << " preload_ = " << preload_
            << " read_ratio_ = " << read_ratio_
            << " read_inserted_ratio_ = " << read_inserted_ratio_
            << " hotkey_ratio_ = " << hotkey_ratio_
            << " hotkey_hit_ratio_ = " << hotkey_hit_ratio_;
  for (int i = 0; i < workers_; ++i) {
    threads.emplace_back([&, i]() {
      const int ops_per_worker =
          (i > 0) ? total_ops_ / workers_
                  : total_ops_ / workers_ + total_ops_ % workers_;
      printf("[WORKER %d] Started\n", i);
      for (int j = 0; j < ops_per_worker; ++j) {
        uint64_t rand = fast_rand64() % 100;
        if (rand * 100 < (read_ratio_ * read_inserted_ratio_)) {
          // Read hit, select an existing id as key.
          uint64_t idx = get_random_key_from_inserted_keys(
              key_count_.load(), hotkey_ratio_, hotkey_hit_ratio_);
          std::string key = rand_keys_[idx];
          bool actual_hit = false;

          auto duration = timer([&]() {
            auto opt = cache_->Lookup(key);
            // Underlaying cache may evict items, so we don't have to check the
            // values.
            if (opt.has_value()) {
              actual_hit = true;
              read_hits++;
              if (opt->size() == 0) {
                printf("Wrong return value size, exit!\n");
                exit(-1);
              } else {
                read_bytes_ += opt->size();
              }
            }
          });
#ifdef CACHE_BENCH_DEBUG
          if (duration > LONG_LATENCY_THRESHOLD) {
            printf("\t Read long latency captured!\n");
            long_reads_count.fetch_add(1, std::memory_order_relaxed);
            PRINT_DEBUG_TIME_TRACE();
          }
#endif
          // Note that even we pre-assume current read will get a result, it
          // still has chance the underlaying item is evicted by the cache.
          // This is because the benchmark tool doesn't want to trace all items
          // in-memory
          if (actual_hit) {
            hists[CacheBenchStatsType::READ_INSERTED][i].append(duration);
          }
        } else if (rand < read_ratio_) {
          // Read miss, use an non-existed id.
          uint64_t data = key_count_.load();
          std::string key = "EMPTY_KEY_" + std::to_string(data);
          auto duration = timer([&]() {
            auto opt = cache_->Lookup(key);
            assert(!opt.has_value());
          });
#ifdef CACHE_BENCH_DEBUG
          if (duration > LONG_LATENCY_THRESHOLD) {
            printf("\t Read long latency captured!\n");
            long_reads_count.fetch_add(1, std::memory_order_relaxed);
            PRINT_DEBUG_TIME_TRACE();
          }
#endif
          hists[CacheBenchStatsType::READ_MISS][i].append(duration);
        } else {
          // Write
          uint64_t idx = key_count_.fetch_add(1, std::memory_order_relaxed);
          if (idx > total_rand_key_cnt_) {
            printf(
                "Insert key index overflow rand key vector, idx = %lu, "
                "total_key: %lu",
                idx, total_rand_key_cnt_);
            break;
          }
          std::string key = rand_keys_[idx];
          int vsize =
              (fast_rand16() % (max_vsize_ - min_vsize_ + 1)) + min_vsize_;
          char* value;
          rand_string(&value, vsize, false);

          auto duration =
              timer([&]() { cache_->Insert(key, std::string(value, vsize)); });
          hists[CacheBenchStatsType::WRITE][i].append(duration);
          write_bytes_ += vsize;

#ifdef CACHE_BENCH_DEBUG
          // Print time cost trace if duration takes too long time.
          if (duration > LONG_LATENCY_THRESHOLD) {
            printf("\t Write long latency captured!\n");
            long_writes_count.fetch_add(1, std::memory_order_relaxed);
            PRINT_DEBUG_TIME_TRACE();
          }
#endif
        }
        progress_count++;
        // progress_count.fetch_add(1, std::memory_order_relaxed);
        if (progress_count % 50000 == 0) {
          // printf("Op CNT = %lu\n", progress_count.load());
          // VLOG(1) << "Op CNT = " << progress_count.load();
        }
      }
    });
  }
  // Wait till all workers finish their task.
  for (auto& thread : threads) {
    thread.join();
  }

  auto t1 = absl::GetCurrentTimeNanos();
  uint64_t duration = (t1 - t0) / (1000 * 1000 * 1000) + 1 /* avoid 0 divison*/;

  // Merge all histgrams
  for (int i = 1; i < workers_; ++i) {
    hists[CacheBenchStatsType::READ_INSERTED][0].merge(
        hists[CacheBenchStatsType::READ_INSERTED][i]);
    hists[CacheBenchStatsType::READ_MISS][0].merge(
        hists[CacheBenchStatsType::READ_MISS][i]);
    hists[CacheBenchStatsType::WRITE][0].merge(
        hists[CacheBenchStatsType::WRITE][i]);
  }

  printf("Benchmark Finished\n");

  // Print histgram result
  uint64_t avg_read_size =
      read_hits.load() == 0 ? 0 : (read_bytes_.load() / read_hits.load());
  auto read_inserted_rst =
      hists[CacheBenchStatsType::READ_INSERTED][0].get_result(
          {0.5, 0.99, 0.999});
  uint64_t read_hit_qps =
      hists[CacheBenchStatsType::READ_INSERTED][0].count() / duration;
  printf("\n---------------- %s -------------\n", msg.data());
  printf("READ INSERTED HISTGRAM(ns): \n");
  printf(
      "P50: %lu, P99: %lu, P999: %lu, AVG: %lu, MAX: %lu, QPS: %lu, "
      "Throughput: %lu KB/s\n",
      read_inserted_rst[0], read_inserted_rst[1], read_inserted_rst[2],
      read_inserted_rst[3], read_inserted_rst[4], read_hit_qps,
      (read_hit_qps * avg_read_size) >> 10);
  uint64_t estimated_read_hits = key_count_.load() * read_ratio_ *
                                 read_inserted_ratio_ /
                                 (100 - read_ratio_ + 1) / 100;
  printf("Read Actual Hits: %lu, Estimated Hits: %lu\n", read_hits.load(),
         estimated_read_hits);

  auto read_miss_rst =
      hists[CacheBenchStatsType::READ_MISS][0].get_result({0.5, 0.99, 0.999});
  if (read_miss_rst[3] != 0) {
    uint64_t read_miss_qps =
        hists[CacheBenchStatsType::READ_MISS][0].count() / duration;
    printf("READ MISS HISTGRAM(ns):\n");
    printf("P50: %lu, P99: %lu, P999: %lu, AVG: %lu, MAX: %lu, QPS: %lu\n",
           read_miss_rst[0], read_miss_rst[1], read_miss_rst[2],
           read_miss_rst[3], read_miss_rst[4], read_miss_qps);
  }
  uint64_t avg_write_size =
      key_count_.load() - preload_ == 0
          ? 0
          : write_bytes_.load() / (key_count_.load() - preload_);
  auto write_rst =
      hists[CacheBenchStatsType::WRITE][0].get_result({0.5, 0.99, 0.999});
  uint64_t write_qps = hists[CacheBenchStatsType::WRITE][0].count() / duration;
  printf("WRITE HISTGRAM(ns):\n");
  printf(
      "P50: %lu, P99: %lu, P999: %lu, AVG: %lu, MAX: %lu, QPS: %lu, "
      "Throughput: %ld KB/s\n",
      write_rst[0], write_rst[1], write_rst[2], write_rst[3], write_rst[4],
      write_qps, (write_qps * avg_write_size) >> 10);
  printf("\n");
#ifdef CACHE_BENCH_DEBUG
  uint64_t long_lat_threshold_ms = LONG_LATENCY_THRESHOLD / 1000 / 1000;
  printf("Long latency reads (> %lu ms) captured: %lu.\n",
         long_lat_threshold_ms, long_reads_count.load());
  printf("Long latency writes (> %lu ms) captured: %lu.\n",
         long_lat_threshold_ms, long_writes_count.load());
#endif
  if (space_amplification_cb) {
    space_amplification_cb();
  }
  printf("\n\n");
  export_result();
}
}  // namespace mtcache

// All flags here.
DEFINE_string(value_size_range, "1000,2000", "value size range");
DEFINE_int32(workers, 1, "Total number of concurrent workers");
DEFINE_int32(read_ratio, 10, "The proportion of reads in all operations");
DEFINE_int32(read_inserted_ratio, 90,
             "Read proportion of inserted keys in all reads");
DEFINE_int32(hotkey_ratio, 0, "The proportion of hot keys in all keys");
DEFINE_int32(hotkey_hit_ratio, 0,
             "The proportion of hits on hotkeys in all hits");
DEFINE_int64(capacity_mb, 1000, "Capacity in mb");
DEFINE_string(bench_type, "flex",
              "[flex | simple | memcached | multitier], set to `simple` to run "
              "SimpleLRUCache, "
              "`memcached` for a quick memcached bench, "
              "`MultiTierCache` for a UnifiedCache test, "
              "`flex` means default");
DEFINE_string(policy, "fifo", "[fifo | slru]");
DEFINE_string(
    engine, "simple",
    "[dram | pmem | ssd_terarkdb |ssd_zonedstore | simple | multissd]");
DEFINE_string(pmem_paths, "/mnt/pmem0,/mnt/pmem1",
              "PMEM test path on NUMA node 0 and 1, comma separted");
DEFINE_string(ssd_paths, "/tmp",
              "SSD test path, comma separted, e.g. /dev/nvmeX,/dev/nvmeY");
DEFINE_int64(preload, 1, "Preload certain amount of entries to the cache.");
DEFINE_int64(total_ops, 1000000, "Total number of operations in the test.");
DEFINE_string(dram_pmem_data_placement_type, "SideBySide",
              "[SideBySide | Tiered]");
DEFINE_bool(eviction, false,
            "Whether eviction handler is enabled [true | false]");
DEFINE_uint64(
    side_by_side_dram_pmem_placement_threshold, 255,
    "Threshold to determine whether a cache item should be placed in DRAM or "
    "PMEM cache instance if SideBySide data placement type is used. If the "
    "value size is smaller than the threshold, the item is placed into DRAM "
    "instance; otherwise, it is placed into PMEM instance.");
DEFINE_int64(dram_capacity_mb, 1000, "Dram capacity in mb");
DEFINE_int64(pmem_capacity_mb, 0, "Pmem capacity in mb");
DEFINE_int64(ssd_capacity_mb, 0, "Ssd capacity in mb");
// The number of NUMA nodes used by the system
DECLARE_int32(used_num_numa_nodes);

// Main
int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  mtcache::CacheBench<std::string, std::string> bench(
      FLAGS_value_size_range, FLAGS_workers, FLAGS_read_ratio,
      FLAGS_read_inserted_ratio, FLAGS_hotkey_ratio, FLAGS_hotkey_hit_ratio,
      FLAGS_preload, FLAGS_total_ops);

  if (FLAGS_bench_type == "simple") {
    // Bench SimpleLRUCache
    if (FLAGS_workers > 1) {
      mtcache::ConcurrentSimpleLRUCache conc_simple_lru(FLAGS_capacity_mb
                                                        << 20);
      bench.run(&conc_simple_lru, "Concurrent Simple LRU Cache");
    } else {
      mtcache::SimpleLRUCache<std::string, std::string> simple_lru(
          FLAGS_capacity_mb << 20);
      bench.run(&simple_lru, "Simple LRU Cache");
    }
    return 0;
  }

  if (FLAGS_bench_type == "memcached") {
    mtcache::MemcachedWrapper memcached_wrapper(FLAGS_capacity_mb << 20);
    bench.run(&memcached_wrapper, "Memcached Cache");
    return 0;
  }
  std::vector<std::string> pmem_paths;
  std::vector<std::string> ssd_paths;
  folly::split(",", FLAGS_pmem_paths, pmem_paths);
  folly::split(",", FLAGS_ssd_paths, ssd_paths);
  // If a multi-tier caching platform is running, use a numa node instead
  if (FLAGS_bench_type == "multitier") {
    FLAGS_used_num_numa_nodes = 1;
    pmem_paths.resize(FLAGS_used_num_numa_nodes);
  }

  if (pmem_paths.size() != FLAGS_used_num_numa_nodes) {
    printf("FLAGS_used_num_numa_nodes != pmem_paths.size()\n");
    return 0;
  }

  if (FLAGS_bench_type == "multitier") {
    printf(
        "Multi-tier cache Benchmarking ... policy = %s, "
        "dram_pmem_data_placement_type = %s, "
        "side_by_side_dram_pmem_placement_threshold = %" PRIu64
        ", "
        "ssd_engine = %s\n ",
        FLAGS_policy.data(), FLAGS_dram_pmem_data_placement_type.data(),
        FLAGS_side_by_side_dram_pmem_placement_threshold, FLAGS_engine.data());

    mtcache::MultiTierCache multi_tier_cache(
        FLAGS_dram_capacity_mb << 20, FLAGS_pmem_capacity_mb << 20,
        FLAGS_ssd_capacity_mb << 20, FLAGS_policy, pmem_paths, ssd_paths,
        FLAGS_dram_pmem_data_placement_type, FLAGS_eviction,
        FLAGS_side_by_side_dram_pmem_placement_threshold, FLAGS_engine);
    bench.run(&multi_tier_cache, "Multi-tier Cache Bench");
    return 0;
  }

  // Otherwise we should specify `policy` and `engine`
  printf("Flexible Benchmarking... policy = %s, engine = %s\n",
         FLAGS_policy.data(), FLAGS_engine.data());

  mtcache::FlexibleCache flex_cache(FLAGS_capacity_mb << 20, FLAGS_policy,
                                    FLAGS_engine, pmem_paths, ssd_paths);
  bench.run(&flex_cache, "Flexible CacheInstance Benchmark");
  return 0;
}
