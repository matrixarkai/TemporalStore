#pragma once

#include "cache_instance.h"
#include "cache_wrapper.h"
#include "debug_utils.h"
#include "mtcache.h"
#include "simple_lru_cache.h"
#include "storage/zoned_store/zoned_store.h"
#include "utils.h"

#include <absl/time/clock.h>
#include <folly/String.h>
#include <libmemcached/memcached.h>
#include <noodle/metric/bytedance_metric_report_buidler.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <set>
#include <shared_mutex>
#include <stdio.h>
#include <typeinfo>
#include <unordered_map>
#include <vector>

//
// A simple cache benchmarking tool.
// This tool uses determinstic random key (xxhash generated keys) and
// variable-size values.
//
//
// Usage:
//    Run `./cache_bench --help` see all flags.
//

namespace mtcache {

enum CacheBenchStatsType : uint8_t { READ_INSERTED = 0, READ_MISS, WRITE };

// Cache benchmarking tool definition.
//
// Each `run` will uses a pre-defined cache implementation (e.g. MockCache,
// DRAMCache. etc.)
//
// Users should make sure all cache implementations are thread-safe.
template <typename Key, typename Value>
class CacheBench {
 public:
  CacheBench(const std::string& value_size_range, uint32_t workers,
             uint32_t read_ratio, uint32_t read_inserted_ratio,
             uint32_t hotkey_ratio, uint32_t hotkey_hit_ratio, uint64_t preload,
             uint64_t total_ops)
      : workers_(workers),
        read_ratio_(read_ratio),
        read_inserted_ratio_(read_inserted_ratio),
        hotkey_ratio_(hotkey_ratio),
        hotkey_hit_ratio_(hotkey_hit_ratio),
        preload_(preload),
        total_ops_(total_ops) {
    folly::split(',', value_size_range, min_vsize_, max_vsize_);
    generate_rand_keys();
  }

  ~CacheBench() = default;

 public:
  // Start benchmarking.
  // @cache Implementations of the cache.h
  // @msg Message title of the benchmarking result.
  // @space_amplification_cb if provided, invoke to get stats about storage
  // engine space amplification.
  void run(Cache<Key, Value>* cache, const std::string& msg,
           const std::function<void(void)>& space_amplification_cb = nullptr);

  // TODO(guokuankuan): Export bench result to target file.
  void export_result() {}

  // Reset the bench tool & prepare for next cache bench.
  void reset() { key_count_ = 1; }

 private:
  // Trace the execute time of target func.
  template <typename Func>
  uint64_t timer(const Func& func) {
    // auto t1 = std::chrono::steady_clock::now();
    auto t1 = absl::GetCurrentTimeNanos();
    func();
    // auto t2 = std::chrono::steady_clock::now();
    auto t2 = absl::GetCurrentTimeNanos();
    // auto duration =
    //    std::chrono::duration_cast<std::chrono::nanoseconds>(t2 - t1).count();
    return t2 - t1;
  }

  // Preload some data (based on `preload_`'s value)
  void preload();

  // Send a read query to cache
  void read(bool cache_miss = true);

  // Send a write query to cache
  void write();

  // Generate all rand keys before benchmarking
  void generate_rand_keys();

  // Generate a key from inserted keys based on target
  // hotkey_ratio & hotkey_hit_ratio
  uint64_t get_random_key_from_inserted_keys(uint64_t key_range,
                                             uint64_t hotkey_ratio,
                                             uint64_t hotkey_hit_ratio);

 private:
  // We pre generate all random keys for further use.
  std::vector<std::string> rand_keys_;

  uint64_t total_rand_key_cnt_ = 0;

  // Use xxHash32(id) as default keys.
  int key_size_ = 4;

  int min_vsize_ = (4 << 10);  // 4KB

  int max_vsize_ = (16 << 10);  // 16KB

  int workers_ = 1;

  // The proportion of read operations in all operations [0, 100]
  int read_ratio_ = 90;

  // The proportion of read inserted in all read operations [0, 100]
  int read_inserted_ratio_ = 90;

  // Total read bytes from Cache
  std::atomic<int64_t> read_bytes_{0};

  // Total written bytes to Cache
  std::atomic<int64_t> write_bytes_{0};

  // The proportion of hot keys in all keys
  int hotkey_ratio_ = 0;

  // The proportion of hits on hotkeys in all hits
  int hotkey_hit_ratio_ = 0;

  // Export filename.
  std::string export_file_ = "cache_bench_result.txt";

  // Benchmark target cache.
  Cache<Key, Value>* cache_;

  // Total written key value pairs.
  std::atomic<uint64_t> key_count_{1};

  // Number of entries preloaded to the cache.
  int64_t preload_ = 0;

  // Total number of operations (read + write).
  int64_t total_ops_ = 0;
};
}  // namespace mtcache
