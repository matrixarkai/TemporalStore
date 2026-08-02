#include "buffer/string_buffer.h"
#include "common/logging.h"
#include "unified_cache.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <tuple>
#include <vector>

DECLARE_uint64(cache_pmem_gc_reserved);
DECLARE_uint64(cache_dram_gc_reserved);
// The number of NUMA nodes used by the system
DECLARE_int32(used_num_numa_nodes);
DECLARE_bool(cache_collect_latency_summary);

namespace mtcache {

class AccessRecordCallbackMock : public AccessRecordCallback {
 public:
  using RecordsType = std::vector<std::pair<AccessRecordType, std::string>>;
  ~AccessRecordCallbackMock() override {}
  void OnAccess(AccessRecordType type, const std::string& key) override {
    records_.emplace_back(type, key);
  }
  RecordsType GetRecords() { return records_; }
  void ClearRecords() { records_.clear(); }

 private:
  RecordsType records_;
};

class UnifiedCacheSSDOnlyTestFixture
    : public testing::TestWithParam<std::string> {
 protected:
  void SetUp() override {
    // Disable NUMA-awareness in UTs.
    FLAGS_used_num_numa_nodes = 1;
    FLAGS_cache_dram_gc_reserved = 8LLU * 1024 * 1024;
    FLAGS_cache_pmem_gc_reserved = 8LLU * 1024 * 1024;
  }

  void TearDown() override {
    for (int i = 0; i < opts_.pmem_paths.size(); i++) {
      std::filesystem::remove_all(opts_.pmem_paths[i]);
    }
    for (auto path : opts_.ssd_paths) {
      std::filesystem::remove_all(path);
    }
  }

  // In CI tests, EXPECT_DEATH will fork child process.
  // which will cost long time to copy memory pre-allocated.
  // Therefore we set allocate capacity small to avoid that case.
  CacheOptions opts_{
      .dram_capacity = 1LLU * 32 * 1024 * 1024,                // 32 MB dram
      .pmem_capacity = 1LLU * 32 * 1024 * 1024,                // 32 MB pmem
      .ssd_capacity = 1LLU * 32 * 1024 * 1024,                 // 32 MB ssd
      .pmem_paths = {"/tmp/mtcache_unified_cache_test_pmem"},  // pmem path
      .ssd_paths = {"/tmp/mtcache_unified_cache_test_ssd"},    // ssd path
      .cache_dram_replacement_policy = GetParam(),
      .cache_pmem_replacement_policy = GetParam(),
      .cache_ssd_replacement_policy = GetParam(),
      .cache_dram_pmem_data_placement_type = "SideBySide",
      .cache_dram_pmem_data_placement_threshold = 256,
      .metric_id_prefix = "ti.mtcache",
      .cache_ssd_instance_only = true};
};

TEST_P(UnifiedCacheSSDOnlyTestFixture, SimpleTest) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, 32);
  auto start_res = cache.Start();
  EXPECT_TRUE(start_res);
  // Remove All Data
  cache.RemoveAll();
  // Stop L2 Cache Migrate
  auto* l2_cache_policy = cache.l2_cache_policy();
  EXPECT_NE(l2_cache_policy, nullptr);
  l2_cache_policy->TEST_Pause();
  l2_cache_policy->TEST_WaitAllTaskSleep();
}

TEST_P(UnifiedCacheSSDOnlyTestFixture, InsertAcquireRelease) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, 32);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);

  // The Used space in all instances starts with zero.
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kDRAM));
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kPMEM));
  EXPECT_EQ(0, cache.Size());

  // Set the data placement type to Tiered so cache buffers are inserted into
  // DRAM instance initially.
  cache.SetDataPlacementType(UnifiedCache::DRAMPMEMDataPlacementType::kTiered);

  // Insert method would insert the cache buffer into the DRAM instance
  std::string foo_data = "12345";
  cache.Insert(
      "foo", folly::IOBuf::wrapBufferAsValue(foo_data.c_str(), foo_data.size()),
      foo_data.size());
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kDRAM));
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kPMEM));
  EXPECT_EQ(8, cache.GetUsed(UnifiedCache::CacheInstanceType::kSSD));
  EXPECT_EQ(8, cache.Size());

  auto unified_inserts =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.unified.puts"));
  auto dram_inserts =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.dram.puts"));
  auto pmem_inserts =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.pmem.puts"));

  EXPECT_EQ(unified_inserts->GetValue(), 1);
  EXPECT_EQ(dram_inserts->GetValue(), 0);
  EXPECT_EQ(pmem_inserts->GetValue(), 0);

  // Acquire would find foo directly from SSD
  auto handle = cache.Acquire("foo");
  ASSERT_NE(nullptr, handle);
  EXPECT_STREQ(handle->key().c_str(), "foo");
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(handle->value().data()),
                        handle->value().length()),
            foo_data);
  EXPECT_EQ(8, cache.GetUsed(UnifiedCache::CacheInstanceType::kSSD));
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kDRAM));
  EXPECT_EQ(8, cache.Size());

  auto unified_acquires =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.unified.acquires"));
  auto unified_hits =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.unified.hits"));
  auto unified_misses =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.unified.misses"));
  auto ssd_hits = noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
      noodle::MetricId("ti.mtcache.ssd.hits"));
  auto ssd_misses =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.ssd.misses"));

  EXPECT_EQ(unified_acquires->GetValue(), 1);
  EXPECT_EQ(unified_hits->GetValue(), 1);
  EXPECT_EQ(unified_misses->GetValue(), 0);
  EXPECT_EQ(ssd_hits->GetValue(), 1);
  EXPECT_EQ(ssd_misses->GetValue(), 0);

  cache.Release(handle);
  // Remove method would delete foo from all cache instances.
  cache.Remove("foo");
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kSSD));
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kPMEM));
  EXPECT_EQ(0, cache.Size());

  auto unified_deletes =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.unified.deletes"));
  auto ssd_evicts =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.ssd.evicts"));
  EXPECT_EQ(unified_deletes->GetValue(), 1);
  EXPECT_EQ(ssd_evicts->GetValue(), 1);
  handle = cache.Acquire("foo");
  // TODO(guokuankuan@bytedance.com) Test soft delete?
  // SSD cache uses soft-delete (will not delete data immediately)
  // EXPECT_EQ(nullptr, handle);
  // EXPECT_EQ(ssd_misses->GetValue(), 1);
  // EXPECT_EQ(unified_misses->GetValue(), 1);
  EXPECT_EQ(unified_acquires->GetValue(), 2);

  // By default, query latency collection is disabled, and thus
  // GetLookupLatencySummarySnapshot should return a nullptr
  auto snapshot = cache.GetLookupLatencySummarySnapshot();
  EXPECT_EQ(nullptr, snapshot);
  cache.Release(handle);
}

TEST_P(UnifiedCacheSSDOnlyTestFixture, GetLookupLatencySummarySnapshot) {
  FLAGS_cache_collect_latency_summary = true;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, 40);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  {
    std::string foo_data = "12345";
    cache.Insert(
        "foo",
        folly::IOBuf::wrapBufferAsValue(foo_data.c_str(), foo_data.size()),
        foo_data.size());
    auto unified_handle = cache.Acquire("foo");
    ASSERT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    auto snapshot = cache.GetLookupLatencySummarySnapshot();
    EXPECT_NE(nullptr, snapshot);
    EXPECT_EQ(1, snapshot->GetCount());
    // A single lookup shouln't take more than 3 seconds
    // Comment out this test because query latency on CI is unpredictable
    // EXPECT_LT(snapshot->GetMax(), 3000);
    if (snapshot->GetMax() > 3000) {
      LOG(WARNING) << "snapshot->GetMax() = " << snapshot->GetMax();
    }
    LOG(INFO) << "UnifiedCacheTest::GetLookupLatencySummarySnapshot: "
              << "p25: " << snapshot->Get25thPercentile()
              << ", p50: " << snapshot->GetMedian()
              << ", p75: " << snapshot->Get75thPercentile()
              << ", p99: " << snapshot->Get99thPercentile()
              << ", p999: " << snapshot->Get999thPercentile()
              << ", max: " << snapshot->GetMax();
  }

  auto stop_res = cache.Stop();
  EXPECT_TRUE(stop_res);
}

TEST_P(UnifiedCacheSSDOnlyTestFixture, SetLargeDataAndGet) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, 40);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);

  std::string d1 = "aaaaaaaaaa";
  std::string d2 = "bbbbbbbbbb";
  std::string d3 = "cccccccccc";
  std::string d4 = "dddddddddd";
  std::string d5 = "eeeeeeeeee";

  for (int i = 0; i < 10; i++) {
    cache.Insert("key1", folly::IOBuf::wrapBufferAsValue(d1.c_str(), d1.size()),
                 d1.size());
    cache.Insert("key2", folly::IOBuf::wrapBufferAsValue(d2.c_str(), d2.size()),
                 d2.size());
    cache.Insert("key3", folly::IOBuf::wrapBufferAsValue(d3.c_str(), d3.size()),
                 d3.size());
    cache.Insert("key4", folly::IOBuf::wrapBufferAsValue(d4.c_str(), d4.size()),
                 d4.size());
    cache.Insert("key5", folly::IOBuf::wrapBufferAsValue(d5.c_str(), d5.size()),
                 d5.size());
    LOG(INFO) << i << ": " << cache.Size();
  }

  EXPECT_EQ(28, cache.Size());

  auto handle = cache.Acquire("key5");
  EXPECT_STREQ(handle->key().c_str(), "key5");
  LOG(INFO) << handle->value().data() << " vs " << d5;
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(handle->value().data()),
                        handle->value().length()),
            d5);
  cache.Release(handle);

  handle = cache.Acquire("key4");
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(handle->value().data()),
                        handle->value().length()),
            d4);
  cache.Release(handle);

  handle = cache.Acquire("key3");
  if (handle) {
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(handle->value().data()),
                          handle->value().length()),
              d3);
  }
  cache.Release(handle);
}

TEST_P(UnifiedCacheSSDOnlyTestFixture, ConcurrentTest) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, 10 * 1000 * 10);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);

  std::vector<std::thread> writers;
  for (int i = 0; i < 10; ++i) {
    writers.emplace_back([&, i]() {
      for (int j = 0; j < 1000; ++j) {
        // We release the value's memory immediately after the insertion, the
        // value is expected to be copied once in SSD cache.
        std::string key = std::to_string(i) + "_" + std::to_string(j) + "_key";
        std::string value =
            std::to_string(i) + "_" + std::to_string(j) + "_value";
        cache.Insert(
            key, folly::IOBuf::wrapBufferAsValue(value.c_str(), value.size()),
            value.size());
      }
    });
  }

  for (auto& worker : writers) {
    worker.join();
  }

  std::vector<std::thread> readers;
  for (int i = 0; i < 10; ++i) {
    readers.emplace_back([&, i]() {
      for (int j = 0; j < 1000; ++j) {
        std::string key = std::to_string(i) + "_" + std::to_string(j) + "_key";
        std::string value =
            std::to_string(i) + "_" + std::to_string(j) + "_value";
        auto handle = cache.Acquire(key);
        if (handle) {
          LOG(INFO) << "Get Success, key = " << handle->key()
                    << ", value = " << handle->value().data();
          // Some of the inserted value is still remind in memory, we should be
          // able to read them without problem (user data was copied to the
          // flush buffer)
          EXPECT_EQ(
              std::string(reinterpret_cast<const char*>(handle->value().data()),
                          handle->value().length()),
              value);
        }
        cache.Release(handle);
      }
    });
  }

  for (auto& worker : readers) {
    worker.join();
  }
}

INSTANTIATE_TEST_SUITE_P(UnifiedCacheSSDOnlyTest,
                         UnifiedCacheSSDOnlyTestFixture,
                         testing::Values("SLRU", "FIFO"));

}  // namespace mtcache

int main(int argc, char** argv) {
  google::InstallFailureSignalHandler();
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
