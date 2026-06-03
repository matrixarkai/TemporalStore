#include "unified_cache.h"

#include "buffer/string_buffer.h"
#include "cache_executor.h"
#include "common/logging.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <vector>

DECLARE_uint64(cache_pmem_gc_reserved);
DECLARE_uint64(cache_dram_gc_reserved);
// The number of NUMA nodes used by the system
DECLARE_int32(used_num_numa_nodes);
DECLARE_bool(cache_collect_latency_summary);
DECLARE_bool(cache_pmem_enable_async_write);
DECLARE_bool(cache_enable_eviction_handler);
DECLARE_int32(slru_num_segments);
DECLARE_bool(mtcache_enable_pmem_promotion);
DECLARE_int32(l2_cache_write_interval_ms);
DECLARE_bool(l2_policy_use_eviction_handler);
DECLARE_bool(mtcache_enable_ssd_promotion);
DECLARE_int32(num_pmem_cache_per_numa_writer_threads);

// The SSD device should be no less than 4GB
#define MIN_SSD_CAPACITY (4UL << 30)

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

class UnifiedCacheTestFixture : public testing::TestWithParam<std::string> {
 protected:
  static void TearDownTestCase() { CacheExecutor::DestroyAllExecutors(); }
  void SetUp() override {
    // Disable NUMA-awareness in UTs.
    FLAGS_used_num_numa_nodes = 1;
    FLAGS_cache_dram_gc_reserved = 8LLU * 1024 * 1024;
    FLAGS_cache_pmem_gc_reserved = 8LLU * 1024 * 1024;
    // The default value 256 may leads to immediate eviction due to too small
    // segment size.
    FLAGS_slru_num_segments = 1;
    // PMEM log allocator has an chance to trigger out of capacity error,
    // because
    // each writer need to occupy at least one chunk.
    // While we use very small PMEM size in the UT (and chunk size is hard coded
    // to 32MB), large number of writes to PMEM could fail.
    FLAGS_num_pmem_cache_per_numa_writer_threads = 1;
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
      .ssd_capacity = MIN_SSD_CAPACITY,                        // 4GB ssd
      .pmem_paths = {"/tmp/mtcache_unified_cache_test_pmem"},  // pmem path
      .ssd_paths = {"/tmp/mtcache_unified_cache_test_ssd"},    // ssd path
      .cache_dram_replacement_policy = GetParam(),
      .cache_pmem_replacement_policy = GetParam(),
      .cache_ssd_replacement_policy = GetParam(),
      .cache_dram_pmem_data_placement_type = "SideBySide",
      .cache_dram_pmem_data_placement_threshold = 256,
      .metric_id_prefix = "ti.mtcache",
      .cache_ssd_instance_only = false};
};

TEST_P(UnifiedCacheTestFixture, SimpleTest) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
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

TEST_P(UnifiedCacheTestFixture, SettersAndGetters) {
  UnifiedCache cache(opts_);
  auto policy = (GetParam() == "SLRU") ? ReplacementPolicyType::kSLRU
                                       : ReplacementPolicyType::kFIFO;
  EXPECT_EQ(
      cache.GetReplacementPolicyType(UnifiedCache::CacheInstanceType::kDRAM),
      policy);
  EXPECT_EQ(
      cache.GetReplacementPolicyType(UnifiedCache::CacheInstanceType::kPMEM),
      policy);
  auto alter_policy = static_cast<ReplacementPolicyType>(
      static_cast<int>(policy) ^
      static_cast<int>(ReplacementPolicyType::kSLRU));
  cache.SetReplacementPolicyType(UnifiedCache::CacheInstanceType::kDRAM,
                                 alter_policy);
  cache.SetReplacementPolicyType(UnifiedCache::CacheInstanceType::kSSD,
                                 alter_policy);
  EXPECT_EQ(
      cache.GetReplacementPolicyType(UnifiedCache::CacheInstanceType::kDRAM),
      alter_policy);
  EXPECT_EQ(
      cache.GetReplacementPolicyType(UnifiedCache::CacheInstanceType::kSSD),
      alter_policy);

  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 10);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 20);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  EXPECT_EQ(cache.GetCapacity(UnifiedCache::CacheInstanceType::kDRAM), 10);
  EXPECT_EQ(cache.GetCapacity(UnifiedCache::CacheInstanceType::kPMEM), 20);
  EXPECT_EQ(cache.Capacity(), MIN_SSD_CAPACITY);

  EXPECT_EQ(cache.GetDataPlacementType(),
            UnifiedCache::DRAMPMEMDataPlacementType::kSideBySide);
  cache.SetDataPlacementType(UnifiedCache::DRAMPMEMDataPlacementType::kTiered);
  EXPECT_EQ(cache.GetDataPlacementType(),
            UnifiedCache::DRAMPMEMDataPlacementType::kTiered);

  EXPECT_EQ(cache.GetDataPlacementThreshold(), 256);
  cache.SetDataPlacementThreshold(128);
  EXPECT_EQ(cache.GetDataPlacementThreshold(), 128);
}

TEST_P(UnifiedCacheTestFixture, InsertAcquireRelease) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
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
  auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
  cache.Insert("foo", *foo_buf, foo_data.size());
  EXPECT_EQ(8, cache.GetUsed(UnifiedCache::CacheInstanceType::kDRAM));
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kPMEM));
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
  EXPECT_EQ(dram_inserts->GetValue(), 1);
  EXPECT_EQ(pmem_inserts->GetValue(), 0);

  // Acquire would find foo and bring it up to the DRAM instance
  auto handle = cache.Acquire("foo");
  ASSERT_NE(nullptr, handle);
  EXPECT_STREQ(handle->key().c_str(), "foo");
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(handle->value().data()),
                        handle->value().length()),
            foo_data);
  EXPECT_EQ(8, cache.GetUsed(UnifiedCache::CacheInstanceType::kDRAM));
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kPMEM));
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
  auto dram_hits =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.dram.hits"));
  auto dram_misses =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.dram.misses"));
  auto pmem_hits =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.pmem.hits"));
  auto pmem_misses =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.pmem.misses"));

  EXPECT_EQ(unified_acquires->GetValue(), 1);
  EXPECT_EQ(unified_hits->GetValue(), 1);
  EXPECT_EQ(unified_misses->GetValue(), 0);
  EXPECT_EQ(dram_hits->GetValue(), 1);
  EXPECT_EQ(dram_misses->GetValue(), 0);
  EXPECT_EQ(pmem_hits->GetValue(), 0);
  EXPECT_EQ(pmem_misses->GetValue(), 0);

  cache.Release(handle);
  // Remove method would delete foo from all cache instances.
  cache.Remove("foo");
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kDRAM));
  EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kPMEM));
  EXPECT_EQ(0, cache.Size());

  auto unified_deletes =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.unified.deletes"));
  auto dram_evicts =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.dram.evicts"));
  auto pmem_evicts =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.pmem.evicts"));
  EXPECT_EQ(unified_deletes->GetValue(), 1);
  EXPECT_EQ(dram_evicts->GetValue(), 1);
  EXPECT_EQ(pmem_evicts->GetValue(), 0);
  // The buffer can no longer be found after the Remove operation.
  handle = cache.Acquire("foo");
  EXPECT_EQ(nullptr, handle);
  EXPECT_EQ(unified_acquires->GetValue(), 2);
  EXPECT_EQ(unified_misses->GetValue(), 1);
  EXPECT_EQ(dram_misses->GetValue(), 1);
  EXPECT_EQ(pmem_misses->GetValue(), 1);

  // By default, query latency collection is disabled, and thus
  // GetLookupLatencySummarySnapshot should return a nullptr
  auto snapshot = cache.GetLookupLatencySummarySnapshot();
  EXPECT_EQ(nullptr, snapshot);
}

TEST_P(UnifiedCacheTestFixture, GetLookupLatencySummarySnapshot) {
  FLAGS_cache_collect_latency_summary = true;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 100);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 0);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  {
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    cache.Insert("foo", *foo_buf, foo_data.size());
    auto unified_handle = cache.Acquire("foo");
    ASSERT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    auto snapshot = cache.GetLookupLatencySummarySnapshot();
    EXPECT_NE(nullptr, snapshot);
    EXPECT_EQ(1, snapshot->GetCount());
    // A single lookup shouldn't take more than 3 seconds
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

TEST_P(UnifiedCacheTestFixture, PmemDisableAsyncWrite) {
  FLAGS_cache_pmem_enable_async_write = false;
  UnifiedCache cache(opts_);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  {
    cache.SetDataPlacementThreshold(8);
    std::string foo_data = "12345678ab";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    cache.Insert("foo", *foo_buf, foo_data.size());
    auto unified_handle = cache.Acquire("foo");
    ASSERT_NE(nullptr, unified_handle);
    auto dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_EQ(nullptr, dram_handle);
    auto pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_NE(nullptr, pmem_handle);
    cache.Release(unified_handle);
    cache.Release(dram_handle);
    cache.Release(pmem_handle);
  }
  auto stop_res = cache.Stop();
  EXPECT_TRUE(stop_res);
}

TEST_P(UnifiedCacheTestFixture, DataPlacement) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 40);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 100);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  {
    // With sidebyside data placement type and threshold being 16, foo_data
    // will only be insert into DRAM instance.
    cache.SetDataPlacementType(
        UnifiedCache::DRAMPMEMDataPlacementType::kSideBySide);
    cache.SetDataPlacementThreshold(16);
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    cache.Insert("foo", *foo_buf, foo_data.size());
    auto unified_handle = cache.Acquire("foo");
    ASSERT_NE(nullptr, unified_handle);
    auto dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_NE(nullptr, dram_handle);
    auto pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);

    cache.Release(unified_handle);
    cache.Release(dram_handle);
    cache.Release(pmem_handle);

    // bar will be inserted up into PMEM only since its value size (18) is
    // greater than the DRAM/PMEM data placement threshold (16)
    std::string bar_data = "barbarbarbarbarbar";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    cache.Insert("bar", *bar_buf, bar_data.size());
    pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "bar");
    EXPECT_NE(nullptr, pmem_handle);
    // const void* pmem_data1 = pmem_handle->value().data();
    unified_handle = cache.Acquire("bar");
    ASSERT_NE(nullptr, unified_handle);
    dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "bar");
    EXPECT_EQ(nullptr, dram_handle);
    cache.Release(unified_handle);
    cache.Release(pmem_handle);

    // The running speed of multi-threads is indeterminate, so we comment
    // the testing below. We leve it here for future debugging.
    //
    // sleep 100ms to wait for AsyncPut PMEM completing
    // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // auto pmem_handle2 =
    //     cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "bar");
    // EXPECT_NE(nullptr, pmem_handle2);
    // const void* pmem_data2 = pmem_handle2->value().Data();
    // EXPECT_NE(pmem_data1, pmem_data2);
    // cache.Release(pmem_handle2);

    EXPECT_EQ(8, cache.GetUsed(UnifiedCache::CacheInstanceType::kDRAM));
    EXPECT_EQ(21, cache.GetUsed(UnifiedCache::CacheInstanceType::kPMEM));

    // After a RemoveAll() call, cached buffers in all instances are cleaned
    // out.
    cache.RemoveAll();
    EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kDRAM));
    EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kPMEM));

    // Test for Tiered placement
    cache.SetDataPlacementType(
        UnifiedCache::DRAMPMEMDataPlacementType::kTiered);
    cache.Insert("foo", *foo_buf, foo_data.size());
    unified_handle = cache.Acquire("foo");
    ASSERT_NE(nullptr, unified_handle);
    dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_NE(nullptr, dram_handle);
    pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);
    cache.Release(unified_handle);
    cache.Release(dram_handle);
    cache.Release(pmem_handle);

    // The data placement threshold is no longer honored when the data
    // placement type is changed to tiered.
    cache.Insert("bar", *bar_buf, bar_data.size());
    unified_handle = cache.Acquire("bar");
    ASSERT_NE(nullptr, unified_handle);
    dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "bar");
    EXPECT_NE(nullptr, dram_handle);
    pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "bar");
    EXPECT_EQ(nullptr, pmem_handle);
    cache.Release(unified_handle);
    cache.Release(dram_handle);
    cache.Release(pmem_handle);

    EXPECT_EQ(29, cache.GetUsed(UnifiedCache::CacheInstanceType::kDRAM));
    EXPECT_EQ(0, cache.GetUsed(UnifiedCache::CacheInstanceType::kPMEM));
  }
  auto stop_res = cache.Stop();
  EXPECT_TRUE(stop_res);
}

TEST_P(UnifiedCacheTestFixture, InsertPinned) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 10);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 20);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  {
    cache.SetDataPlacementType(
        UnifiedCache::DRAMPMEMDataPlacementType::kTiered);
    std::string bar_data = "56789";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    auto unified_handle = cache.InsertPinned("bar", *bar_buf, bar_data.size());
    EXPECT_NE(nullptr, unified_handle);
    EXPECT_EQ(unified_handle->key(), std::string("bar"));
    EXPECT_EQ(std::string(
                  reinterpret_cast<const char*>(unified_handle->value().data()),
                  unified_handle->value().length()),
              bar_data);
    cache.Release(unified_handle);
    auto unified_inserts =
        noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
            noodle::MetricId("ti.mtcache.unified.puts"));
    auto unified_insert_pinned =
        noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
            noodle::MetricId("ti.mtcache.unified.insert_pinned"));
    EXPECT_EQ(0, unified_inserts->GetValue());
    EXPECT_EQ(1, unified_insert_pinned->GetValue());
  }
  auto stop_res = cache.Stop();
  EXPECT_TRUE(stop_res);
}

TEST_P(UnifiedCacheTestFixture, EvictionHandlingDisabledHandler) {
  // Disable the eviction handler of DRAM cache instance.
  // Thus cache buffers are just dropped when they are evicted from the DRAM
  // instance.
  FLAGS_cache_enable_eviction_handler = false;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  cache.SetDataPlacementType(UnifiedCache::DRAMPMEMDataPlacementType::kTiered);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  {
    // Insert the first cache buffer foo
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    cache.Insert("foo", *foo_buf, foo_data.size());
    auto unified_handle = cache.Acquire("foo");
    cache.Release(unified_handle);
    // Remove foo from PMEM cache instance
    cache.TEST_Remove(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    auto pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);
    // Insert bar into unified cache and acquire it, foo will be evicted from
    // dram cache instance, but eviction handler is disabled by default.
    std::string bar_data = "56789";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    cache.Insert("bar", *bar_buf, bar_data.size());
    unified_handle = cache.Acquire("bar");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    unified_handle = cache.Acquire("foo");
    EXPECT_EQ(nullptr, unified_handle);
  }
}

TEST_P(UnifiedCacheTestFixture, EvictionHandlingPMEMNoHandler) {
  FLAGS_cache_enable_eviction_handler = false;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 18);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  {
    cache.SetDataPlacementType(
        UnifiedCache::DRAMPMEMDataPlacementType::kSideBySide);
    cache.SetDataPlacementThreshold(5);
    std::string foo_long_data = "1234567890";
    auto foo_buf =
        folly::IOBuf::copyBuffer(foo_long_data.c_str(), foo_long_data.size());
    cache.Insert("foo_long", *foo_buf, foo_long_data.size());
    auto unified_handle = cache.Acquire("foo_long");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    // Insert bar_long into unified cache and acquire it, foo_long will be
    // evicted from pmem cache instance.
    std::string bar_long_data = "0123456789";
    auto bar_buf =
        folly::IOBuf::copyBuffer(bar_long_data.c_str(), bar_long_data.size());
    cache.Insert("bar_long", *bar_buf, bar_long_data.size());
    unified_handle = cache.Acquire("bar_long");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    unified_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo_long");
    EXPECT_EQ(nullptr, unified_handle);
  }
}

TEST_P(UnifiedCacheTestFixture, EvictionHandlingSideBySideDataPlacement) {
  FLAGS_cache_enable_eviction_handler = true;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);

  {
    // When kSideBySide data placement type is used, even if eviction handler of
    // DRAM instance is enabled, cache buffers evicted from DRAM will be
    // dropped.
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    cache.Insert("foo", *foo_buf, foo_data.size());
    auto unified_handle = cache.Acquire("foo");
    EXPECT_NE(nullptr, unified_handle);
    auto dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_NE(nullptr, dram_handle);
    auto pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);
    cache.Release(unified_handle);
    cache.Release(dram_handle);
    cache.Release(pmem_handle);
    std::string bar_data = "56789";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    cache.Insert("bar", *bar_buf, bar_data.size());
    unified_handle = cache.Acquire("bar");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_EQ(nullptr, dram_handle);
    pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);
    cache.RemoveAll();
  }
}

TEST_P(UnifiedCacheTestFixture, EvictionHandlingTieredDataPlacement) {
  FLAGS_cache_enable_eviction_handler = true;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  // Change data placement type to Tiered
  cache.SetDataPlacementType(UnifiedCache::DRAMPMEMDataPlacementType::kTiered);
  {
    // With tiered data placement type and eviction handler enabled, cache
    // buffers evicted from DRAM cache instance are inserted into PMEM cache
    // instance.
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    auto unified_handle = cache.InsertPinned("foo", *foo_buf, foo_data.size());
    EXPECT_NE(unified_handle, nullptr);
    auto dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_NE(nullptr, dram_handle);
    auto pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);
    const void* dram_ptr1 = unified_handle->value().data();
    const void* dram_ptr2 = dram_handle->value().data();
    EXPECT_EQ(dram_ptr1, dram_ptr2);
    cache.Release(unified_handle);
    cache.Release(dram_handle);

    // foo is evicted into pmem when bar is inserted.
    std::string bar_data = "56789";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    unified_handle = cache.InsertPinned("bar", *bar_buf, bar_data.size());
    EXPECT_NE(unified_handle, nullptr);
    cache.Release(unified_handle);
    // The running speed of multi-threads is indeterminate, so we comment
    // the testing below. We leve it here for future debugging.
    //
    // pmem_handle =
    //     cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    // EXPECT_NE(pmem_handle, nullptr);
    // const void* pmem_ptr1 = pmem_handle->value().Data();
    // EXPECT_EQ(dram_ptr1, pmem_ptr1);

    // The running speed of multi-threads is indeterminate, so we comment
    // the testing below. We leve it here for future debugging.
    //
    // sleep 100ms to wait for AsyncPut PMEM completing
    // std::this_thread::sleep_for(std::chrono::milliseconds(100));
    // auto pmem_handle2 =
    //     cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    // const void* pmem_ptr2 = pmem_handle2->value().Data();
    // EXPECT_NE(dram_ptr1, pmem_ptr2);
    // cache.Release(pmem_handle2);
    cache.Release(pmem_handle);

    // bar is evicted from pmem instance when new buffers are inserted
    std::string fur_data = "00111";
    auto fur_buf = folly::IOBuf::copyBuffer(fur_data.c_str(), fur_data.size());
    unified_handle = cache.InsertPinned("fur", *fur_buf, fur_data.size());
    EXPECT_NE(unified_handle, nullptr);
    cache.Release(unified_handle);
    std::string fuu_data = "11111";
    auto fuu_buf = folly::IOBuf::copyBuffer(fuu_data.c_str(), fuu_data.size());
    unified_handle = cache.InsertPinned("fuu", *fuu_buf, fuu_data.size());
    EXPECT_NE(unified_handle, nullptr);
    cache.Release(unified_handle);
    pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);
  }
}

TEST_P(UnifiedCacheTestFixture, TieredDataPlacementAsyncPromotion) {
  FLAGS_cache_enable_eviction_handler = true;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  // Change data placement type to Tiered
  cache.SetDataPlacementType(UnifiedCache::DRAMPMEMDataPlacementType::kTiered);
  {
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    auto unified_handle = cache.InsertPinned("foo", *foo_buf, foo_data.size());
    EXPECT_NE(unified_handle, nullptr);
    auto dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_NE(nullptr, dram_handle);
    auto pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);
    cache.Release(dram_handle);
    cache.Release(pmem_handle);
    cache.Release(unified_handle);

    std::string bar_data = "56789";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    unified_handle = cache.InsertPinned("bar", *bar_buf, bar_data.size());
    EXPECT_NE(unified_handle, nullptr);
    cache.Release(unified_handle);

    dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "bar");
    EXPECT_NE(nullptr, dram_handle);
    pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "bar");
    EXPECT_EQ(nullptr, pmem_handle);
    cache.Release(dram_handle);
    cache.Release(pmem_handle);

    // foo data has been evicted to PMEM cache instance
    dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_EQ(nullptr, dram_handle);
    pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_NE(nullptr, pmem_handle);

    cache.Release(dram_handle);
    cache.Release(pmem_handle);

    unified_handle = cache.Acquire("foo");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    // foo data should be promoted to DRAM cache instance
    dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    // The data may be in dram now as the promotion is async. But the index
    // is updated syncly.
    EXPECT_NE(nullptr, dram_handle);
    cache.Release(dram_handle);
  }
  auto stop_res = cache.Stop();
  ASSERT_TRUE(stop_res);
}

TEST_P(UnifiedCacheTestFixture, EvictionHandlingDeregisteredHandler) {
  FLAGS_cache_enable_eviction_handler = true;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  EXPECT_TRUE(start_res);
  // Change data placement type to Tiered
  cache.SetDataPlacementType(UnifiedCache::DRAMPMEMDataPlacementType::kTiered);
  // Deregister eviction handler
  cache.DeregisterEvictionHandler();

  {
    // After the eviction handler is deregistered, evicted buffers from
    // DRAM instance is no longer inserted into PMEM instance even if the
    // eviction handler is enabled, and data placement type is tiered.
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    cache.Insert("foo", *foo_buf, foo_data.size());
    auto unified_handle = cache.Acquire("foo");
    cache.Release(unified_handle);
    // Remove foo from PMEM cache instance
    cache.TEST_Remove(UnifiedCache::CacheInstanceType::kPMEM, "foo");

    std::string bar_data = "56789";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    cache.Insert("bar", *bar_buf, bar_data.size());
    unified_handle = cache.Acquire("bar");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    auto dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_EQ(nullptr, dram_handle);
    auto pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);
  }
}

TEST_P(UnifiedCacheTestFixture, AccessRecordCallback) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 8);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  AccessRecordCallbackMock mock;
  auto start_res = cache.Start();
  cache.RegisterAccessRecordCallback(&mock);
  EXPECT_TRUE(start_res);
  // Remove All Data
  cache.RemoveAll();
  // Stop L2 Cache Migrate
  auto l2_cache_policy = cache.l2_cache_policy();
  CHECK_NOTNULL(l2_cache_policy);
  l2_cache_policy->TEST_Pause();
  l2_cache_policy->TEST_WaitAllTaskSleep();

  {
    // Insert, Acquire and Delete method calls create access records of Put, Get
    // and Delete.
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    cache.Insert("foo", *foo_buf, foo_data.size());
    auto unified_handle = cache.Acquire("foo");
    cache.Release(unified_handle);
    cache.Remove("foo");
    auto records = mock.GetRecords();
    ASSERT_EQ(3, records.size());
    EXPECT_EQ(AccessRecordType::kPut, records[0].first);
    EXPECT_EQ(AccessRecordType::kGet, records[1].first);
    EXPECT_EQ(AccessRecordType::kDelete, records[2].first);
    for (auto i = 0; i < records.size(); ++i) {
      EXPECT_STREQ("foo", records[i].second.c_str());
    }

    // InsertPinned and Remove method calls also create access records of Put,
    // Get and Delete
    mock.ClearRecords();
    records = mock.GetRecords();
    EXPECT_EQ(0, records.size());
    unified_handle = cache.InsertPinned("foo", *foo_buf, foo_data.size());
    cache.Release(unified_handle);
    cache.Remove("foo");
    records = mock.GetRecords();
    ASSERT_EQ(2, records.size());
    EXPECT_EQ(AccessRecordType::kPut, records[0].first);
    EXPECT_EQ(AccessRecordType::kDelete, records[1].first);
    for (auto i = 0; i < records.size(); ++i) {
      EXPECT_STREQ("foo", records[i].second.c_str());
    }
  }
}

TEST_P(UnifiedCacheTestFixture, GetBypassReplacementPolicy) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 0);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  cache.SetReplacementPolicyType(UnifiedCache::CacheInstanceType::kDRAM,
                                 ReplacementPolicyType::kSLRU);
  auto start_res = cache.Start();
  EXPECT_TRUE(start_res);
  // Remove All Data
  cache.RemoveAll();
  // Stop L2 Cache Migrate
  auto l2_cache_policy = cache.l2_cache_policy();
  CHECK_NOTNULL(l2_cache_policy);
  l2_cache_policy->TEST_Pause();
  l2_cache_policy->TEST_WaitAllTaskSleep();
  cache.DisablePolicyMemEvictionHandler();

  {
    // Cache buffer bar is evicted even though foo is inserted into the cache
    // earlier, because foo is accessed by Acquire more recently.
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    cache.Insert("foo", *foo_buf, foo_data.size());
    std::string bar_data = "67890";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    cache.Insert("bar", *bar_buf, bar_data.size());
    // Access foo twice so SLRU will mark it as an ACTIVE buffer
    auto unified_handle = cache.Acquire("foo");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    unified_handle = cache.Acquire("foo");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    std::string fur_data = "11111";
    auto fur_buf = folly::IOBuf::copyBuffer(fur_data.c_str(), fur_data.size());
    sleep(2);
    cache.Insert("fur", *fur_buf, fur_data.size());
    unified_handle = cache.Acquire("foo");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);
    unified_handle = cache.Acquire("bar");
    EXPECT_EQ(nullptr, unified_handle);
    cache.RemoveAll();
  }
}

TEST_P(UnifiedCacheTestFixture, SSDCache) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 0);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  cache.SetReplacementPolicyType(UnifiedCache::CacheInstanceType::kDRAM,
                                 ReplacementPolicyType::kSLRU);
  auto start_res = cache.Start();
  EXPECT_TRUE(start_res);
  // Remove All Data
  cache.RemoveAll();
  // Stop L2 Cache Migrate
  auto l2_cache_policy = cache.l2_cache_policy();
  CHECK_NOTNULL(l2_cache_policy);
  l2_cache_policy->TEST_Pause();
  l2_cache_policy->TEST_WaitAllTaskSleep();
  cache.DisablePolicyMemEvictionHandler();

  {
    // Insert to SSD and Get it
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());

    auto ssd_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "foo");
    EXPECT_EQ(nullptr, ssd_handle);
    cache.Release(ssd_handle);

    auto insert_result =
        cache.TEST_Insert(UnifiedCache::CacheInstanceType::kSSD, "foo",
                          *foo_buf, foo_data.size());

    EXPECT_TRUE(insert_result.IsOk());
    EXPECT_NE(nullptr, insert_result.Get());
    auto unified_handle = cache.Acquire("foo");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);

    ssd_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "foo");
    EXPECT_NE(nullptr, ssd_handle);
    cache.Release(ssd_handle);

    // delete in SSD
    cache.TEST_Remove(UnifiedCache::CacheInstanceType::kSSD, "foo");
    // We don't need this because ZonedStore did softdel.
    // ssd_handle =
    //     cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "foo");
    // EXPECT_EQ(nullptr, ssd_handle);
    // cache.Release(ssd_handle);
  }
}

TEST_P(UnifiedCacheTestFixture, SSDCacheOnly) {
  CacheOptions local_opts = opts_;
  local_opts.cache_ssd_instance_only = true;
  UnifiedCache cache(local_opts);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 32);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  EXPECT_TRUE(start_res);
  // Remove All Data
  cache.RemoveAll();
  // Stop L2 Cache Migrate
  auto l2_cache_policy = cache.l2_cache_policy();
  CHECK_NOTNULL(l2_cache_policy);
  l2_cache_policy->TEST_Pause();
  l2_cache_policy->TEST_WaitAllTaskSleep();
  cache.DisablePolicyMemEvictionHandler();

  {
    // Insert and Get it
    std::string foo_data = "12345";
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());

    cache.Insert("foo", *foo_buf, foo_data.size());

    auto unified_handle = cache.Acquire("foo");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);

    auto ssd_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "foo");
    EXPECT_NE(nullptr, ssd_handle);
    cache.Release(ssd_handle);

    auto dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_EQ(nullptr, dram_handle);

    auto pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);

    // InsertPinned the data
    std::string bar_data = "11111";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    unified_handle = cache.InsertPinned("bar", *bar_buf, bar_data.size());

    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);

    ssd_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "foo");
    EXPECT_NE(nullptr, ssd_handle);
    cache.Release(ssd_handle);

    dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "foo");
    EXPECT_EQ(nullptr, dram_handle);

    pmem_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
    EXPECT_EQ(nullptr, pmem_handle);

    // delete in SSD
    cache.Remove("foo");
    // We don't need this because ZonedStore did softdel.
    // unified_handle = cache.Acquire("foo");
    // EXPECT_EQ(nullptr, unified_handle);

    cache.Remove("bar");
    // We don't need this because ZonedStore did softdel.
    // ssd_handle =
    //     cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "bar");
    // EXPECT_EQ(nullptr, ssd_handle);
  }
}

TEST_P(UnifiedCacheTestFixture, L2CacheMigration) {
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 16);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 0);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  cache.SetReplacementPolicyType(UnifiedCache::CacheInstanceType::kDRAM,
                                 ReplacementPolicyType::kSLRU);
  auto start_res = cache.Start();
  EXPECT_TRUE(start_res);
  // Remove All Data
  cache.RemoveAll();
  // Stop L2 Cache Migrate
  auto l2_cache_policy = cache.l2_cache_policy();
  CHECK_NOTNULL(l2_cache_policy);
  l2_cache_policy->TEST_Pause();
  l2_cache_policy->TEST_WaitAllTaskSleep();

  {
    // Insert to DRAM, trigger migration, Get it from SSD
    std::string bar_data = "67890";
    auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
    cache.Insert("bar", *bar_buf, bar_data.size());
    auto unified_handle = cache.Acquire("bar");
    EXPECT_NE(nullptr, unified_handle);
    cache.Release(unified_handle);

    auto dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "bar");
    EXPECT_NE(nullptr, dram_handle);
    cache.Release(dram_handle);

    // not exist
    auto ssd_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "bar");
    EXPECT_EQ(nullptr, ssd_handle);
    cache.Release(ssd_handle);

    // continue task & wait for migrate
    l2_cache_policy->TEST_Continue();
    sleep(4);

    dram_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kDRAM, "bar");
    EXPECT_NE(nullptr, dram_handle);
    cache.Release(dram_handle);

    // exist in SSD
    ssd_handle =
        cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "bar");
    EXPECT_NE(nullptr, ssd_handle);
    cache.Release(ssd_handle);

    // delete in SSD
    cache.TEST_Remove(UnifiedCache::CacheInstanceType::kSSD, "bar");
    // We don't need this because ZonedStore did softdel.
    // ssd_handle =
    //     cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "bar");
    // EXPECT_EQ(nullptr, ssd_handle);
    // cache.Release(ssd_handle);
  }
}

TEST_P(UnifiedCacheTestFixture,
       AvoidReinsertExistingCacheItemIntoPmemCacheByDramCacheEviction) {
  FLAGS_cache_enable_eviction_handler = true;
  FLAGS_cache_pmem_enable_async_write = true;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 10);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 20);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  EXPECT_TRUE(start_res);
  // Change data placement type to Tiered
  cache.SetDataPlacementType(UnifiedCache::DRAMPMEMDataPlacementType::kTiered);

  // Insert some data
  std::string foo_data = "12345";
  auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
  cache.Insert("foo", *foo_buf, foo_data.size());

  std::string bar_data = "56789";
  auto bar_buf = folly::IOBuf::copyBuffer(bar_data.c_str(), bar_data.size());
  cache.Insert("bar", *bar_buf, bar_data.size());
  // At this point, foo_data is in PMEM cache and bar_data is in DRAM cache

  auto dram_inserts =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.dram.puts"));
  auto pmem_inserts =
      noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
          noodle::MetricId("ti.mtcache.pmem.puts"));

  EXPECT_EQ(dram_inserts->GetValue(), 2);
  EXPECT_EQ(pmem_inserts->GetValue(), 1);

  // Query foo_data again and it will be promote to DRAM cache
  auto acquire_res = cache.Acquire("foo");
  cache.Release(acquire_res);
  // At this point, foo_data has a copy in DRAM cache and a copy in PMEM
  // cache. bar_data has a copy in PMEM cache

  dram_inserts = noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
      noodle::MetricId("ti.mtcache.dram.puts"));
  pmem_inserts = noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
      noodle::MetricId("ti.mtcache.pmem.puts"));

  EXPECT_EQ(dram_inserts->GetValue(), 3);
  EXPECT_EQ(pmem_inserts->GetValue(), 2);

  // Insert car_data to force DRAM cache eviction foo_data
  // But the eviction handler won't insert foo_data into the PMEM cache again
  // because it is already in the PMEM cache
  std::string car_data = "00000";
  auto car_buf = folly::IOBuf::copyBuffer(car_data.c_str(), car_data.size());
  cache.Insert("car", *car_buf, car_data.size());

  dram_inserts = noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
      noodle::MetricId("ti.mtcache.dram.puts"));
  pmem_inserts = noodle::GetGlobalMetricRegistry()->Get<noodle::AtomicCounter>(
      noodle::MetricId("ti.mtcache.pmem.puts"));

  EXPECT_EQ(dram_inserts->GetValue(), 4);
  EXPECT_EQ(pmem_inserts->GetValue(), 2);

  auto stop_res = cache.Stop();
  ASSERT_TRUE(stop_res);
}

TEST_P(UnifiedCacheTestFixture, PromoteFromSSDToPMEM) {
  FLAGS_cache_pmem_enable_async_write = true;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 10);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 2000);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, MIN_SSD_CAPACITY);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);

  auto iobuf = ::folly::IOBuf::create(1000);
  // value size must be larger that 256 so that it will be promoted into PMEM
  // rather than DRAM.
  iobuf->append(1000);
  memset(iobuf->writableData(), 1, 1000);
  {
    auto insert_res = cache.TEST_Insert(UnifiedCache::CacheInstanceType::kSSD,
                                        "foo", *iobuf, 1000);
    ASSERT_TRUE(insert_res.IsOk());
    ASSERT_NE(nullptr, insert_res.Get());
  }
  auto pmem_handle =
      cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
  EXPECT_EQ(nullptr, pmem_handle);

  auto ssd_handle =
      cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kSSD, "foo");
  EXPECT_NE(nullptr, ssd_handle);
  cache.Release(ssd_handle);

  auto unified_handle = cache.Acquire("foo");
  ASSERT_NE(nullptr, unified_handle);
  EXPECT_EQ(unified_handle->value().length(), 1000);
  EXPECT_EQ(memcmp(unified_handle->value().data(), iobuf->data(), 1000), 0);
  cache.Release(unified_handle);

  pmem_handle =
      cache.TEST_Acquire(UnifiedCache::CacheInstanceType::kPMEM, "foo");
  ASSERT_NE(nullptr, pmem_handle);
  EXPECT_EQ(pmem_handle->value().length(), 1000);
  EXPECT_EQ(memcmp(pmem_handle->value().data(), iobuf->data(), 1000), 0);
  cache.Release(pmem_handle);

  auto stop_res = cache.Stop();
  ASSERT_TRUE(stop_res);
}

// This UT tests if evicted cache entries will be missing during the eviction
// These cache entries hangs in L2 write queue and does not reach L2 cache yet.
TEST_P(UnifiedCacheTestFixture, SingleThreadEvictionSidebySide) {
  FLAGS_l2_policy_use_eviction_handler = true;
  FLAGS_cache_enable_eviction_handler = true;
  FLAGS_mtcache_enable_pmem_promotion = false;
  FLAGS_mtcache_enable_ssd_promotion = false;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 128);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 256);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, 4096);
  cache.SetDataPlacementThreshold(10);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  for (int i = 0; i < 16; i++) {
    // Values inserted to DRAM
    std::string dram_foo_data = "12345" + std::to_string(i);
    auto dram_foo_buf =
        folly::IOBuf::copyBuffer(dram_foo_data.c_str(), dram_foo_data.size());
    auto dram_handle = cache.InsertPinned("dram" + std::to_string(i),
                                          *dram_foo_buf, dram_foo_data.size());
    EXPECT_NE(dram_handle, nullptr);
    cache.Release(dram_handle);
    // Values inserted to PMEM
    std::string pmem_foo_data = std::string(20, i + 'A') + std::to_string(i);
    auto pmem_foo_buf =
        folly::IOBuf::copyBuffer(pmem_foo_data.c_str(), pmem_foo_data.size());
    auto pmem_handle = cache.InsertPinned("pmem" + std::to_string(i),
                                          *pmem_foo_buf, pmem_foo_data.size());
    EXPECT_NE(pmem_handle, nullptr);
    cache.Release(pmem_handle);
  }

  for (int i = 0; i < 16; i++) {
    auto unified_handle = cache.Acquire("dram" + std::to_string(i));
    EXPECT_NE(nullptr, unified_handle);
    std::string foo_data = "12345" + std::to_string(i);
    EXPECT_EQ(std::string(
                  reinterpret_cast<const char*>(unified_handle->value().data()),
                  unified_handle->value().length()),
              foo_data);
    cache.Release(unified_handle);
  }

  for (int i = 0; i < 16; i++) {
    auto unified_handle = cache.Acquire("pmem" + std::to_string(i));
    EXPECT_NE(nullptr, unified_handle);
    std::string foo_data = std::string(20, i + 'A') + std::to_string(i);
    EXPECT_EQ(std::string(
                  reinterpret_cast<const char*>(unified_handle->value().data()),
                  unified_handle->value().length()),
              foo_data);
    cache.Release(unified_handle);
  }
  std::vector<std::thread> readers;

  auto stop_res = cache.Stop();
  ASSERT_TRUE(stop_res);
  FLAGS_mtcache_enable_pmem_promotion = false;
}

// Similar to the above UT but uses Tiered setting
TEST_P(UnifiedCacheTestFixture, MultiThreadedEvictionTiered) {
  FLAGS_l2_policy_use_eviction_handler = true;
  FLAGS_cache_enable_eviction_handler = true;
  FLAGS_mtcache_enable_pmem_promotion = false;
  FLAGS_mtcache_enable_ssd_promotion = false;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 128);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 256);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, 4096);
  cache.SetDataPlacementType(UnifiedCache::DRAMPMEMDataPlacementType::kTiered);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  for (int i = 0; i < 16; i++) {
    std::string foo_data = std::string(16, i + 'A') + std::to_string(i);
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    auto unified_handle = cache.InsertPinned("foo" + std::to_string(i),
                                             *foo_buf, foo_data.size());
    EXPECT_NE(unified_handle, nullptr);
    cache.Release(unified_handle);
  }

  std::vector<std::thread> readers;
  const int thread_num = 16;
  for (auto i = 0; i < thread_num; ++i) {
    readers.emplace_back([&]() {
      for (int i = 0; i < 16; i++) {
        auto unified_handle = cache.Acquire("foo" + std::to_string(i));
        EXPECT_NE(nullptr, unified_handle);
        std::string foo_data = std::string(16, i + 'A') + std::to_string(i);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(
                                  unified_handle->value().data()),
                              unified_handle->value().length()),
                  foo_data);
        cache.Release(unified_handle);
      }
    });
  }
  for (auto& worker : readers) {
    worker.join();
  }

  auto stop_res = cache.Stop();
  ASSERT_TRUE(stop_res);
  FLAGS_mtcache_enable_pmem_promotion = false;
}
// Enables promotion and tiered setting, promotion cause more eviction on L1
// cache. There should be no cache miss as well.
TEST_P(UnifiedCacheTestFixture, MultiThreadedAsyncPromotion) {
  FLAGS_l2_policy_use_eviction_handler = true;
  FLAGS_cache_enable_eviction_handler = true;
  FLAGS_mtcache_enable_pmem_promotion = false;
  FLAGS_mtcache_enable_ssd_promotion = true;
  UnifiedCache cache(opts_);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kDRAM, 128);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kPMEM, 256);
  cache.SetCapacity(UnifiedCache::CacheInstanceType::kSSD, 2048);
  cache.SetDataPlacementType(UnifiedCache::DRAMPMEMDataPlacementType::kTiered);
  auto start_res = cache.Start();
  ASSERT_TRUE(start_res);
  for (int i = 0; i < 16; i++) {
    std::string foo_data = std::string(16, i + 'A') + std::to_string(i);
    auto foo_buf = folly::IOBuf::copyBuffer(foo_data.c_str(), foo_data.size());
    auto unified_handle = cache.InsertPinned("foo" + std::to_string(i),
                                             *foo_buf, foo_data.size());
    EXPECT_NE(unified_handle, nullptr);
    cache.Release(unified_handle);
  }

  std::vector<std::thread> readers;
  const int thread_num = 16;
  for (auto i = 0; i < thread_num; ++i) {
    readers.emplace_back([&]() {
      for (int i = 0; i < 16; i++) {
        auto unified_handle = cache.Acquire("foo" + std::to_string(i));
        EXPECT_NE(nullptr, unified_handle);
        std::string foo_data = std::string(16, i + 'A') + std::to_string(i);
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(
                                  unified_handle->value().data()),
                              unified_handle->value().length()),
                  foo_data);
        cache.Release(unified_handle);
      }
    });
  }
  for (auto& worker : readers) {
    worker.join();
  }

  auto stop_res = cache.Stop();
  ASSERT_TRUE(stop_res);
  FLAGS_mtcache_enable_pmem_promotion = false;
}

INSTANTIATE_TEST_SUITE_P(UnifiedCacheTest, UnifiedCacheTestFixture,
                         testing::Values("SLRU", "FIFO"));
}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
