#include "storage/pmem_storage.h"

#include "common/numa_utils.h"
#include "gc_copy_callback_mock.h"
#include "storage/mem_storage.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <stdlib.h>

// Fragmentation rate to tigger gc
DECLARE_int32(fragmentation_ratio_max);
DECLARE_int32(gc_check_interval);
// The number of NUMA nodes used by the system
DECLARE_int32(used_num_numa_nodes);
DECLARE_uint64(cache_pmem_gc_reserved);

namespace mtcache {

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

class StorageEnginePMemTest : public ::testing::Test {
 protected:
  static void TearDownTestCase() { CacheExecutor::DestroyAllExecutors(); }

  void SetUp() override {
    char path[64] = "/tmp/mtcache_storage_pmem_test_XXXXXX";
    char path1[64] = "/tmp/mtcache_storage1_pmem_test_XXXXXX";
    if (mkdtemp(path) == nullptr) {
      LOG(FATAL) << "create tmp_dir failed, errno=" << errno;
    }
    if (mkdtemp(path1) == nullptr) {
      LOG(FATAL) << "create tmp_dir1 failed, errno=" << errno;
    }
    pmem_path = std::string(path);
    pmem_path1 = std::string(path1);

    cb_ = new GCCopyCallbackMock();
    registry_ = noodle::GetMetricRegistry("ti.mtcache.pmem_storage");
    // Disable NUMA-awareness in UTs.
    FLAGS_used_num_numa_nodes = 1;
    FLAGS_cache_pmem_gc_reserved = 10ULL * 1024 * 1024;
    NumaInfo::Init();
  }

  void TearDown() override {
    std::filesystem::remove_all(pmem_path);
    std::filesystem::remove_all(pmem_path1);
    delete cb_;
    noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.pmem_storage");
  }

  GCCopyCallbackMock* cb_{nullptr};
  std::string pmem_path;
  std::string pmem_path1;
  uint64_t pmem_capacity = 2 * kLogChunkSize;
  std::shared_ptr<noodle::MetricRegistry> registry_;
};

TEST_F(StorageEnginePMemTest, LogEnginePutGetAndDelete) {
  // we use an independent engine because we will check the allocator stats
  // of this engine.
  StorageEnginePMem* engine =
      new StorageEnginePMem(pmem_capacity, {pmem_path}, cb_, registry_);
  ASSERT_TRUE(engine->Start());

  // Put method test
  std::string data = "StorageEnginePMEM";
  std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
  auto put_res = engine->Put("key1", std::move(*value));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer = std::move(put_res).Get();
  EXPECT_EQ(data.size(), buffer->Size());
  EXPECT_EQ(0, memcmp(data.c_str(), buffer->Data(), buffer->Size()));

  auto fut = engine->AsyncPut(
      buffer, [&data](noodle::Result<CacheBufferSharedPtr, CacheError> res) {
        ASSERT_TRUE(res.IsOk());
        auto new_buf = std::move(res).Get();
        EXPECT_EQ(new_buf->Key(), std::string("key1"));
        EXPECT_EQ(std::string(new_buf->Data(), new_buf->Size()), data);
      });

  auto async_res = std::move(fut).get();
  ASSERT_TRUE(async_res.IsOk());
  auto buf_new = std::move(async_res).Get();
  EXPECT_NE(buf_new, nullptr);
  EXPECT_NE(buf_new, buffer);
  buf_new.reset();

  // Get method test
  auto get_res = engine->Get("key1");
  EXPECT_FALSE(get_res.IsOk());
  EXPECT_EQ(get_res.GetError(), &Errors::kStorageUnsupported);

  // When buffer is deleted, its ref count will drops to zero and
  // StorageEngine::AsyncDelete will be called.
  buffer.reset();

  ASSERT_TRUE(engine->Stop());
  delete engine;
}

TEST_F(StorageEnginePMemTest, LogEngineGCEventListener) {
  StorageEnginePMem* engine =
      new StorageEnginePMem(pmem_capacity, {pmem_path}, cb_, registry_);
  LogBasedAllocatorGCEventListenerBase listener(engine, cb_, true);

  ASSERT_TRUE(engine->Start());

  std::string data = "StorageEnginePMEM2";
  std::string key = "key2";
  std::string wrong_key = "key3";
  std::unique_ptr<folly::IOBuf> value1 = folly::IOBuf::copyBuffer(data);
  std::unique_ptr<folly::IOBuf> value2 = folly::IOBuf::copyBuffer(data);
  std::unique_ptr<folly::IOBuf> value3 = folly::IOBuf::copyBuffer(data);

  auto put_res = engine->Put("key2", std::move(*value1));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer2 = std::move(put_res).Get();
  cb_->AddCacheBuffer("key2", buffer2);

  // We use StorageEngine::Put to emulate the allocator copy process.
  // Note that Put method will call Seal() of the internal allocator, which
  // will increase the ref count of the chunk in allocator. But this will
  // not affect the result as the storage engine will be destroyed soon.
  put_res = engine->Put("key2", std::move(*value2));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer2_copy = std::move(put_res).Get();

  put_res = engine->Put("key3", std::move(*value3));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer3 = std::move(put_res).Get();

  // please refer to StorageEnginePMem::Put for data layout
  uint32_t kKeyLen = sizeof(uint32_t);
  uint32_t kValueLen = sizeof(uint32_t);
  uint32_t kHeadLen = kKeyLen + kValueLen;

  // The size for key (i.e. "key2") is 4
  auto* alloc = engine->TEST_GetLogAllocator(0);
  ASSERT_TRUE(alloc != nullptr);
  auto alloc_res = alloc->Allocate(kHeadLen + data.size() + 4);
  ASSERT_TRUE(alloc_res.IsOk());
  char* mem = alloc_res.Get();
  ASSERT_NE(mem, nullptr);
  uint32_t sz = data.size();
  memcpy(mem, &sz, kValueLen);
  sz = key.size();
  memcpy(mem + kValueLen, &sz, kKeyLen);
  memcpy(mem + kHeadLen, data.data(), data.size());
  memcpy(mem + kHeadLen + data.size(), key.data(), key.size());
  alloc->Seal(mem);

  auto cb_res = listener.OnGCCopy(buffer3->Data() - kHeadLen, mem);
  EXPECT_FALSE(cb_res.IsOk());
  EXPECT_EQ(cb_res.GetError(), &Errors::kCacheBufferNotFound);

  cb_res = listener.OnGCCopy(buffer2_copy->Data() - kHeadLen, mem);
  EXPECT_FALSE(cb_res.IsOk());
  EXPECT_EQ(cb_res.GetError(), &Errors::kCacheReplaceMismatch);

  cb_res = listener.OnGCCopy(buffer2->Data() - kHeadLen, mem);
  EXPECT_TRUE(cb_res.IsOk());
  auto buffer2_new = cb_->GetCacheBuffer("key2");
  ASSERT_NE(buffer2_new, nullptr);
  EXPECT_EQ(buffer2_new->Data() - kHeadLen, mem);

  // this will free 'mem'
  bool del_res = cb_->DeleteCacheBuffer("key2");
  ASSERT_TRUE(del_res);
  buffer2_new.reset();
  buffer2.reset();
  buffer2_copy.reset();
  buffer3.reset();

  ASSERT_TRUE(engine->Stop());
  delete engine;
}

TEST_F(StorageEnginePMemTest, NotImplementedMethods) {
  StorageEnginePMem engine(pmem_capacity, {pmem_path}, cb_, registry_);
  auto reset_res = engine.Reset();
  EXPECT_TRUE(reset_res.IsOk());
}

TEST_F(StorageEnginePMemTest, LogEngineVerifyGC) {
  StorageEnginePMem* engine =
      new StorageEnginePMem(pmem_capacity, {pmem_path}, cb_, registry_);

  ASSERT_TRUE(engine->Start());

  auto* alloc = engine->TEST_GetLogAllocator(0);
  auto stats_res_init = alloc->GetStats();
  ASSERT_TRUE(stats_res_init.IsOk());
  const AllocatorStats& alloc_stats_init = stats_res_init.Get();
  ASSERT_EQ(alloc_stats_init.num_allocated_bytes, 0);
  ASSERT_EQ(alloc_stats_init.num_freed_bytes, 0);
  ASSERT_EQ(alloc_stats_init.num_occupied_bytes, 0);

  std::string data(3145728, 'a');
  std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
  auto put_res = engine->Put("key1", std::move(*value));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer = std::move(put_res).Get();

  std::string data2(3145728, 'b');
  std::unique_ptr<folly::IOBuf> value2 = folly::IOBuf::copyBuffer(data2);
  auto put_res2 = engine->Put("key2", std::move(*value2));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer2 = std::move(put_res2).Get();

  auto stats_res_before = alloc->GetStats();
  ASSERT_TRUE(stats_res_before.IsOk());
  const AllocatorStats& alloc_stats_before = stats_res_before.Get();
  // please refer to StorageEnginePMem::Put for data layout
  uint32_t kKeyLen = sizeof(uint32_t);
  uint32_t kValueLen = sizeof(uint32_t);
  uint32_t kHeadLen = kKeyLen + kValueLen;
  auto data_size = kHeadLen + data.size() + 4 + 4;
  auto data_size2 = kHeadLen + data2.size() + 4 + 4;

  ASSERT_EQ(alloc_stats_before.num_allocated_bytes, data_size + data_size2);
  ASSERT_EQ(alloc_stats_before.num_freed_bytes, 0);

  // force to tigger gc
  // Any delete will trigger gc
  FLAGS_fragmentation_ratio_max = 0;

  buffer.reset();
  buffer2.reset();

  // wait for gc task
  // GC interval is FLAGS_gc_check_interval, so waiting for extra 3 seconds
  // should be enough.
  LOG(INFO) << "Waiting " << (FLAGS_gc_check_interval / 1000 + 3)
            << "s for gc to complete...";
  std::this_thread::sleep_for(
      std::chrono::milliseconds(FLAGS_gc_check_interval + 3000));

  auto stats_res_after = alloc->GetStats();
  ASSERT_TRUE(stats_res_after.IsOk());
  const AllocatorStats& alloc_stats_after = stats_res_after.Get();

  // Test whether the GC related gauges match with the stats directly returned
  // by allocator.
  auto gc_pmem_occupied_bytes_gauge = registry_->Get<noodle::AtomicGauge>(
      noodle::MetricId("numa0_occupied_bytes"));
  auto gc_pmem_allocated_bytes_gauge = registry_->Get<noodle::AtomicGauge>(
      noodle::MetricId("numa0_allocated_bytes"));
  auto gc_pmem_freed_bytes_gauge = registry_->Get<noodle::AtomicGauge>(
      noodle::MetricId("numa0_freed_bytes"));
  auto gc_pmem_alloc_failed_count_gauge = registry_->Get<noodle::AtomicGauge>(
      noodle::MetricId("numa0_failed_alloc_count"));

  EXPECT_EQ(gc_pmem_occupied_bytes_gauge->GetValue(),
            alloc_stats_after.num_occupied_bytes);
  EXPECT_EQ(gc_pmem_allocated_bytes_gauge->GetValue(),
            alloc_stats_after.num_allocated_bytes);
  EXPECT_EQ(gc_pmem_freed_bytes_gauge->GetValue(),
            alloc_stats_after.num_freed_bytes);

  ASSERT_TRUE(engine->Stop());

  delete engine;
}

TEST_F(StorageEnginePMemTest, PoolEnginePutGetAndDelete) {
  // we use an independent engine because we will check the allocator stats
  // of this engine.
  // object len is 1024 bytes
  // StorageEnginePMem* engine;

  // PMEM storage engine does not support pool-based allocator for now because
  // pool-based allocator does now support numa-aware feature.
  //
  // TODO(dbc) Add it back when pool-based allocator support numa-aware.
  // EXPECT_DEATH(new StorageEnginePMem(pmem_capacity, {pmem_path}, 1024), "");
  /*
  ASSERT_TRUE(engine->Start());

  // Put method test
  std::string data = "StorageEnginePMEM";
  std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
  auto put_res = engine->Put("key1", std::move(value));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer = std::move(put_res).Get();
  EXPECT_EQ(data.size(), buffer->Size());
  EXPECT_EQ(0, memcmp(data.c_str(), buffer->Data(), buffer->Size()));

  // Get method test
  auto get_res = engine->Get("key1");
  EXPECT_FALSE(get_res.IsOk());
  EXPECT_EQ(get_res.GetError(), &Errors::kStorageUnsupported);

  auto* alloc = engine->TEST_GetPoolAllocator(0);
  ASSERT_TRUE(alloc != nullptr);
  auto stats_res_before = alloc->GetStats();
  ASSERT_TRUE(stats_res_before.IsOk());
  const AllocatorStats& alloc_stats_before = stats_res_before.Get();
  // one object len is 1024 bytes
  EXPECT_EQ(alloc_stats_before.num_allocated_bytes, 1024);

  // When buffer is deleted, its ref count will drops to zero and
  // StorageEngine::Delete will be called.
  buffer.reset();

  auto stats_res_after = alloc->GetStats();
  ASSERT_TRUE(stats_res_after.IsOk());
  const AllocatorStats& alloc_stats_after = stats_res_after.Get();

  // one object len is 1024 bytes
  EXPECT_EQ(alloc_stats_after.num_allocated_bytes, 0);
  EXPECT_EQ(alloc_stats_after.num_freed_bytes,
            alloc_stats_before.num_freed_bytes + 1024);

  ASSERT_TRUE(engine->Stop());
  delete engine;
  */
}

#pragma GCC diagnostic pop

}  // namespace mtcache
