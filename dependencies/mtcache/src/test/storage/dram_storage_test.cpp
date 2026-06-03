#include "storage/dram_storage.h"

#include "gc_copy_callback_mock.h"
#include "storage/mem_storage.h"
#include "storage/storage_engine.h"

#include <gtest/gtest.h>

// Fragmentation rate to tigger gc
DECLARE_int32(fragmentation_ratio_max);
DECLARE_int32(gc_check_interval);
DECLARE_string(dram_allocator_type);

namespace mtcache {

class StorageEngineDramTest : public ::testing::Test {
 protected:
  static void TearDownTestCase() { CacheExecutor::DestroyAllExecutors(); }

  void SetUp() override {
    cb_ = new GCCopyCallbackMock();
    registry_ = noodle::GetMetricRegistry("ti.mtcache.dram");
  }

  void TearDown() override {
    delete cb_;
    noodle::GetGlobalMetricRegistry()->Deregister("ti.mtcache.dram");
  }

  GCCopyCallbackMock* cb_{nullptr};
  uint64_t dram_capacity_ = 10ULL * 1024 * 1024 * 1024;
  std::shared_ptr<noodle::MetricRegistry> registry_;
};

TEST_F(StorageEngineDramTest, LogEnginePutGetAndDelete) {
  // we use a independent engine because we will check the allocator stats
  // of this engine.
  StorageEngineDram* engine =
      new StorageEngineDram(dram_capacity_, cb_, registry_);
  ASSERT_TRUE(engine->Start());
  // Put method test
  std::string data = "StorageEngineDRAM";
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

  auto* alloc = engine->TEST_GetLogAllocator();
  ASSERT_TRUE(alloc != nullptr);
  auto stats_res_before = alloc->GetStats();
  ASSERT_TRUE(stats_res_before.IsOk());
  const AllocatorStats& alloc_stats_before = stats_res_before.Get();

  // When buffer is deleted, its ref count will drops to zero and
  // StorageEngine::Delete will be called.
  buffer.reset();

  auto stats_res_after = alloc->GetStats();
  ASSERT_TRUE(stats_res_after.IsOk());
  const AllocatorStats& alloc_stats_after = stats_res_after.Get();

  // please refer to StorageEngineDram::Put for data layout
  uint32_t kKeyLen = sizeof(uint32_t);
  uint32_t kValueLen = sizeof(uint32_t);
  uint32_t kHeadLen = kKeyLen + kValueLen;
  EXPECT_EQ(alloc_stats_after.num_freed_bytes,
            alloc_stats_before.num_freed_bytes + kHeadLen + data.size() +
                4 /*size of 'key1' is 4*/);

  ASSERT_TRUE(engine->Stop());
  delete engine;
}

TEST_F(StorageEngineDramTest, LogEngineGCEventListener) {
  StorageEngineDram* engine =
      new StorageEngineDram(dram_capacity_, cb_, registry_);
  ASSERT_TRUE(engine->Start());
  LogBasedAllocatorGCEventListenerBase listener(engine, cb_, false);

  std::string data = "StorageEngineDRAM2";
  std::string key = "key2";
  std::string wrong_key = "key3";
  std::unique_ptr<folly::IOBuf> value1 = folly::IOBuf::copyBuffer(data);
  std::unique_ptr<folly::IOBuf> value2 = folly::IOBuf::copyBuffer(data);
  std::unique_ptr<folly::IOBuf> value3 = folly::IOBuf::copyBuffer(data);

  auto put_res = engine->Put(key, std::move(*value1));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer2 = std::move(put_res).Get();
  cb_->AddCacheBuffer(key, buffer2);

  // We use StorageEngine::Put to emulate the allocator copy process.
  // Note that Put method will call Seal() of the internal allocator, which
  // will increase the ref count of the chunk in allocator. But this will
  // not affect the result as the storage engine will be destroyed soon.
  put_res = engine->Put(key, std::move(*value2));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer2_copy = std::move(put_res).Get();

  put_res = engine->Put(wrong_key, std::move(*value3));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer3 = std::move(put_res).Get();

  // please refer to StorageEngineDram::Put for data layout
  uint32_t kKeyLen = sizeof(uint32_t);
  uint32_t kValueLen = sizeof(uint32_t);
  uint32_t kHeadLen = kKeyLen + kValueLen;

  // The size for key (i.e. "key2") is 4
  auto* alloc = engine->TEST_GetLogAllocator();
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
  auto buffer2_new = cb_->GetCacheBuffer(key);
  ASSERT_NE(buffer2_new, nullptr);
  EXPECT_EQ(buffer2_new->Data() - kHeadLen, mem);

  auto stats_res_before = alloc->GetStats();
  ASSERT_TRUE(stats_res_before.IsOk());
  const AllocatorStats& alloc_stats_before = stats_res_before.Get();

  // this will free 'mem'
  bool del_res = cb_->DeleteCacheBuffer(key);
  ASSERT_TRUE(del_res);
  buffer2_new.reset();

  auto stats_res_after = alloc->GetStats();
  ASSERT_TRUE(stats_res_after.IsOk());
  const AllocatorStats& alloc_stats_after = stats_res_after.Get();
  EXPECT_EQ(alloc_stats_after.num_freed_bytes,
            alloc_stats_before.num_freed_bytes + kHeadLen + data.size() +
                4 /*size of 'key2' is 4*/);

  buffer2.reset();
  buffer2_copy.reset();
  buffer3.reset();
  ASSERT_TRUE(engine->Stop());
  delete engine;
}

TEST_F(StorageEngineDramTest, EmptyMethods) {
  StorageEngineDram engine(dram_capacity_, cb_, registry_);
  auto reset_res = engine.Reset();
  EXPECT_TRUE(reset_res.IsOk());
}

TEST_F(StorageEngineDramTest, UnsupportedMethods) {
  StorageEngineDram engine(dram_capacity_, cb_, registry_);
  auto recover_res = engine.RecoverData(nullptr);
  EXPECT_FALSE(recover_res.IsOk());
  EXPECT_EQ(recover_res.GetError(), &Errors::kStorageUnsupported);
}

TEST_F(StorageEngineDramTest, LogEngineVerifyGC) {
  StorageEngineDram* engine =
      new StorageEngineDram(dram_capacity_, cb_, registry_);

  ASSERT_TRUE(engine->Start());

  auto* alloc = engine->TEST_GetLogAllocator();
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
  // please refer to StorageEngineDram::PutInternalLogBased for data layout
  uint32_t kKeyLen = sizeof(uint32_t);
  uint32_t kValueLen = sizeof(uint32_t);
  uint32_t kHeadLen = kKeyLen + kValueLen;
  auto data_size = kHeadLen + data.size() + 4;
  auto data_size2 = kHeadLen + data2.size() + 4;

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
  auto gc_dram_occupied_bytes_gauge =
      registry_->Get<noodle::AtomicGauge>(noodle::MetricId("_occupied_bytes"));
  auto gc_dram_allocated_bytes_gauge =
      registry_->Get<noodle::AtomicGauge>(noodle::MetricId("_allocated_bytes"));
  auto gc_dram_freed_bytes_gauge =
      registry_->Get<noodle::AtomicGauge>(noodle::MetricId("_freed_bytes"));

  EXPECT_EQ(gc_dram_occupied_bytes_gauge->GetValue(),
            alloc_stats_after.num_occupied_bytes);
  EXPECT_EQ(gc_dram_allocated_bytes_gauge->GetValue(),
            alloc_stats_after.num_allocated_bytes);
  EXPECT_EQ(gc_dram_freed_bytes_gauge->GetValue(),
            alloc_stats_after.num_freed_bytes);

  ASSERT_TRUE(engine->Stop());
  delete engine;
}

TEST_F(StorageEngineDramTest, PoolEnginePutGetAndDelete) {
  // we use a independent engine because we will check the allocator stats
  // of this engine.
  // object len is 1024 bytes
  FLAGS_dram_allocator_type = "Pool";
  StorageEngineDram* engine =
      new StorageEngineDram(dram_capacity_, nullptr, nullptr);
  ASSERT_TRUE(engine->Start());
  // Put method test
  std::string data = "StorageEngineDRAM";
  std::unique_ptr<folly::IOBuf> value = folly::IOBuf::copyBuffer(data);
  auto put_res = engine->Put("key1", std::move(*value));
  ASSERT_TRUE(put_res.IsOk());
  auto buffer = std::move(put_res).Get();
  EXPECT_EQ(data.size(), buffer->Size());
  EXPECT_EQ(0, memcmp(data.c_str(), buffer->Data(), buffer->Size()));

  // Get method test
  auto get_res = engine->Get("key1");
  EXPECT_FALSE(get_res.IsOk());
  EXPECT_EQ(get_res.GetError(), &Errors::kStorageUnsupported);

  auto* alloc = engine->TEST_GetPoolAllocator();
  ASSERT_TRUE(alloc != nullptr);
  auto stats_res_before = alloc->GetStats();
  ASSERT_TRUE(stats_res_before.IsOk());
  const AllocatorStats& alloc_stats_before = stats_res_before.Get();
  // one object len is 1024 bytes
  EXPECT_EQ(alloc_stats_before.num_allocated_bytes, 1024);

  // When buffer is deleted, its ref count will drops to zero and
  // StorageEngine::Delete(const char*, size_t) will be called.
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
}

}  // namespace mtcache
