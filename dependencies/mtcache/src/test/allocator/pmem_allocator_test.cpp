#include "allocator/alloc_utils.h"
#include "allocator/pmem_allocators.h"
#include "test/log_allocator_gc_listener_mock.h"

#include <gtest/gtest.h>

#include <filesystem>

namespace mtcache {

class PmemAllocatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registry_ = noodle::GetMetricRegistry("ti.mtcache.pmem_allocator_test");
  }

  void TearDown() override {
    noodle::GetGlobalMetricRegistry()->Deregister(
        "ti.mtcache.pmem_allocator_test");
    std::filesystem::remove_all(test_dir_);
  }

  size_t max_tls_num_ = 10;
  const std::string test_dir_ = "/tmp/mtcache_pmem_allocator_test";
  LogBasedAllocatorGCEventListenerMock gc_listener_;
  std::shared_ptr<noodle::MetricRegistry> registry_;
};

TEST_F(PmemAllocatorTest, ShrinkCapacity) {
  size_t capacity_bytes = 3 * kLogChunkSize;
  size_t gc_reserved_bytes = kLogChunkSize;
  // Each PMEM chunk contains 3 records.
  size_t val_len = kLogChunkSize / 4 + 1;

  auto log_allocator = std::make_unique<LogBasedMemoryAllocatorPMem>(
      test_dir_, FlushPolicy::kNoFlush, 0, &gc_listener_, capacity_bytes,
      gc_reserved_bytes, max_tls_num_, registry_, -1);

  // (3 chunks) * (3 records per chunk) = (9 records)
  for (int32_t i = 0; i < 3 * 3; ++i) {
    auto alloc_res = log_allocator->Allocate(val_len);
    ASSERT_TRUE(alloc_res.IsOk());
    char* ptr = alloc_res.Get();
    ASSERT_TRUE(log_allocator->Seal(ptr).IsOk());
  }

  log_allocator.reset();
  std::vector<std::string> pmem_files = GetPmemFileName(test_dir_);
  EXPECT_EQ(pmem_files.size(), 3);
  std::sort(pmem_files.begin(), pmem_files.end());
  auto max_chunk_name = fmt::format("{:020d}.pmem_chunk", 2);
  EXPECT_EQ(pmem_files[2], max_chunk_name);
  registry_.reset();
  noodle::GetGlobalMetricRegistry()->Deregister(
      "ti.mtcache.pmem_allocator_test");

  registry_ = noodle::GetMetricRegistry("ti.mtcache.pmem_allocator_test");
  // Shrink the capacity from 3*kLogChunkSize to 1*kLogChunkSize
  auto log_allocator_new = std::make_unique<LogBasedMemoryAllocatorPMem>(
      test_dir_, FlushPolicy::kNoFlush, 0, &gc_listener_, kLogChunkSize,
      gc_reserved_bytes, max_tls_num_, registry_, -1);

  // After shrinking the capacity, there are at most 2 chunk files (1 normal +
  // 1 gc).
  std::vector<std::string> pmem_files_new = GetPmemFileName(test_dir_);
  EXPECT_EQ(pmem_files_new.size(), 2);
  std::sort(pmem_files_new.begin(), pmem_files_new.end());
  auto max_chunk_name_new = fmt::format("{:020d}.pmem_chunk", 1);
  EXPECT_EQ(pmem_files_new[1], max_chunk_name_new);
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
