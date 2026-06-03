#include "allocator/dram_allocators.h"
#include "allocator/pmem_allocators.h"
#include "common/logging.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <vector>

namespace mtcache {

class PoolBasedMemoryAllocatorPMemInstantFlushPolicy
    : public PoolBasedMemoryAllocatorPMem {
 public:
  using PoolBasedMemoryAllocatorPMem::PoolBasedMemoryAllocatorPMem;
};

static std::unique_ptr<PoolBasedMemoryAllocatorDram> CreateAllocator(
    PoolBasedMemoryAllocatorDram*) {
  return std::make_unique<PoolBasedMemoryAllocatorDram>(1 << 28 /* 256 MB*/,
                                                        100, 1 << 12);
}

static std::unique_ptr<PoolBasedMemoryAllocatorPMem> CreateAllocator(
    PoolBasedMemoryAllocatorPMem*) {
  return std::make_unique<PoolBasedMemoryAllocatorPMem>(
      "/tmp", FlushPolicy::kNoFlush, 1 << 28 /* 256 MB*/, 100, 1 << 12);
}

static std::unique_ptr<PoolBasedMemoryAllocatorPMemInstantFlushPolicy>
CreateAllocator(PoolBasedMemoryAllocatorPMemInstantFlushPolicy*) {
  return std::make_unique<PoolBasedMemoryAllocatorPMemInstantFlushPolicy>(
      "/tmp", FlushPolicy::kInstantFlush, 1 << 28 /* 256 MB*/, 100, 1 << 12);
}

template <typename AllocatorType>
class PoolBasedMemoryAllocatorTest : public testing::Test {
 public:
  PoolBasedMemoryAllocatorTest()
      : allocator_(CreateAllocator((AllocatorType*){})) {}

  ~PoolBasedMemoryAllocatorTest() override {
    for (auto& p : std::filesystem::directory_iterator("/tmp")) {
      if (p.path().extension() == ".pmem_chunk") {
        std::filesystem::remove(p.path());
      }
    }
  }

  std::unique_ptr<AllocatorType> allocator_;
};

typedef testing::Types<PoolBasedMemoryAllocatorDram,
                       PoolBasedMemoryAllocatorPMem,
                       PoolBasedMemoryAllocatorPMemInstantFlushPolicy>
    AllocatorImplTypes;
TYPED_TEST_SUITE(PoolBasedMemoryAllocatorTest, AllocatorImplTypes);

TYPED_TEST(PoolBasedMemoryAllocatorTest, AllocateAndFree) {
  auto alloc_res = this->allocator_->Allocate(3);
  ASSERT_TRUE(alloc_res.IsOk());
  char* ptr = alloc_res.Get();
  ptr[0] = 'A';
  ptr[1] = 'B';
  ptr[2] = '\0';
  auto free_res = this->allocator_->Free(ptr, 3);
  ASSERT_TRUE(free_res.IsOk());
}

TYPED_TEST(PoolBasedMemoryAllocatorTest, Capacity) {
  auto cap_res = this->allocator_->Capacity();
  ASSERT_TRUE(cap_res.IsOk());
  ASSERT_EQ(cap_res.Get(), 1 << 28);
}

TYPED_TEST(PoolBasedMemoryAllocatorTest, CapacityLimit) {
  for (size_t i = 0; i < 65537; ++i) {
    auto alloc_res = this->allocator_->Allocate(3 * (1 << 10));
    if (!alloc_res.IsOk()) {
      ASSERT_EQ(alloc_res.GetError(), &Errors::kAllocatorOutOfSpace);
      return;
    }
  }
  // Should not reach here
  ASSERT_TRUE(false);
}

TYPED_TEST(PoolBasedMemoryAllocatorTest, AllocationSizeLimit) {
  auto alloc_res = this->allocator_->Allocate(1 << 12);
  if (!alloc_res.IsOk()) {
    ASSERT_EQ(alloc_res.GetError(), &Errors::kAllocatorRequestTooLarge);
    return;
  }
  // Should not reach here
  ASSERT_TRUE(false);
}

TYPED_TEST(PoolBasedMemoryAllocatorTest, ObjectReuse) {
  // Allocate and free
  for (size_t i = 0; i < 2; ++i) {
    auto alloc_res = this->allocator_->Allocate(1 << 10);
    ASSERT_TRUE(alloc_res.IsOk());
    auto seal_res = this->allocator_->Seal(alloc_res.Get());
    ASSERT_TRUE(seal_res.IsOk());
    auto free_res = this->allocator_->Free(alloc_res.Get(), 0);
    ASSERT_TRUE(free_res.IsOk());
  }
  {
    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    // The chunk size defaults to 4 MB
    ASSERT_EQ(stats.num_freed_bytes, 4 * (1 << 20));
    ASSERT_EQ(stats.num_occupied_bytes, 4 * (1 << 20));
  }
  {
    // Allocate two objects
    auto alloc_res = this->allocator_->Allocate(1 << 10);
    ASSERT_TRUE(alloc_res.IsOk());
    auto seal_res = this->allocator_->Seal(alloc_res.Get());
    ASSERT_TRUE(seal_res.IsOk());
    alloc_res = this->allocator_->Allocate(2 * (1 << 10));
    ASSERT_TRUE(alloc_res.IsOk());
    seal_res = this->allocator_->Seal(alloc_res.Get());
    ASSERT_TRUE(seal_res.IsOk());
  }
  {
    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    // The chunk size defaults to 4 MB
    ASSERT_EQ(stats.num_freed_bytes, 4 * (1 << 20) - 2 * (1 << 12));
    ASSERT_EQ(stats.num_occupied_bytes, 4 * (1 << 20));
  }
}

TYPED_TEST(PoolBasedMemoryAllocatorTest, ObjectCacheRebalance) {
  // The chunk size defaults to 4 MB
  // The object length defaults to 4 KB
  // // The preload size defaults to (chunk size / object len) 1024

  // Allocate 101 objects (occupies two chunks)
  std::vector<char*> allocated_address;
  {
    for (size_t i = 0; i < 1025; ++i) {
      auto alloc_res = this->allocator_->Allocate(3 * (1 << 10));
      ASSERT_TRUE(alloc_res.IsOk());
      auto seal_res = this->allocator_->Seal(alloc_res.Get());
      ASSERT_TRUE(seal_res.IsOk());
      allocated_address.push_back(alloc_res.Get());
    }
  }
  {
    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    ASSERT_EQ(stats.num_freed_bytes, 2 * 4 * (1 << 20) - 1025 * (1 << 12));
    ASSERT_EQ(stats.num_occupied_bytes, 2 * 4 * (1 << 20));
  }
  // Free 1025 objects
  {
    for (size_t i = 0; i < 1025; ++i) {
      auto free_res = this->allocator_->Free(allocated_address[i], 0);
      ASSERT_TRUE(free_res.IsOk());
    }
  }
  // 1024 free objects in the object cache and 100 objects in the global free
  // list
  {
    auto get_stats_res = this->allocator_->GetStats();
    ASSERT_TRUE(get_stats_res.IsOk());
    auto stats = get_stats_res.Get();
    ASSERT_EQ(stats.num_freed_bytes, 4 * (1 << 20));
    ASSERT_EQ(stats.num_occupied_bytes, 2 * 4 * (1 << 20));
    auto num_obj_in_freelist = this->allocator_->TEST_GetGobalFreeListSize();
    ASSERT_EQ(num_obj_in_freelist, 1024);
  }
}

}  // namespace mtcache
