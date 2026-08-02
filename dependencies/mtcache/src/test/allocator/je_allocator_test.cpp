#include "allocator/je_allocator.h"

#include <gtest/gtest.h>

namespace mtcache {

class JeAllocatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    registry_ = noodle::GetMetricRegistry("ti.mtcache.je_allocator_test");
  }

  void TearDown() override {
    noodle::GetGlobalMetricRegistry()->Deregister(
        "ti.mtcache.je_allocator_test");
  }

  size_t capacity_ = 4 * 1024;
  std::shared_ptr<noodle::MetricRegistry> registry_;
};

TEST_F(JeAllocatorTest, Basic) {
  JeAllocator allocator(capacity_, registry_);
  auto alloc_res = allocator.Allocate(1024);
  ASSERT_TRUE(alloc_res.IsOk());
  char* ptr = alloc_res.Get();
  ASSERT_NE(ptr, nullptr);
  auto stat_res = allocator.GetStats();
  ASSERT_TRUE(stat_res.IsOk());
  auto stats = std::move(stat_res).Get();
  EXPECT_EQ(stats.num_allocated_bytes, 1024);
  EXPECT_EQ(stats.num_freed_bytes, 0);
  EXPECT_EQ(stats.num_occupied_bytes, 1024);
  ASSERT_TRUE(allocator.Seal(ptr).IsOk());

  alloc_res = allocator.Allocate(4096);
  ASSERT_FALSE(alloc_res.IsOk());
  ASSERT_EQ(alloc_res.GetError(), &Errors::kAllocatorOutOfSpace);

  auto free_res = allocator.Free(ptr, 1024);
  ASSERT_TRUE(free_res.IsOk());
  stat_res = allocator.GetStats();
  ASSERT_TRUE(stat_res.IsOk());
  stats = std::move(stat_res).Get();
  EXPECT_EQ(stats.num_allocated_bytes, 1024);
  EXPECT_EQ(stats.num_freed_bytes, 1024);
  EXPECT_EQ(stats.num_occupied_bytes, 0);
}

}  // namespace mtcache

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
