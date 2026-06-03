#include "allocator/simple_allocator.h"

#include <gtest/gtest.h>

namespace mtcache {

TEST(SimpleAllocator, AllocateAndFree) {
  SimpleLogBasedMemoryAllocator allocator;
  auto ptr_res = allocator.Allocate(3);
  ASSERT_TRUE(ptr_res.IsOk());
  char* ptr = ptr_res.Get();
  ptr[0] = 'A';
  ptr[1] = 'B';
  ptr[2] = '\0';
  auto free_res = allocator.Free(ptr, 3);
  ASSERT_TRUE(free_res.IsOk());
}

}  // namespace mtcache
