#pragma once

#include "allocator/simple_allocator.h"
#include "storage_engine.h"

#include <folly/io/IOBuf.h>

#include <cstddef>
#include <memory>
#include <string>

namespace mtcache {

class StorageEngineSimple : public StorageEngine {
 public:
  StorageEngineSimple() {
    allocator_ = std::make_unique<SimpleLogBasedMemoryAllocator>();
  }

  ~StorageEngineSimple() override = default;

  bool Start() override {
    initialized_ = true;
    return true;
  }

  bool Stop() override {
    initialized_ = false;
    return true;
  }

  noodle::Result<CacheBufferSharedPtr, CacheError> Put(
      const std::string& key, folly::IOBuf value) override;

  noodle::Result<CacheBufferSharedPtr, CacheError> Get(
      const std::string& key) override;

  noodle::Result<void, CacheError> Delete(CacheBufferPtr buffer) override;

  noodle::Result<void, CacheError> Reset() override;

  noodle::Result<void, CacheError> RecoverData(
      RecoverDataCallback* callback) override;

  // UT interfaces
  uint32_t TEST_GetNumDeleteCompletedCount() {
    return TEST_num_delete_tasks_.load(std::memory_order_acquire);
  }

  void TEST_IncreaseDeleteCompletedCount() {
    TEST_num_delete_tasks_.fetch_add(1, std::memory_order_release);
  }

 private:
  // allocator_ is a SimpleLogBasedMemoryAllocator instance to allocate buffer
  // from DRAM
  std::unique_ptr<SimpleLogBasedMemoryAllocator> allocator_;
  // UT variables
  std::atomic<uint32_t> TEST_num_delete_tasks_{0};
};

}  // namespace mtcache
