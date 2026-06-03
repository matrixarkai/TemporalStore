#pragma once

#include "storage/storage_engine.h"

#include <unordered_map>

namespace mtcache {

// Mock the GCCopyCallback for storage_dram_test and storage_pmem_test
class GCCopyCallbackMock : public StorageEngine::GCCopyCallback {
 public:
  virtual ~GCCopyCallbackMock() {
    // free the memory used by CacheBuffer
    for (auto& it : map_) {
      it.second.reset();
    }
  }

  noodle::Result<void, CacheError> Update(
      const std::string& key, const char* old_data_ptr,
      CacheBufferSharedPtr new_buffer) override {
    auto it = map_.find(key);
    if (it == map_.end()) {
      return &Errors::kCacheBufferNotFound;
    }
    CacheBufferSharedPtr old_buffer = it->second;
    const char* old_ptr = old_buffer->Data();
    if (old_ptr != old_data_ptr) {
      return &Errors::kCacheReplaceMismatch;
    }
    // replace old cache buffer with new cache buffer
    map_[key] = std::move(new_buffer);
    return {};
  }

  bool DeleteCacheBuffer(const std::string& key) {
    auto it = map_.find(key);
    if (it != map_.end()) {
      CacheBufferSharedPtr buffer_ptr = it->second;
      map_.erase(it);
      return true;
    }
    return false;
  }

  bool AddCacheBuffer(const std::string& key, CacheBufferSharedPtr buffer) {
    auto it = map_.find(key);
    if (it != map_.end()) {
      // The cache buffer has existed
      return false;
    }
    map_[key] = std::move(buffer);
    return true;
  }

  CacheBufferSharedPtr GetCacheBuffer(const std::string& key) {
    auto it = map_.find(key);
    if (it != map_.end()) {
      // The cache buffer has existed
      return it->second;
    }
    return CacheBufferSharedPtr();
  }

 private:
  std::unordered_map<std::string, CacheBufferSharedPtr> map_;
};

}  // namespace mtcache
