#pragma once

#include "allocator/allocator.h"

#include <gtest/gtest.h>

#include <mutex>
#include <unordered_map>

namespace mtcache {

class LogBasedAllocatorGCEventListenerMock
    : public LogBasedAllocatorGCEventListener {
 public:
  ~LogBasedAllocatorGCEventListenerMock() override = default;

  noodle::Result<void, CacheError> OnGCCopy(const char* old_ptr,
                                            const char* new_ptr) override {
    std::unique_lock<std::mutex> lk(mtx_);
    auto it = ptr2key_map_.find(old_ptr);
    if (it == ptr2key_map_.end()) {
      return &Errors::kAllocatorGCEventListenerGCCopyEventFailed;
    }

    std::string& key = it->second;
    EXPECT_TRUE(key2ptr_map_.at(key) == old_ptr);
    key2ptr_map_[key] = new_ptr;
    ptr2key_map_[new_ptr] = std::move(key);
    ptr2key_map_.erase(it);
    lk.unlock();

    auto free_res = alloc_->Free(const_cast<char*>(old_ptr), 0);
    EXPECT_TRUE(free_res.IsOk());
    return {};
  }

  std::string GetInternalMap(const std::string& key) {
    std::lock_guard<std::mutex> guard(mtx_);
    auto it = key2ptr_map_.find(key);
    if (it == key2ptr_map_.end()) {
      return {};
    }
    return {it->second};
  }

  const char* SetInternalMapAndReturnOldPtr(const std::string& key,
                                            const char* new_ptr) {
    std::lock_guard<std::mutex> guard(mtx_);
    auto& ptr = key2ptr_map_[key];
    const char* old_ptr = ptr;
    if (old_ptr != nullptr) {
      ptr2key_map_.erase(old_ptr);
    }

    ptr = new_ptr;
    ptr2key_map_[new_ptr] = key;
    return old_ptr;
  }

  const char* DelInternalMapAndReturnOldPtr(const std::string& key) {
    std::lock_guard<std::mutex> guard(mtx_);
    auto it = key2ptr_map_.find(key);
    if (it == key2ptr_map_.end()) {
      return nullptr;
    }

    const char* ptr = it->second;
    ptr2key_map_.erase(ptr);
    key2ptr_map_.erase(it);
    return ptr;
  }

 private:
  LogBasedMemoryAllocator* alloc_ = nullptr;
  std::mutex mtx_;
  std::unordered_map<std::string, const char*> key2ptr_map_;
  std::unordered_map<const char*, std::string> ptr2key_map_;

  template <typename AllocatorType>
  friend class LogBasedMemoryAllocatorTest;
  friend class MockLogBasedAllocGC;
};

}  // namespace mtcache
