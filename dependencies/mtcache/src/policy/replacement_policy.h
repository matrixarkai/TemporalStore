#pragma once

#include "buffer/cache_buffer.h"
#include "storage/storage_engine.h"

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>

namespace mtcache {

// ReplacementPolicyType is an enum definition of all cache replacement policies
// supported by MTCache.
enum class ReplacementPolicyType : uint8_t {
  // Simple First In First Out (FIFO) policy
  kFIFO = 0,
  // Classic Least Recently Used (LRU) policy
  kLRU = 1,
  // Segmented LRU (SLRU) policy from Memcached
  kSLRU = 2,
  // Maximum/Invalid Replacement Policy Type
  kMaxCode
};

// ReplacementPolicy is an abstract class that defines the interfaces for a
// CacheInstance to interact with a specific implementation (e.g., FIFO, LRU) of
// cache replacement policy.

class ReplacementPolicy {
 public:
  // This constructor takes two parameters: capacity and replacement policy
  // type. The used_ variable is initialized to zero as there is no cached
  // buffer initially.
  // TODO(david.gong): To support pinning buffers in cache, we need to add an
  // unordered_set to track all pinned buffers
  explicit ReplacementPolicy(size_t capacity)
      : capacity_(capacity), used_(0), initialized_(false) {
    CHECK_GT(capacity_, 0);
  }

  // Default destructor for ReplacementPolicy
  virtual ~ReplacementPolicy() {}

  // Init method creates and initializes data structures used by the
  // implementing replacement policy If init fails, a
  // kReplacementPolicyUninitialized error is returned. The cache instance
  // cannot be used if the replacement policy fails to initialize.
  virtual noodle::Result<void, CacheError> Init() = 0;

  // Reset method clears all the buffers from the index and reset related stats
  // If resets fails, initialized_ is set to false and a
  // kReplacementPolicyUnintialized error is returned. The cache instance should
  // be disabled if Reset() fails.
  virtual noodle::Result<void, CacheError> Reset() = 0;

  // Put method requests the replacement policy to start tracking a new
  // CacheBuffer pointed to by buffer_ptr. If the cache buffer is already
  // tracked by the replacement policy, the access stats of the buffer is
  // updated. If the cache is not full yet, no cache buffer would be evicted, so
  // nothing is returned; Otherwise, a vector of buffer pointers that are
  // evicted by the replacement policy are returned.
  virtual std::vector<CacheBufferSharedPtr> Put(
      CacheBufferSharedPtr buffer_ptr) = 0;

  // UpdateCacheBuffer method updates an old CacheBuffer with key with the
  // new buffer_ptr if the raw pointer in old buffer is the same as
  // old_data_ptr. This method should ONLY be used by GC and AsyncPut. It will
  // NOT update the access stats of the cachebuffer. If the key cannot be found
  // in the index of the replacement policy, a kCacheBufferNotFound error is
  // returned. If the raw data pointer in the old buffer does not match with
  // old_data_ptr, a kCacheReplaceMismatch error is returned.
  virtual noodle::Result<void, CacheError> UpdateCacheBuffer(
      const std::string& key, const char* old_data_ptr,
      CacheBufferSharedPtr buffer_ptr) = 0;

  // Get method tries to find a cache buffer with the specified key. If the
  // buffer is found, a shared pointer to it is returned and its access stats
  // are updated; otherwise, an empty CacheBuffer shared pointer is returned.
  virtual CacheBufferSharedPtr Get(const std::string& key) = 0;

  // Peek method tries to find a cache buffer with the specified key. Different
  // from Get method, the access stats of the buffer will not be updated and it
  // has no impact on later eviction decisions.
  virtual CacheBufferSharedPtr Peek(const std::string& key) = 0;

  // Delete method requests the replacement policy to stop tracking a cache
  // buffer with the specified key. If the key is found, a pointer to the cache
  // buffer is returned; otherwise, an empty shared pointer is returned.
  virtual CacheBufferSharedPtr Delete(const std::string& key) = 0;

  // GetCapacity returns the capacity (in bytes) of the replacement policy,
  // which should be equal to the capacity of the cache instance.
  size_t GetCapacity() const { return capacity_; }

  // SetCapacity updates the capacity of the replacement to new_capacity
  virtual void SetCapacity(size_t new_capacity) {
    CHECK_GT(new_capacity, 0);
    capacity_ = new_capacity;
  }

  // GetUsedSpace returns the amount of space already used by the replacement
  // policy. Note space used by key and value of the cached buffers is counted.
  virtual size_t GetUsedSpace() { return used_; }

  // GetFreeSpace returns how much space is left for the replacement policy to
  // install new cache buffers.
  virtual size_t GetFreeSpace() {
    return capacity_ >= used_ ? (capacity_ - used_) : 0;
  }

  // GetItemNumber returns how many items exist by the replacement policy, which
  // can be used to estimate hit rate.
  virtual size_t GetItemNum() = 0;

  // RegisterMemEvictionHandler registers the RegisterMemEvictionHandler function.
  // This function is used when conduct eviction, which guarantees data exist
  // in the index of the lower tiered cache.
  void RegisterMemEvictionHandler(
      std::function<void(CacheBufferSharedPtr)> func) {
    mem_eviction_func_ = func;
  }

 protected:
  // capacity_ is maximum space that keys and values could use in the cache
  // instance.
  size_t capacity_{0};

  // used_ is the capacity already used by keys and values in the storage
  // engine, this number is atomic as multiple read/write threads may update it
  // simulateneously.
  std::atomic<size_t> used_{0};

  // initialized_ is the state whether the replacement policy has been
  // initialized successfully.
  bool initialized_{false};

  // This function is used in eviction, which guarantees data exist
  // in the index of the lower tiered cache.
  std::function<void(CacheBufferSharedPtr)> mem_eviction_func_;
};

}  // namespace mtcache
