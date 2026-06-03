#pragma once

#include "map/cache_map.h"
#include "replacement_policy.h"

#include <folly/concurrency/UnboundedQueue.h>

namespace mtcache {

// ReplacementFIFO implements a first-in, first-out cache replacement policy
// When a new buffer is added to the cache, if the cache is not full yet, the
// buffer is enqueued into the FIFO queue and no buffer is dequeued. If the
// cache is full, the first buffer in the FIFO queue is dequeued and evicted.

class ReplacementFIFO : public ReplacementPolicy {
 public:
  // Constructor instantiate a FIFO replacement policy with the specified
  // capacity.
  explicit ReplacementFIFO(size_t capacity) : ReplacementPolicy(capacity) {
    // TODO(xiao.liu): initialize ConcurrentHashMap with a large size to reduce
    // rehash induced long latency, need to implement a heuristic way to get
    // size number. The current size number is estimated based on cache size,
    // key and value sizes in the microbenchmark.
    index_ = ConcurrentHashMap<std::string, CacheBufferSharedPtr>(33554432, 0);
  }

  // Default destructor
  virtual ~ReplacementFIFO() {}

  // Detailed comments for the overriding functions below could be found in
  // the parent class in cache_replacement_policy.h
  noodle::Result<void, CacheError> Init() override;

  noodle::Result<void, CacheError> Reset() override;

  std::vector<CacheBufferSharedPtr> Put(
      CacheBufferSharedPtr buffer_ptr) override;

  noodle::Result<void, CacheError> UpdateCacheBuffer(
      const std::string& key, const char* old_data_ptr,
      CacheBufferSharedPtr buffer_ptr) override;

  CacheBufferSharedPtr Get(const std::string& key) override;

  CacheBufferSharedPtr Peek(const std::string& key) override;

  CacheBufferSharedPtr Delete(const std::string& key) override;

  size_t GetItemNum() override { return index_.size(); }

 private:
  std::vector<CacheBufferSharedPtr> Evict();

  ConcurrentHashMap<std::string, CacheBufferSharedPtr> index_;

  // TODO(kaiwu.kw) Storing key is not space-efficient. Instead, we can store a
  // pointer to the CacheBufferSharedPtr.
  folly::UMPMCQueue<std::string, true> queue_;
};

}  // namespace mtcache
