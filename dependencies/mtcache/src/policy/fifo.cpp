#include "fifo.h"

#include "common/logging.h"

namespace mtcache {

noodle::Result<void, CacheError> ReplacementFIFO::Init() {
  initialized_ = true;
  return {};
}

noodle::Result<void, CacheError> ReplacementFIFO::Reset() {
  index_.clear();
  while (!queue_.empty()) {
    queue_.dequeue();
  }
  used_ = 0;
  return {};
}

std::vector<CacheBufferSharedPtr> ReplacementFIFO::Evict() {
  std::vector<CacheBufferSharedPtr> evicted;
  while (used_ > capacity_ && !queue_.empty()) {
    const std::string& key = queue_.dequeue();
    // Invalid key found, skip this record
    if (key.size() == 0) continue;

    auto iter = index_.find(key);
    if (iter != index_.end()) {
      index_.map_lock(key);
      auto to_evict = iter->second;
      used_ -= (to_evict->Size() + key.size());
      evicted.emplace_back(to_evict);
      // If there exist a lower tier, we need to insert to its index first
      // to guarantee consistency.
      if (mem_eviction_func_) {
        mem_eviction_func_(to_evict);
      }
      index_.erase(iter);
      index_.map_unlock(key);
    }
  }
  return evicted;
}

std::vector<CacheBufferSharedPtr> ReplacementFIFO::Put(
    CacheBufferSharedPtr buffer_ptr) {
  DCHECK(initialized_);
  CHECK(buffer_ptr != nullptr);
  const std::string& key = buffer_ptr->Key();
  index_.map_lock(key);
  auto result = index_.insert_or_assign(key, buffer_ptr);
  index_.map_unlock(key);
  used_ += (buffer_ptr->Size() + key.size());
  if (result.second) {
    // A new entry is inserted into the cache
    queue_.enqueue(key);
  } else {
    // An existing entry in the cache is updated
    used_ -= (result.first->second->Size() + key.size());
  }
  return Evict();
}

noodle::Result<void, CacheError> ReplacementFIFO::UpdateCacheBuffer(
    const std::string& key, const char* old_data_ptr,
    CacheBufferSharedPtr buffer_ptr) {
  DCHECK(initialized_);
  CHECK(buffer_ptr != nullptr);

  std::unique_lock lock(*index_.get_map_lock(key));
  auto iter = index_.find(key);
  if (UNLIKELY(iter == index_.end())) {
    return &Errors::kCacheBufferNotFound;
  } else {
    auto old_buffer = iter->second;
    if (old_buffer->Data() != old_data_ptr) {
      return &Errors::kCacheReplaceMismatch;
    }
  }

  auto result = index_.assign(key, buffer_ptr);
  CHECK(result.has_value());
  return {};
}

CacheBufferSharedPtr ReplacementFIFO::Get(const std::string& key) {
  return Peek(key);
}

CacheBufferSharedPtr ReplacementFIFO::Peek(const std::string& key) {
  DCHECK(initialized_);
  auto iter = index_.find(key);
  if (iter == index_.end()) {
    return std::shared_ptr<CacheBuffer>();
  }
  return iter->second;
}

CacheBufferSharedPtr ReplacementFIFO::Delete(const std::string& key) {
  DCHECK(initialized_);
  auto iter = index_.find(key);
  if (iter == index_.end()) {
    return std::shared_ptr<CacheBuffer>();
  } else {
    auto buffer = iter->second;
    size_t value_size = buffer->Size();
    index_.map_lock(key);
    size_t erased = index_.erase(key);
    if (erased) {
      used_ -= (value_size + key.size());
    }
    index_.map_unlock(key);
    return buffer;
  }
}

}  // namespace mtcache
