#pragma once

#include "allocator.h"

#include <mutex>
#include <vector>

namespace mtcache {

// A simple allocator based on `new` and `delete` primitives
// PMem workload is not supported
//
// The main purposes of the allocator are:
// 1. debuging
// 2. behavior verification
// 3. base line for other more sophisticated impls
class SimpleLogBasedMemoryAllocator : public LogBasedMemoryAllocator {
 public:
  virtual ~SimpleLogBasedMemoryAllocator() {
    for (char* p : regions_) {
      if (p != nullptr) delete[] p;
    }
  }

  noodle::Result<char*, CacheError> Allocate(size_t len) override {
    auto* ptr = new char[len];
    std::lock_guard<std::mutex> lk(mtx_);
    regions_.push_back(ptr);
    return ptr;
  }

  noodle::Result<void, CacheError> Free(char* ptr, size_t len) override {
    delete[] ptr;
    std::lock_guard<std::mutex> lk(mtx_);
    for (size_t i = 0; i < regions_.size(); ++i) {
      if (regions_[i] == ptr) {
        regions_[i] = nullptr;
      }
    }
    return {};
  }

  bool Contains(const char* ptr) const override {
    for (const char* p : regions_) {
      if (p == ptr) return true;
    }
    return false;
  }

  noodle::Result<void, CacheError> Seal(char* ptr, size_t len,
                                        uint32_t crc32) override {
    return {};
  }

  noodle::Result<void, CacheError> Seal(char* ptr) override { return {}; }

  noodle::Result<void, CacheError> IterateRecyclableChunkMeta(
      const std::function<bool(const ChunkMeta* meta)>& func) const override {
    return {};
  }

  noodle::Result<void, CacheError> RetrieveChunkMeta(
      ChunkID chunk_id,
      const std::function<void(const ChunkMeta* meta)>& func) override {
    return {};
  }

  noodle::Result<void, CacheError> GC(const ChunkID* chunk_id_arr,
                                      size_t arr_len) override {
    return {};
  }

  noodle::Result<AllocatorStats, CacheError> GetStats() const override {
    return AllocatorStats{};
  }

  noodle::Result<uint64_t, CacheError> Capacity() const override { return uint64_t{0}; }

 private:
  // `regions_` saves the pointers to the memory space allocated by this
  // allocator. The un-freed memory spaces will be freed automatically
  // when this allocator is destroyed to avoid memory leak.
  std::vector<char*> regions_;

  // The mutex to protect the concurrent update to `regions_`.
  std::mutex mtx_;
};

}  // namespace mtcache
