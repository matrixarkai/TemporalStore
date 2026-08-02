#pragma once

#include "allocator.h"

#include <atomic>
#include <functional>
#include <list>
#include <mutex>
#include <vector>

namespace mtcache {

class PoolBasedMemoryAllocatorBase : public PoolBasedMemoryAllocator {
 public:
  // These two functions are same as LogBasedMemoryAllocatorBase above
  using AllocateMemoryObjectFunc =
      std::function<noodle::Result<void* /* addr */, CacheError>(
          ChunkID, size_t /* object_len */, size_t /* alignment */)>;
  using FreeMemoryObjectFunc = std::function<noodle::Result<void, CacheError>(
      void* /* addr */, size_t /* len */)>;

  PoolBasedMemoryAllocatorBase(AllocateMemoryObjectFunc alloc_mem_obj_func,
                               FreeMemoryObjectFunc free_mem_obj_func,
                               size_t capacity_bytes, size_t max_thread_num,
                               size_t obj_len);

  ~PoolBasedMemoryAllocatorBase() override;

  noodle::Result<char*, CacheError> Allocate(size_t len) override;

  noodle::Result<void, CacheError> Free(char* ptr, size_t len) override;

  bool Contains(const char* ptr) const override;

  // See comments at `LogBasedMemoryAllcatorBase::Seal` for detail.
  noodle::Result<void, CacheError> Seal(char* ptr, size_t len,
                                        uint32_t crc32) override;

  // See comments at `LogBasedMemoryAllcatorBase::Seal` for detail.
  noodle::Result<void, CacheError> Seal(char* ptr) override;

  noodle::Result<AllocatorStats, CacheError> GetStats() const override;

  noodle::Result<size_t, CacheError> Capacity() const override;

  size_t TEST_GetGobalFreeListSize() const { return global_free_list_.size(); }

 protected:
  struct PoolChunk;

  // See comments at LogBasedMemoryAllocatorBase::OnSeal for detail.
  virtual void OnSeal(char* payload_ptr, size_t payload_len, uint32_t crc32) {}

  // binary format: mem_obj_len:uint32_t + data:char[mem_obj_len]
  static constexpr uint32_t kHeaderLen = sizeof(uint32_t);
  // When a memory object is deleted, the hightest bit of its uint32_t capacity
  // field will be filpped to 1. kTombstoneMask is bit operation mask for
  // flipping.
  static constexpr uint32_t kTombstoneMask = 1U << 31U;

  // Each chunk contains multiple fixed size memory objects. We use 'chunk' to
  // improve concurrency
  struct PoolChunk {
    ChunkID id;
    // Memory directly allocated from OS
    char* data;
    // No simultaneous write
    std::atomic<bool> write_lock{false};

    PoolChunk(ChunkID id, char* data) : id(id), data(data) {}
  };

  struct ThreadLocalContext {
    // TODO(kaiwu.kw) Later possible optimization: A Queue generally has a
    // better CPU cache hit rate than a list. But a list can move multiple
    // objects from the object cache to the global free list at once. We need to
    // profile which design choice has better performance here. Each thread has
    // a private a recycle list to collect freed memory objects
    std::list<char*> obj_cache;

    // TLS-level stats
    std::atomic<size_t> num_allocated_objects = 0;
    std::atomic<size_t> num_available_objects_obj_cache = 0;
  };

  noodle::Result<ThreadLocalContext*, CacheError> GetThreadLocalContext();

  noodle::Result<PoolChunk*, CacheError> AllocateChunk();

  noodle::Result<void, CacheError> RefillObjCache();

  noodle::Result<void, CacheError> RebalanceObjCache();

  AllocateMemoryObjectFunc alloc_mem_obj_func_;
  FreeMemoryObjectFunc free_mem_obj_func_;

  // Max space the allocator can use
  const size_t capacity_bytes_;

  // Single object memory size
  const size_t obj_len_;

  // Number of objects preloaded from the heap to each thread
  const size_t preload_num_;

  // Container of all chunks
  // Its size is fixed after init, therefore chunks_[n] is lock-free.
  std::vector<std::atomic<PoolChunk*>> chunks_;

  // Container of all free objects returned from each thread
  std::list<char*> global_free_list_;

  // Container of all thread local contexts
  // Its size is fixed after init, therefore ctxs_[n] is lock-free.
  std::vector<std::unique_ptr<ThreadLocalContext>> ctxs_;

  // How many thread local contexts have been initialized
  std::atomic<size_t> num_inited_tls_ctx_{0};

  // How many bytes the allocator currently occupies
  // It's always <= capacity_bytes_
  std::atomic<size_t> num_occupied_bytes_{0};

  std::mutex free_list_mtx_;

  // ChunkID for next chunk allocation
  std::atomic<ChunkID> next_chunk_id_{0};
};

}  // namespace mtcache
