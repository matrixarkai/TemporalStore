#include "allocator/pool_allocator.h"

#include "allocator/alloc_utils.h"
#include "common/logging.h"

#include <gflags/gflags.h>

// Chunk size of the memory pool (in bytes) of the pool-based allocator
DECLARE_uint64(pool_chunk_sz);
// The maximum number of memory chunks preloaded by the object cache
DECLARE_int32(pool_obj_cache_max_chunk_num);

namespace mtcache {

PoolBasedMemoryAllocatorBase::PoolBasedMemoryAllocatorBase(
    AllocateMemoryObjectFunc alloc_mem_obj_func,
    FreeMemoryObjectFunc free_mem_obj_func, size_t capacity_bytes,
    size_t max_thread_num, size_t obj_len)
    : alloc_mem_obj_func_(std::move(alloc_mem_obj_func)),
      free_mem_obj_func_(std::move(free_mem_obj_func)),
      capacity_bytes_(capacity_bytes),
      obj_len_(obj_len),
      preload_num_(FLAGS_pool_chunk_sz / obj_len),
      chunks_(capacity_bytes / FLAGS_pool_chunk_sz + 1),
      ctxs_(max_thread_num) {
  CHECK(capacity_bytes > 0) << "Cache capacity must be larger than 0!";
  CHECK(obj_len > 0) << "Object length must be larger than 0!";
}

PoolBasedMemoryAllocatorBase::~PoolBasedMemoryAllocatorBase() {
  for (auto& chunk : chunks_) {
    PoolChunk* p = chunk.load(std::memory_order_acquire);
    if (p != nullptr) {
      if (p->data != nullptr) {
        free_mem_obj_func_(p->data, FLAGS_pool_chunk_sz);
      }
      delete p;
    }
  }
}

noodle::Result<char*, CacheError> PoolBasedMemoryAllocatorBase::Allocate(
    size_t len) {
  if (len == 0) {
    return &Errors::kAllocatorRequestIsZero;
  } else if (len > obj_len_ - kHeaderLen) {
    return &Errors::kAllocatorRequestTooLarge;
  }

  auto get_tls_ctx_res = GetThreadLocalContext();
  if (!get_tls_ctx_res.IsOk()) {
    return get_tls_ctx_res.GetError();
  }

  ThreadLocalContext& ctx = *get_tls_ctx_res.Get();
  char* ptr = nullptr;
  uint32_t mem_obj_len = len;

  // if there is no aviable memory object in the object cache, then
  // refill it from the global free list or a new chunk of PMEM device
  if (ctx.obj_cache.empty()) {
    auto refill_res = RefillObjCache();
    if (!refill_res.IsOk() || ctx.obj_cache.empty()) {
      return refill_res.GetError();
    }
  }

  ptr = ctx.obj_cache.back();
  ctx.obj_cache.pop_back();

  // update meta
  ctx.num_available_objects_obj_cache.fetch_sub(1, std::memory_order_relaxed);
  ctx.num_allocated_objects.fetch_add(1, std::memory_order_relaxed);

  // write object header
  memcpy(ptr, &mem_obj_len, sizeof(mem_obj_len));
  ptr += sizeof(mem_obj_len);

  return ptr;
}

noodle::Result<void, CacheError> PoolBasedMemoryAllocatorBase::Free(
    char* ptr, size_t len) {
  // Get thread information
  auto get_tls_ctx_res = GetThreadLocalContext();
  if (!get_tls_ctx_res.IsOk()) {
    return get_tls_ctx_res.GetError();
  }
  ThreadLocalContext& ctx = *get_tls_ctx_res.Get();

  // Read object header
  uint32_t mem_obj_len;
  ptr -= sizeof(mem_obj_len);
  memcpy(&mem_obj_len, ptr, sizeof(mem_obj_len));
  if (mem_obj_len & kTombstoneMask) {
    return &Errors::kAllocatorDoubleFree;
  }

  // Write tombstone
  mem_obj_len |= kTombstoneMask;
  memcpy(ptr, &mem_obj_len, sizeof(mem_obj_len));

  // Recycle freed memory objects
  ctx.obj_cache.emplace_front(ptr);

  // Update tls stats
  ctx.num_available_objects_obj_cache.fetch_add(1, std::memory_order_relaxed);
  ctx.num_allocated_objects.fetch_sub(1, std::memory_order_relaxed);

  // Check if there are too many free objects in the local object cache
  auto rebalance_res = RebalanceObjCache();
  if (!rebalance_res.IsOk()) {
    return rebalance_res.GetError();
  }
  return {};
}

bool PoolBasedMemoryAllocatorBase::Contains(const char* ptr) const {
  // TODO(kaiwu.kw) NUMA-awareness is only supported in log-based allocator now.
  // Will add it for pool-based allocator later.
  LOG(FATAL) << "Pool-based allocator does not support Contains yet!";
  return false;
}

noodle::Result<void, CacheError> PoolBasedMemoryAllocatorBase::Seal(
    char* ptr, size_t len, uint32_t crc32) {
  OnSeal(ptr, len, crc32);
  return {};
}

noodle::Result<void, CacheError> PoolBasedMemoryAllocatorBase::Seal(char* ptr) {
  return Seal(ptr, 0, 0);
}

noodle::Result<AllocatorStats, CacheError>
PoolBasedMemoryAllocatorBase::GetStats() const {
  AllocatorStats stats;
  size_t num_allocated_bytes = 0;
  size_t num_freed_bytes = 0;

  // aggregate all TLS stats
  size_t i = 0;
  size_t n = num_inited_tls_ctx_.load(std::memory_order_relaxed);
  for (auto& p : ctxs_) {
    if (p == nullptr) {
      continue;
    }
    num_allocated_bytes +=
        (p->num_allocated_objects.load(std::memory_order_relaxed) * obj_len_);
    num_freed_bytes +=
        (p->num_available_objects_obj_cache.load(std::memory_order_relaxed) *
         obj_len_);
    if (++i == n) {
      break;
    }
  }

  stats.num_allocated_bytes = num_allocated_bytes;
  stats.num_freed_bytes = num_freed_bytes;
  // `num_occupied_bytes` is recorded globally
  stats.num_occupied_bytes =
      num_occupied_bytes_.load(std::memory_order_relaxed);
  return stats;
}

noodle::Result<size_t, CacheError> PoolBasedMemoryAllocatorBase::Capacity()
    const {
  return capacity_bytes_;
}

noodle::Result<PoolBasedMemoryAllocatorBase::ThreadLocalContext*, CacheError>
PoolBasedMemoryAllocatorBase::GetThreadLocalContext() {
  int id = GetThreadLocalResourceID();
  if (id >= ctxs_.size()) {
    return &Errors::kThreadLocalResourceIDTooLarge;
  }

  auto& p = ctxs_[id];
  if (p == nullptr) {
    p = std::make_unique<ThreadLocalContext>();
    num_inited_tls_ctx_.fetch_add(1, std::memory_order_relaxed);
  }
  return p.get();
}

noodle::Result<PoolBasedMemoryAllocatorBase::PoolChunk*, CacheError>
PoolBasedMemoryAllocatorBase::AllocateChunk() {
  size_t limit = capacity_bytes_;

  while (true) {
    size_t before_bytes = num_occupied_bytes_.load(std::memory_order_relaxed);
    size_t after_bytes = before_bytes + FLAGS_pool_chunk_sz;
    if (after_bytes > limit) {
      return &Errors::kAllocatorOutOfSpace;
    }
    if (num_occupied_bytes_.compare_exchange_weak(before_bytes, after_bytes,
                                                  std::memory_order_relaxed,
                                                  std::memory_order_relaxed)) {
      break;
    }
  }

  PoolChunk* chunk = nullptr;
  // Create a new chunk
  ChunkID chunk_id = next_chunk_id_.fetch_add(1, std::memory_order_relaxed);
  if (chunk_id >= chunks_.size()) {
    num_occupied_bytes_.fetch_sub(FLAGS_pool_chunk_sz,
                                  std::memory_order_relaxed);
    return &Errors::kAllocatorChunkIDTooLarge;
  }

  auto alloc_res =
      alloc_mem_obj_func_(chunk_id, FLAGS_pool_chunk_sz, FLAGS_pool_chunk_sz);
  while (true) {
    if (alloc_res.GetError() != &Errors::kAllocationRetry) {
      break;
    }
    alloc_res =
        alloc_mem_obj_func_(chunk_id, FLAGS_pool_chunk_sz, FLAGS_pool_chunk_sz);
  }
  if (!alloc_res.IsOk()) {
    num_occupied_bytes_.fetch_sub(FLAGS_pool_chunk_sz,
                                  std::memory_order_relaxed);
    return alloc_res.GetError();
  }
  chunk = new PoolChunk(chunk_id, reinterpret_cast<char*>(alloc_res.Get()));

  // Set pointer at last
  auto& p = chunks_[chunk_id];
  DCHECK(p == nullptr);
  p.store(chunk, std::memory_order_release);

  // Update chunk meta
  DCHECK(!chunk->write_lock);
  chunk->write_lock.store(true, std::memory_order_relaxed);

  return chunk;
}

noodle::Result<void, CacheError>
PoolBasedMemoryAllocatorBase::RefillObjCache() {
  auto get_tls_ctx_res = GetThreadLocalContext();
  if (!get_tls_ctx_res.IsOk()) {
    return get_tls_ctx_res.GetError();
  }

  ThreadLocalContext& ctx = *get_tls_ctx_res.Get();
  // Try global free list first
  {
    std::lock_guard<std::mutex> guard(free_list_mtx_);
    auto num_objs_in_freelist = global_free_list_.size();
    if (num_objs_in_freelist >
        0) {  // If there are some objects in the free list
      if (num_objs_in_freelist >= preload_num_) {
        // auto it = ctx.obj_cache.begin();
        auto it = global_free_list_.begin();
        advance(it, preload_num_);
        // move objects to the the local object cache
        ctx.obj_cache.splice(ctx.obj_cache.end(), global_free_list_, it,
                             global_free_list_.end());
        ctx.num_available_objects_obj_cache.fetch_add(
            preload_num_, std::memory_order_relaxed);
      } else {
        // move objects to the the local object cache
        ctx.obj_cache.splice(ctx.obj_cache.end(), global_free_list_);
        ctx.num_available_objects_obj_cache.fetch_add(
            num_objs_in_freelist, std::memory_order_relaxed);
      }
      return {};
    }
  }
  // Create a new chunk to refill
  {
    auto alloc_res = AllocateChunk();
    if (!alloc_res.IsOk()) {
      return alloc_res.GetError();
    }
    PoolChunk* chunk = alloc_res.Get();
    auto num_alloc_objects = 0;
    char* ptr = nullptr;
    while (num_alloc_objects * obj_len_ < FLAGS_pool_chunk_sz) {
      ptr = chunk->data + num_alloc_objects * obj_len_;
      ctx.obj_cache.emplace_front(ptr);

      // Update tls stats
      ctx.num_available_objects_obj_cache.fetch_add(1,
                                                    std::memory_order_relaxed);
      num_alloc_objects += 1;
    }
    return {};
  }
}

noodle::Result<void, CacheError>
PoolBasedMemoryAllocatorBase::RebalanceObjCache() {
  auto get_tls_ctx_res = GetThreadLocalContext();
  if (!get_tls_ctx_res.IsOk()) {
    return get_tls_ctx_res.GetError();
  }

  ThreadLocalContext& ctx = *get_tls_ctx_res.Get();
  auto num_objs = ctx.obj_cache.size();
  // If there are too many (i.e., more than twice the number of preloads by
  // default) free objects in the local object cache The return some to the
  // global free list
  if (num_objs >= preload_num_ * FLAGS_pool_obj_cache_max_chunk_num) {
    std::lock_guard<std::mutex> guard(free_list_mtx_);
    auto it = ctx.obj_cache.begin();
    advance(it, preload_num_);
    // move objects to the global free list
    global_free_list_.splice(global_free_list_.end(), ctx.obj_cache, it,
                             ctx.obj_cache.end());
    ctx.num_available_objects_obj_cache.fetch_sub(preload_num_,
                                                  std::memory_order_relaxed);
  }
  return {};
}

}  // namespace mtcache
