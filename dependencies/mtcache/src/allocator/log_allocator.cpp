#include "allocator/log_allocator.h"

#include "allocator/alloc_utils.h"
#include "common/logging.h"
#include "util/align_util.h"

#include <folly/hash/Checksum.h>

#include <algorithm>

namespace mtcache {

static const char* ALLOCATOR_ERROR_ID_TAG = "allocator_error_id";

static std::map<std::string, std::string> ALLOCATOR_ERROR_COUNTER_TAGS_MAP{
    {"kAllocatorOutOfSpace", "out_of_space"},
    {"kAllocatorChunkIDTooLarge", "chunkid_too_large"},
    {"kAllocatorInvalidAddress", "invalid_address"},
    {"kAllocatorInvalidChunkID", "invalid_chunkid"},
    {"kAllocatorDoubleFree", "double_free"},
    {"kAllocatorRequestTooLarge", "request_too_large"},
    {"kAllocatorRequestIsZero", "request_is_zero"},
    {"kAllocationRetry", "allocation_retry"},
    {"kThreadLocalResourceIDTooLarge", "thread_local_resourceid_too_large"},
    {"kObjFuncAllocationFailed", "obj_func_allocation_failed"}};

LogBasedMemoryAllocatorBase::LogBasedMemoryAllocatorBase(
    AllocateMemoryObjectFunc alloc_mem_obj_func,
    FreeMemoryObjectFunc free_mem_obj_func,
    LogBasedAllocatorGCEventListener* gc_event_listener, size_t capacity_bytes,
    size_t gc_reserved_bytes, size_t max_thread_num,
    std::shared_ptr<noodle::MetricRegistry> registry, int numa_id_prefix)
    : alloc_mem_obj_func_(std::move(alloc_mem_obj_func)),
      free_mem_obj_func_(std::move(free_mem_obj_func)),
      gc_event_listener_(gc_event_listener),
      capacity_bytes_(ROUND_UP(capacity_bytes, kLogChunkSize)),
      capacity_and_gc_bytes_(
          ROUND_UP(capacity_bytes + gc_reserved_bytes, kLogChunkSize)),
      chunks_(capacity_and_gc_bytes_ / kLogChunkSize + 1),
      ctxs_(max_thread_num),
      alloc_metric_registry_(registry) {
  CHECK(gc_reserved_bytes > 0) << "GC reserved capacity must be larger than 0!";
  CHECK(capacity_bytes > 0) << "Cache capacity must be larger than 0!";
  CHECK(registry != nullptr);
  auto pre_res = ReserveVM();
  CHECK(pre_res.IsOk()) << "Fail to reserve VM";
  RegisterMetrics(numa_id_prefix);
}

LogBasedMemoryAllocatorBase::~LogBasedMemoryAllocatorBase() {
  for (auto& chunk : chunks_) {
    GeneralChunk* p = chunk.load(std::memory_order_acquire);
    if (p != nullptr) {
      delete p;
    }
  }
  auto res = ReleaseVM();
  if (!res.IsOk()) {
    LOG(ERROR) << "Fail to release the reserved VM.";
  }
}

noodle::Result<char*, CacheError> LogBasedMemoryAllocatorBase::Allocate(
    size_t len) {
  if (len == 0) {
    HandleErrorMetrics("kAllocatorRequestIsZero");
    return &Errors::kAllocatorRequestIsZero;
  } else if (len > kLogChunkSize - kHeaderLen) {
    // TODO(lyj): do we need to support large allocation?
    HandleErrorMetrics("kAllocatorRequestTooLarge");
    return &Errors::kAllocatorRequestTooLarge;
  }

  auto get_tls_ctx_res = GetThreadLocalContext();
  if (!get_tls_ctx_res.IsOk()) {
    return get_tls_ctx_res.GetError();
  }

  ThreadLocalContext& ctx = *get_tls_ctx_res.Get();
  GeneralChunk* chunk = ctx.chunk;
  if (chunk == nullptr) {  // didn't allocate before
    auto alloc_res = AllocateChunk();
    if (!alloc_res.IsOk()) {
      return alloc_res.GetError();
    }

    ctx.chunk = alloc_res.Get();
    chunk = ctx.chunk;
  }

  size_t alloc_bytes =
      chunk->meta.num_allocated_bytes.load(std::memory_order_relaxed);
  size_t physical_len = kHeaderLen + len;
  if (alloc_bytes + physical_len > kLogChunkSize) {  // switch to new chunk
    auto alloc_res = AllocateChunk();
    if (!alloc_res.IsOk()) {
      return alloc_res.GetError();
    }

    SealChunk(chunk);
    // replace with new chunk
    ctx.chunk = alloc_res.Get();
    chunk = ctx.chunk;
    alloc_bytes = 0;
  }
  // update tls stats
  ctx.num_allocated_bytes.store(
      ctx.num_allocated_bytes.load(std::memory_order_relaxed) + len,
      std::memory_order_relaxed);

  // write header
  char* ptr = chunk->data + alloc_bytes;
  uint32_t mem_obj_len = len;
  memcpy(ptr, &mem_obj_len, sizeof(mem_obj_len));
  ptr += sizeof(mem_obj_len);

  // update meta
  chunk->unsealed_cnt.fetch_add(1, std::memory_order_relaxed);
  // no need to use atomic fetch_add, since there is only one writer
  chunk->meta.num_allocated_bytes.store(alloc_bytes + physical_len,
                                        std::memory_order_relaxed);
  RefChunk(chunk);
  return ptr;
}

noodle::Result<void, CacheError> LogBasedMemoryAllocatorBase::Free(char* ptr,
                                                                   size_t len) {
  GeneralChunk* chunk = RawPointerToChunk(ptr);
  if (chunk == nullptr) {
    HandleErrorMetrics("kAllocatorInvalidAddress");
    return &Errors::kAllocatorInvalidAddress;
  }

  auto get_tls_ctx_res = GetThreadLocalContext();
  if (!get_tls_ctx_res.IsOk()) {
    return get_tls_ctx_res.GetError();
  }
  ThreadLocalContext& ctx = *get_tls_ctx_res.Get();

  // read header
  uint32_t mem_obj_len;
  ptr -= sizeof(mem_obj_len);
  memcpy(&mem_obj_len, ptr, sizeof(mem_obj_len));
  if (mem_obj_len & kTombstoneMask) {
    HandleErrorMetrics("kAllocatorDoubleFree");
    return &Errors::kAllocatorDoubleFree;
  }
  size_t physical_len = sizeof(mem_obj_len) + mem_obj_len;

  // update tls stats
  ctx.num_freed_bytes.store(
      ctx.num_freed_bytes.load(std::memory_order_relaxed) + mem_obj_len,
      std::memory_order_relaxed);

  // write tombstone
  mem_obj_len |= kTombstoneMask;
  memcpy(ptr, &mem_obj_len, sizeof(mem_obj_len));

  // update meta
  chunk->meta.num_freed_bytes.fetch_add(physical_len,
                                        std::memory_order_relaxed);
  UnrefChunk(chunk);
  return {};
}

bool LogBasedMemoryAllocatorBase::Contains(const char* ptr) const {
  return (ptr >= chunk_base_) && (ptr < chunk_base_ + capacity_and_gc_bytes_);
}

noodle::Result<void, CacheError> LogBasedMemoryAllocatorBase::Seal(
    char* ptr, size_t len, uint32_t crc32) {
  GeneralChunk* chunk = RawPointerToChunk(ptr);
  if (chunk == nullptr) {
    HandleErrorMetrics("kAllocatorInvalidAddress");
    return &Errors::kAllocatorInvalidAddress;
  }

  OnSeal(chunk, ptr, len, crc32);
  chunk->unsealed_cnt.fetch_sub(1, std::memory_order_release);
  return {};
}

noodle::Result<void, CacheError> LogBasedMemoryAllocatorBase::Seal(char* ptr) {
  return Seal(ptr, 0, 0);
}

noodle::Result<void, CacheError>
LogBasedMemoryAllocatorBase::IterateRecyclableChunkMeta(
    const std::function<bool(const ChunkMeta* meta)>& func) const {
  struct SortableMeta {
    ChunkMeta* meta;
    size_t free_bytes;  // the higher the better
    size_t ref_cnt;     // the lower  the better

    SortableMeta(ChunkMeta* p_meta, size_t p_free_bytes, size_t p_ref_cnt)
        : meta(p_meta), free_bytes(p_free_bytes), ref_cnt(p_ref_cnt) {}

    bool operator<(const SortableMeta& another) const {
      return free_bytes < another.free_bytes ||
             (free_bytes == another.free_bytes && ref_cnt > another.ref_cnt);
    }
  };

  std::vector<SortableMeta> meta_vec;
  ChunkID end_id = next_chunk_id_.load(std::memory_order_relaxed);
  meta_vec.reserve(end_id);
  for (ChunkID begin_id = 0; begin_id < end_id; ++begin_id) {
    GeneralChunk* p = chunks_[begin_id].load(std::memory_order_acquire);
    if (p == nullptr) {  // `p` has a very small chance to be nullptr, cuz we
                         // fetch_add `next_chunk_id_` firstly then
                         // new the GeneralChunk `p` in `AllocateChunk`
      continue;
    }
    // skip the chunk that is owned by a writer
    // pick the chunk whose objects are all sealed
    // pick the chunk that is full
    if (!p->write_lock.load(std::memory_order_relaxed) &&
        p->unsealed_cnt.load(std::memory_order_relaxed) == 0 &&
        p->meta.num_allocated_bytes.load(std::memory_order_relaxed) ==
            kLogChunkSize) {
      // The reason behind only picking full chunk is that a non-full chunk can
      // only be one of these two status list below:
      // 1. A writer owns the chunk
      // 2. The chunk has been GCed and is waiting `ref_cnt` becomes 0
      // None of these are needed.
      // Status 1 rationale: A writer will never switch to a new chunk if the
      // old one is not full
      // Status 2 rationale: `num_allocated_bytes` is set to 0 in `GC`
      meta_vec.emplace_back(
          &p->meta, p->meta.num_freed_bytes.load(std::memory_order_relaxed),
          p->meta.ref_cnt.load(std::memory_order_relaxed));
    }
  }

  std::make_heap(meta_vec.begin(), meta_vec.end());  // max heap
  while (!meta_vec.empty()) {
    bool need_more = func(meta_vec.front().meta);
    if (need_more) {
      std::pop_heap(meta_vec.begin(), meta_vec.end());
      meta_vec.pop_back();
    } else {
      break;
    }
  }
  return {};
}

noodle::Result<void, CacheError> LogBasedMemoryAllocatorBase::RetrieveChunkMeta(
    ChunkID chunk_id, const std::function<void(const ChunkMeta* meta)>& func) {
  if (chunk_id >= chunks_.size()) {
    HandleErrorMetrics("kAllocatorChunkIDTooLarge");
    return &Errors::kAllocatorChunkIDTooLarge;
  }

  GeneralChunk* p = chunks_[chunk_id].load(std::memory_order_acquire);
  if (p == nullptr) {
    HandleErrorMetrics("kAllocatorInvalidChunkID");
    return &Errors::kAllocatorInvalidChunkID;
  }

  func(&p->meta);
  return {};
}

class LogBasedMemoryAllocatorBase::GCContext {
 public:
  explicit GCContext(LogBasedMemoryAllocatorBase* allocator)
      : alloc_(allocator) {
    auto get_tls_ctx_res = alloc_->GetThreadLocalContext();
    if (!get_tls_ctx_res.IsOk()) {
      res_ = get_tls_ctx_res.GetError();
    } else {
      alloc_tls_ctx_ = get_tls_ctx_res.Get();
    }
  }

  bool IsOk() const { return res_.IsOk(); }

  const noodle::Result<void, CacheError>& GetResult() const { return res_; }

  // consume all objects in `chunk` and rewrite to `new_chunk`
  void ConsumeChunk(GeneralChunk* chunk) {
    const char* begin = chunk->data;
    const char* end = begin + kLogChunkSize;
    while (begin + kHeaderLen < end) {  // iterate chunk
      uint32_t mem_obj_len;
      memcpy(&mem_obj_len, begin, sizeof(mem_obj_len));
      if (mem_obj_len == kChunkStopMark) {
        break;
      }

      if (mem_obj_len & kTombstoneMask) {  // skip deleted object
        mem_obj_len &= (kTombstoneMask - 1);
      } else {
        const char* old_ptr = begin + sizeof(mem_obj_len);
        const char* new_ptr = AppendObjectToNewChunk(mem_obj_len, old_ptr);
        if (!IsOk()) {
          // partially consumed chunk resistence
          // Every consumed object must has been passed to `gc_event_listener_`
          // via `OnGCCopy` method.
          // There can only be two outcomes:
          // 1. Success
          // The holder of the object has replaced old_ptr with new_ptr. Even if
          // GC may consume this object mutiple times in some really bad
          // cases, only first call to `OnGCCopy` can succeed.
          // 2. Failure
          // No side effect, partially consumed chunk resist naturally
          break;
        }

        auto replace_ptr_res =
            alloc_->gc_event_listener_->OnGCCopy(old_ptr, new_ptr);
        if (!replace_ptr_res.IsOk()) {
          DiscardLastObjectInNewChunk(mem_obj_len);
          if (chunk->meta.ref_cnt.load(std::memory_order_acquire) == 1) {
            // Only gc ctx refs to this chunk, which means this chunk is freed
            // during GC. So there is no need to continue scanning this chunk
            // any more. The upper caller will UnRef this chunk and free this
            // chunk (because this GC ctx holds the last reference), where it
            // will set all the stats of this chunk to "freed" state.
            //
            // This state may happen frequently when FIFO is used in
            // ReplacementPolicy because the records are evicted/freed with
            // the same order of allocation. In this case, when a chunk needs
            // to be GC-ed, the remaining valid records in this chunk also have
            // a high chance of be freed in the short time, which may be faster
            // than the GC process.
            //
            // TODO(dbc) Maybe we can just disable GC when FIFO is used?
            break;
          }
        } else {
          // TODO(lyj): we may need to flush here as well
          alloc_tls_ctx_->num_gc_move_bytes.store(
              alloc_tls_ctx_->num_gc_move_bytes.load(
                  std::memory_order_relaxed) +
                  mem_obj_len,
              std::memory_order_relaxed);
        }
      }

      size_t physical_len = sizeof(mem_obj_len) + mem_obj_len;
      begin += physical_len;
    }
    if (IsOk()) {
      // Therefore we won't pick this chunk as GC candidate again in
      // `IterateRecyclableChunkMeta`
      chunk->meta.num_allocated_bytes.store(0, std::memory_order_relaxed);
    }
  }

  void Finish() {
    if (new_chunk_ != nullptr) {
      size_t alloc_bytes =
          new_chunk_->meta.num_allocated_bytes.load(std::memory_order_relaxed);
      if (alloc_bytes + kHeaderLen >= kLogChunkSize) {
        // no more data can be written to the chunk
        alloc_->SealChunk(new_chunk_);
      } else {
        MoveNewChunkToGCLeftChunks();
      }
    }
  }

 private:
  const char* AppendObjectToNewChunk(uint32_t mem_obj_len,
                                     const char* old_ptr) {
    if (new_chunk_ == nullptr) {
      if (!AllocateNewChunkFromGCLeftChunks()) {
        SwitchChunk();
      }
      if (!IsOk()) {
        return nullptr;
      }
    }

    size_t alloc_bytes =
        new_chunk_->meta.num_allocated_bytes.load(std::memory_order_relaxed);
    size_t physical_len = kHeaderLen + mem_obj_len;
    if (alloc_bytes + physical_len > kLogChunkSize) {
      SwitchChunk();
      if (!IsOk()) {
        return nullptr;
      }
      alloc_bytes = 0;
    }

    // write header
    char* new_ptr = new_chunk_->data + alloc_bytes;
    memcpy(new_ptr, &mem_obj_len, sizeof(mem_obj_len));
    new_ptr += sizeof(mem_obj_len);

    // copy user data
    memcpy(new_ptr, old_ptr, mem_obj_len);

    // update meta
    // no need to use atomic fetch_add, since there is only one writer
    new_chunk_->meta.num_allocated_bytes.store(alloc_bytes + physical_len,
                                               std::memory_order_relaxed);
    alloc_->RefChunk(new_chunk_);
    return new_ptr;
  }

  void SwitchChunk() {
    auto alloc_res = alloc_->AllocateChunk(AllocationPurpose::kGC);
    if (!alloc_res.IsOk()) {
      res_ = alloc_res.GetError();
      return;
    }

    if (new_chunk_ != nullptr) {
      alloc_->SealChunk(new_chunk_);
    }
    new_chunk_ = alloc_res.Get();
  }

  bool AllocateNewChunkFromGCLeftChunks() {
    std::lock_guard<std::mutex> guard(alloc_->gc_left_chunks_mtx_);
    auto& left_chunks = alloc_->gc_left_chunks_;
    if (!left_chunks.empty()) {
      new_chunk_ = left_chunks.back();
      left_chunks.pop_back();
      return true;
    }
    return false;
  }

  void MoveNewChunkToGCLeftChunks() {
    std::lock_guard<std::mutex> guard(alloc_->gc_left_chunks_mtx_);
    auto& left_chunks = alloc_->gc_left_chunks_;
    left_chunks.emplace_back(new_chunk_);
  }

  void DiscardLastObjectInNewChunk(uint32_t mem_obj_len) {
    size_t alloc_bytes =
        new_chunk_->meta.num_allocated_bytes.load(std::memory_order_relaxed);
    size_t physical_len = sizeof(mem_obj_len) + mem_obj_len;
    // no need to use atomic fetch_add, since there is only one writer
    new_chunk_->meta.num_allocated_bytes.store(alloc_bytes - physical_len,
                                               std::memory_order_relaxed);
    alloc_->UnrefChunk(new_chunk_);
  }

  LogBasedMemoryAllocatorBase* alloc_;
  LogBasedMemoryAllocatorBase::ThreadLocalContext* alloc_tls_ctx_ = nullptr;
  noodle::Result<void, CacheError> res_;
  GeneralChunk* new_chunk_ = nullptr;
};

noodle::Result<bool, CacheError> LogBasedMemoryAllocatorBase::ReserveVM() {
  // Pre Allocate capacity vm.
  auto pre_alloc_res = PreAllocate(capacity_and_gc_bytes_, kLogChunkSize);
  if (!pre_alloc_res.IsOk()) {
    return pre_alloc_res.GetError();
  }
  chunk_base_ = reinterpret_cast<char*>(pre_alloc_res.Get());
  return true;
}

noodle::Result<void, CacheError> LogBasedMemoryAllocatorBase::ReleaseVM() {
  return PostFree(reinterpret_cast<void*>(chunk_base_), capacity_and_gc_bytes_);
}

noodle::Result<void, CacheError> LogBasedMemoryAllocatorBase::GC(
    const ChunkID* chunk_id_arr, size_t arr_len) {
  if (arr_len == 0) {
    return {};
  }
  GCContext ctx(this);

  for (size_t i = 0; i < arr_len && ctx.IsOk(); ++i) {
    ChunkID chunk_id = chunk_id_arr[i];
    GeneralChunk* p = chunks_[chunk_id].load(std::memory_order_acquire);
    DCHECK(p != nullptr);

    // We cannot simply use `RefChunk`, cuz we cannot pick a chunk that
    // just has been moved to free list. That's quite tricky!
    while (true) {
      size_t ref_cnt = p->meta.ref_cnt.load(std::memory_order_relaxed);
      if (ref_cnt == 0) {  // already in free list
        break;
      }

      if (p->meta.ref_cnt.compare_exchange_weak(ref_cnt, ref_cnt + 1,
                                                std::memory_order_acquire,
                                                std::memory_order_relaxed)) {
        if (!p->write_lock.exchange(true, std::memory_order_acquire)) {  // lock
          if (p->unsealed_cnt.load(std::memory_order_acquire) == 0 &&
              p->meta.num_allocated_bytes.load(std::memory_order_relaxed) ==
                  kLogChunkSize) {  // double check
            ctx.ConsumeChunk(p);
          }

          p->write_lock.store(false, std::memory_order_release);  // unlock
        }
        UnrefChunk(p);
        break;
      }
    }
  }

  ctx.Finish();
  return ctx.GetResult();
}

noodle::Result<AllocatorStats, CacheError>
LogBasedMemoryAllocatorBase::GetStats() const {
  AllocatorStats stats;
  size_t num_allocated_bytes = 0;
  size_t num_freed_bytes = 0;
  size_t num_gc_move_bytes = 0;

  // aggregate all TLS stats
  size_t i = 0;
  size_t n = num_inited_tls_ctx_.load(std::memory_order_relaxed);
  if (n > 0) {
    for (auto& p : ctxs_) {
      if (p == nullptr) {
        continue;
      }
      num_allocated_bytes +=
          p->num_allocated_bytes.load(std::memory_order_relaxed);
      num_freed_bytes += p->num_freed_bytes.load(std::memory_order_relaxed);
      num_gc_move_bytes += p->num_gc_move_bytes.load(std::memory_order_relaxed);
      if (++i == n) {
        break;
      }
    }
  }
  // The following DCHECK isn't always true. Due to reference counting, the
  // object that was logically freed by GC thread may still be held by
  // someone. In short run, num_freed_bytes could be < num_gc_move_bytes.
  // DCHECK(num_freed_bytes >= num_gc_move_bytes);
  stats.num_allocated_bytes = num_allocated_bytes;
  // After GC, new objects are created. Users need to `Free` both old
  // objects and new ones in the end, which introduces stats error. Reducing
  // `num_freed_bytes` by `num_gc_move_bytes` to correct the error.
  stats.num_freed_bytes = num_freed_bytes - num_gc_move_bytes;
  // `num_occupied_bytes` is recorded globally
  stats.num_occupied_bytes =
      num_occupied_bytes_.load(std::memory_order_relaxed);
  return stats;
}

noodle::Result<size_t, CacheError> LogBasedMemoryAllocatorBase::Capacity()
    const {
  return capacity_bytes_;
}

noodle::Result<LogBasedMemoryAllocatorBase::ThreadLocalContext*, CacheError>
LogBasedMemoryAllocatorBase::GetThreadLocalContext() {
  int id = GetThreadLocalResourceID();
  if (id >= ctxs_.size()) {
    HandleErrorMetrics("kThreadLocalResourceIDTooLarge");
    return &Errors::kThreadLocalResourceIDTooLarge;
  }

  auto& p = ctxs_[id];
  if (p == nullptr) {
    p = std::make_unique<ThreadLocalContext>();
    num_inited_tls_ctx_.fetch_add(1, std::memory_order_relaxed);
  }
  return p.get();
}

noodle::Result<LogBasedMemoryAllocatorBase::GeneralChunk*, CacheError>
LogBasedMemoryAllocatorBase::AllocateChunk(AllocationPurpose purpose) {
  size_t limit = 0;
  switch (purpose) {
    case AllocationPurpose::kNormal:
      limit = capacity_bytes_;
      break;
    case AllocationPurpose::kGC:
      // Only GC tasks are able to use GC-reserved chunks
      limit = capacity_and_gc_bytes_;
      break;
  }

  while (true) {
    size_t before_bytes = num_occupied_bytes_.load(std::memory_order_relaxed);
    size_t after_bytes = before_bytes + kLogChunkSize;
    if (after_bytes > limit) {
      HandleErrorMetrics("kAllocatorOutOfSpace");
      return &Errors::kAllocatorOutOfSpace;
    }
    if (num_occupied_bytes_.compare_exchange_weak(before_bytes, after_bytes,
                                                  std::memory_order_relaxed,
                                                  std::memory_order_relaxed)) {
      break;
    }
  }

  VLOG(4) << "AllocateChunk "
          << "num_occupied_bytes_:"
          << num_occupied_bytes_.load(std::memory_order_relaxed);

  GeneralChunk* chunk = nullptr;
  {  // try to find chunk from free list
    std::lock_guard<std::mutex> guard(alloc_mtx_);
    DCHECK(free_chunks_.size() <= next_chunk_id_);
    if (!free_chunks_.empty()) {
      chunk = free_chunks_.back();
      DCHECK(chunk->data != nullptr);
      free_chunks_.pop_back();
    }
  }

  if (chunk == nullptr) {  // create new chunk
    // Fetch from recycled chunk_id first.
    ChunkID chunk_id;
    {
      std::lock_guard<std::mutex> lck(recycle_mtx_);
      if (recycled_chunk_id_.empty()) {
        chunk_id = next_chunk_id_.fetch_add(1, std::memory_order_relaxed);
      } else {
        chunk_id = recycled_chunk_id_.front();
        recycled_chunk_id_.pop_front();
      }
    }
    if (chunk_id >= chunks_.size()) {
      num_occupied_bytes_.fetch_sub(kLogChunkSize, std::memory_order_relaxed);
      VLOG(4) << "AllocateChunk(chunk_id too large) "
              << "num_occupied_bytes_:"
              << num_occupied_bytes_.load(std::memory_order_relaxed);
      HandleErrorMetrics("kAllocatorChunkIDTooLarge");
      return &Errors::kAllocatorChunkIDTooLarge;
    }
    void* address = reinterpret_cast<void*>(
        chunk_id * kLogChunkSize + reinterpret_cast<uintptr_t>(chunk_base_));
    auto alloc_res =
        alloc_mem_obj_func_(address, chunk_id, kLogChunkSize, kLogChunkSize);
    if (!alloc_res.IsOk()) {
      HandleErrorMetrics("kObjFuncAllocationFailed");
      num_occupied_bytes_.fetch_sub(kLogChunkSize, std::memory_order_relaxed);
      VLOG(4) << "AllocateChunk(cannot create new "
                 "file in PMEM.) "
              << "num_occupied_bytes_:"
              << num_occupied_bytes_.load(std::memory_order_relaxed);
      // When allocated failed, return chunk_id to recycled chunk_id list.
      std::lock_guard<std::mutex> lck(recycle_mtx_);
      recycled_chunk_id_.push_front(chunk_id);
      return alloc_res.GetError();
    }
    DCHECK(address == alloc_res.Get());
    // TODO(lbw): Maybe we should allocate all GeneralChunks at once,
    // and put them into the chunks_ structure.
    // Note that chunk meta in GeneralChunk will be accessed frequently,
    // and it's important to make it cacheline-aligned
    chunk = new GeneralChunk(chunk_id, reinterpret_cast<char*>(address));
    // Set pointer at last
    auto& p = chunks_[chunk_id];
    DCHECK(p == nullptr);
    p.store(chunk, std::memory_order_release);
  }

  // update meta
  DCHECK(chunk->meta.ref_cnt == 0 && !chunk->write_lock);
  chunk->write_lock.store(true, std::memory_order_relaxed);
  // A writable chunk always has a holder.
  // Instead call `RefChunk` after creation every time,
  // we set `ref_cnt` to 1 here.
  chunk->meta.ref_cnt.store(1, std::memory_order_release);
  return chunk;
}

void* LogBasedMemoryAllocatorBase::ChunkId2Addr(ChunkID id) {
  return reinterpret_cast<void*>(id * kLogChunkSize +
                                 reinterpret_cast<uintptr_t>(chunk_base_));
}

void LogBasedMemoryAllocatorBase::SealChunk(GeneralChunk* chunk) {
  OnSealChunk(chunk);
  size_t alloc_bytes =
      chunk->meta.num_allocated_bytes.load(std::memory_order_relaxed);
  // write stop mark if there is space more than 4bytes left in chunk
  if (alloc_bytes + kHeaderLen < kLogChunkSize) {
    const uint32_t stop_mark = kChunkStopMark;
    memcpy(chunk->data + alloc_bytes, &stop_mark, sizeof(stop_mark));
  }

  // update meta
  chunk->meta.num_freed_bytes.fetch_add(kLogChunkSize - alloc_bytes,
                                        std::memory_order_relaxed);
  chunk->meta.num_allocated_bytes.store(kLogChunkSize,
                                        std::memory_order_relaxed);
  chunk->write_lock.store(false, std::memory_order_release);
  UnrefChunk(chunk);
}

void LogBasedMemoryAllocatorBase::RefChunk(GeneralChunk* chunk) {
  chunk->meta.ref_cnt.fetch_add(1, std::memory_order_release);
}

void LogBasedMemoryAllocatorBase::UnrefChunk(GeneralChunk* chunk) {
  DCHECK(chunk->meta.ref_cnt >= 1);
  if (chunk->meta.ref_cnt.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    DCHECK(chunk->unsealed_cnt == 0);
    VLOG(1) << "Chunk " << chunk->meta.id << " will be joined free_list!";
    // reset meta
    chunk->meta.num_allocated_bytes.store(0, std::memory_order_relaxed);
    chunk->meta.num_freed_bytes.store(0, std::memory_order_relaxed);

    {
      allocator_recycled_chunks_counter_->Increase();
      // add to free list
      std::lock_guard<std::mutex> guard(alloc_mtx_);
      free_chunks_.emplace_back(chunk);
    }

    // update global stats at last
    DCHECK(num_occupied_bytes_ >= kLogChunkSize);
    num_occupied_bytes_.fetch_sub(kLogChunkSize, std::memory_order_relaxed);
    VLOG(4) << "UnrefChunk "
            << "num_occupied_bytes_:"
            << num_occupied_bytes_.load(std::memory_order_relaxed);
  }
}

LogBasedMemoryAllocatorBase::GeneralChunk*
LogBasedMemoryAllocatorBase::RawPointerToChunk(char* ptr) {
  uintptr_t aligned_addr = reinterpret_cast<uintptr_t>(ptr) &
                           ~(static_cast<uintptr_t>(kLogChunkSize) - 1ULL);
  size_t pos = (aligned_addr - reinterpret_cast<uintptr_t>(chunk_base_)) /
               static_cast<uintptr_t>(kLogChunkSize);
  DCHECK(pos < chunks_.size());
  auto& it = chunks_[pos];
  return it.load(std::memory_order_acquire);
}

void LogBasedMemoryAllocatorBase::RegisterMetrics(int numa_id_prefix) {
  CHECK(alloc_metric_registry_ != nullptr);

  // Configure allocator metric prefix
  // If StorageGCController is created by a DRAM storage, then prefix is -1
  std::string allocator_metric_prefix = "";
  if (numa_id_prefix >= 0) {
    allocator_metric_prefix += "numa";
    allocator_metric_prefix += std::to_string(numa_id_prefix);
  }
  allocator_error_metric_prefix_ = allocator_metric_prefix;
  allocator_error_metric_prefix_ += "failed_allocator_counter";
  // Register allocator-related metrics
  // TODO(kaiwu.kw) Merge all cache-related metrics into one file and provide a
  // unified interface for metrics measurement Allocator error-related metrics
  for (auto error_type : ALLOCATOR_ERROR_COUNTER_TAGS_MAP) {
    std::shared_ptr<noodle::AtomicCounter> error_counter =
        std::make_shared<noodle::AtomicCounter>();
    noodle::MetricId error_metric_id(
        allocator_error_metric_prefix_,
        {{ALLOCATOR_ERROR_ID_TAG, error_type.second}});
    alloc_metric_registry_->MustRegister<noodle::AtomicCounter>(
        std::move(error_metric_id), error_counter);
    allocator_error_metrics_map_[error_type.first] = error_counter;
  }
  allocator_recycled_chunks_counter_ =
      alloc_metric_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId(allocator_metric_prefix + "_recycled_chunks"),
          std::make_shared<noodle::AtomicCounter>());
}

void LogBasedMemoryAllocatorBase::HandleErrorMetrics(std::string error_type) {
  CHECK(alloc_metric_registry_ != nullptr);
  CHECK(allocator_error_metric_prefix_.size() != 0);
  if (!ALLOCATOR_ERROR_COUNTER_TAGS_MAP.count(error_type) ||
      !allocator_error_metrics_map_.count(error_type)) {
    LOG(WARNING) << "Use invalid error type to report the metric";
  } else {
    allocator_error_metrics_map_[error_type]->Increase();
  }
}

}  // namespace mtcache
