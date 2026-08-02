#pragma once

#include "allocator.h"

#include <noodle/metric/metric_registry.h>

#include <atomic>
#include <forward_list>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace mtcache {

constexpr uint32_t kLogChunkSize = 32 * (1 << 20);

class LogBasedMemoryAllocatorBase : public LogBasedMemoryAllocator {
 public:
  // According to CPP standard, calling virtual method in deconstructor is UB.
  // Therefore `LogBasedMemoryAllocatorBase` cannot call virtual method
  // implemented by derived class to release resource in its deconstructor.
  // The workaround is passing alloc/free implementation by std::function
  // instead of virtual method. The class follows `Template Method Design
  // Pattern`.
  using AllocateMemoryObjectFunc =
      std::function<noodle::Result<void* /* addr */, CacheError>(
          void* /* address */, ChunkID, size_t /* object_len */,
          size_t /* alignment */)>;
  using FreeMemoryObjectFunc = std::function<noodle::Result<void, CacheError>(
      void* /* addr */, size_t /* len */)>;

  LogBasedMemoryAllocatorBase(
      AllocateMemoryObjectFunc alloc_mem_obj_func,
      FreeMemoryObjectFunc free_mem_obj_func,
      LogBasedAllocatorGCEventListener* gc_event_listener,
      size_t capacity_bytes, size_t gc_reserved_bytes, size_t max_thread_num,
      std::shared_ptr<noodle::MetricRegistry> registry,
      int numa_id_prefix = -1);

  ~LogBasedMemoryAllocatorBase() override;

  // The `Allocate` method in LogBasedMemoryAllocator will allcoate at lease
  // `len + kHeaderLen` space where `len` is the desired memory length
  // (payload_size)  and kHeaderLen=sizeof(uint32_t) is a uint32 before the
  // payload standing for the following memory space length. The caller should
  // never access the `header` field.
  //
  //   DRAM:                           PMEM:
  //   payload_size                    payload_size+sizeof(crc32)
  //      |                               |
  //      |  return_ptr                   |  return_ptr
  //      |  |                            |  |
  //      |  |    payload                 |  |    payload          crc32
  //      |  |      |                     |  |      |               |
  //      v  v      v                     v  v      v               v
  //    +-------------------------+     +-------------------------+---+
  //    |    |                    |     |    |                    |   |
  //    +-------------------------+     +-------------------------+---+
  //
  // For DRAM allocator, the value of header is equal to payload_size.
  // But for PMEM allocator, the value of header is euqal to
  // `payload_size + sizeof(uint32_t)` where the terminal uint32 is the
  // checksum (crc32) of the payload (the size field in the header is not
  // inlcuded when computing crc32). The upper caller should never access
  // the checksum field either.
  //
  // The return address is a pointer to the payload.
  noodle::Result<char*, CacheError> Allocate(size_t len) override;

  noodle::Result<void, CacheError> Free(char* ptr, size_t len) override;

  bool Contains(const char* ptr) const override;

  // Persist this address pointed by `ptr`.
  //
  // The param `len` is the size of allocated payload from `Allocate()`,
  // excluding the 4B space for crc32 in PMEM allocator.
  //
  // DRAM allocator should not call this method. Instead it should call the
  // overloaded version `Seal(char* ptr)`.
  // As for PMEM allocator it will store the crc at [ptr + len, ptr + len + 4).
  // See memory layout related comments at `Allocate()`.
  noodle::Result<void, CacheError> Seal(char* ptr, size_t len,
                                        uint32_t crc32) override;

  // Used for DRAM allocator which does not need crc32.
  // The PMEM allocator will implement this function by computing the crc32
  // with data already written to PMEM. So it will bring much read traffic to
  // PMEM.
  noodle::Result<void, CacheError> Seal(char* ptr) override;

  noodle::Result<void, CacheError> IterateRecyclableChunkMeta(
      const std::function<bool(const ChunkMeta* meta)>& func) const override;

  noodle::Result<void, CacheError> RetrieveChunkMeta(
      ChunkID chunk_id,
      const std::function<void(const ChunkMeta* meta)>& func) override;

  noodle::Result<void, CacheError> GC(const ChunkID* chunk_id_arr,
                                      size_t arr_len) override;

  noodle::Result<AllocatorStats, CacheError> GetStats() const override;

  noodle::Result<size_t, CacheError> Capacity() const override;

  size_t TEST_GetGCLeftChunksSize() const { return gc_left_chunks_.size(); }

  size_t TEST_GetNumInitedTLSCtx() const { return num_inited_tls_ctx_; }

 protected:
  struct GeneralChunk;

  // Called before executing actual `Seal` logic
  // Derived class may override the method to change certain behavior of
  // allocator
  //
  // The param `payload_len` is the size of payload data, excluding the 4B
  // space used by crc32.
  //
  // If `payload_len` is 0, the subclass should compute the crc32 itself with
  // the length from the header before payload. See the comments at `Allocator`
  // method for details about memory layout.
  virtual void OnSeal(GeneralChunk* chunk, char* payload_ptr,
                      size_t payload_len, uint32_t crc32) {}

  // Called before executing actual `SealChunk` logic
  // Derived class may override the method to change certain behavior of
  // allocator
  virtual void OnSealChunk(GeneralChunk* chunk) {}

 protected:
  // binary format: mem_obj_len:uint32_t + data:char[mem_obj_len]
  static constexpr uint32_t kHeaderLen = sizeof(uint32_t);
  // When a memory object is deleted, the hightest bit of its uint32_t capacity
  // field will be filpped to 1. kTombstoneMask is bit operation mask for
  // flipping.
  static constexpr uint32_t kTombstoneMask = 1U << 31U;
  static constexpr uint32_t kRecordLenMask = (kTombstoneMask - 1U);
  // In order to `SealChunk`, a uint32_t kChunkStopMark is written to the end
  // of the chunk. So GC task can know where valid data ends in that chunk. It's
  // chunk's EOF.
  static constexpr uint32_t kChunkStopMark = 0;

  struct GeneralChunk {
    // Memory directly allocated from OS
    char* data;
    // One special scenario in which `meta` is changed is `SealChunk`
    // In `SealChunk`:
    // 1. the size of unused space will be added to `num_freed_bytes`
    // 2. `num_allocated_bytes` will be set to kLogChunkSize
    ChunkMeta meta;
    // How many objects are not yet sealed
    std::atomic<unsigned int> unsealed_cnt{0};
    // How many bytes of data have been flushed
    // Currently, it is only used in `LogBasedMemoryAllocatorPMem` in which
    // flush policy is set to `kMiniBatchFlush`
    unsigned int flushed_bytes = 0;
    // No simultaneous write
    std::atomic<bool> write_lock{false};

    GeneralChunk(ChunkID id, char* data) : data(data) { meta.id = id; }
  };

  struct ThreadLocalContext {
    // The thread binding to the context can allocate memory from `chunk`
    // wait-freely
    GeneralChunk* chunk = nullptr;

    // TLS-level stats
    // Those stats have no relation with `ChunkMeta` stats in `GeneralChunk`
    // above. Updating a single global variable(num_allocated_bytes /
    // num_freed_bytes etc.) would cause serious cache line conficts decreasing
    // performance. So we record stats thread-locally, then aggregate those
    // TLS-level stats to obtain correct global stats.
    std::atomic<size_t> num_allocated_bytes = 0;
    std::atomic<size_t> num_freed_bytes = 0;
    std::atomic<size_t> num_gc_move_bytes = 0;
  };

  noodle::Result<ThreadLocalContext*, CacheError> GetThreadLocalContext();

  // Different purpose comes with different capacity
  enum class AllocationPurpose {
    // Normal tasks e.g. `Allocate` can only use `capacity_bytes_` space
    kNormal,
    // Only GC tasks are able to use GC-reserved chunks
    kGC,
  };

  // Reserve the continuous virtual memory of size `capacity_and_gc_bytes_`
  // for future allocation.
  noodle::Result<bool, CacheError> ReserveVM();

  // Release the reserved continuous range of virtual memory from `ReserveVM`.
  // The VM must be "continuous", which means no "holes" should exist within
  // the VM.
  noodle::Result<void, CacheError> ReleaseVM();

  // During `ReserveVM`, the allocator gets a continuous range of VM with the
  // size of capacity_and_gc_bytes_, pointed by `chunk_base_`.
  // This function allocates `kLogChunkSize` DRAM/PMEM at the
  // "next usable position" within the "reserved VM", where the "next usable
  // position" is computed by:
  //   next_usable_postion = next_chunk_id * kLogChunkSize + chunk_base_
  // If there are reusable chunks in the free chunk list, this function will
  // reuse the chunk in it to avoid allocating new chunk.
  //
  // chunk_id:   0                1               2
  //             |                |               |
  //             v                v               v
  //          +-----------------------------------------------------------+
  //          | kLogChunkSize | kLogChunkSize | kLogChunkSize |           |
  //          +-----------------------------------------------------------+
  //          |
  //          v
  //       chunk_base_
  noodle::Result<GeneralChunk*, CacheError> AllocateChunk(
      AllocationPurpose purpose = AllocationPurpose::kNormal);

  // Get the mapping address of the chunk.
  void* ChunkId2Addr(ChunkID id);

  // After calling `SealChunk`, no more data appends to `chunk`,
  // `num_allocated_bytes` in `meta` is set to kLogChunkSize and `chunk` is
  // ready to be picked as GC candidate
  void SealChunk(GeneralChunk* chunk);

  void RefChunk(GeneralChunk* chunk);

  // `chunk` is moved to free list if `ref_cnt` once becomes 0
  void UnrefChunk(GeneralChunk* chunk);

  GeneralChunk* RawPointerToChunk(char* ptr);

  void RegisterMetrics(int numa_id_prefix);

  void HandleErrorMetrics(std::string error_type);

  AllocateMemoryObjectFunc alloc_mem_obj_func_;
  FreeMemoryObjectFunc free_mem_obj_func_;

  LogBasedAllocatorGCEventListener* const gc_event_listener_;

  // Max space the allocator can use
  const size_t capacity_bytes_;
  const size_t capacity_and_gc_bytes_;

  // Container of all chunks
  // Its size is fixed after init, therefore chunks_[n] is lock-free.
  std::vector<std::atomic<GeneralChunk*>> chunks_;

  // Container of all thread local contexts
  // Its size is fixed after init, therefore ctxs_[n] is lock-free.
  std::vector<std::unique_ptr<ThreadLocalContext>> ctxs_;

  // How many thread local contexts have been initialized
  std::atomic<size_t> num_inited_tls_ctx_{0};

  // How many bytes the allocator currently occupies
  // It's always <= capacity_bytes_
  std::atomic<size_t> num_occupied_bytes_{0};

  // TODO(lyj): may make it lock-free
  std::mutex gc_left_chunks_mtx_;
  // In each GC run, garbage are recycled by merging chunks. The last newly
  // created chunk is moved to `gc_left_chunks_` because the chunk is likely to
  // be not full. Next GC run can continue to use those not full chunks.
  std::vector<GeneralChunk*>
      gc_left_chunks_;  // protected by gc_left_chunks_mtx_

  // TODO(lyj): may make it lock-free
  std::mutex alloc_mtx_;
  // Chunks in the container are ready for reuse
  std::vector<GeneralChunk*> free_chunks_;  // protected by alloc_mtx_

  // ChunkID for next chunk allocation
  std::atomic<ChunkID> next_chunk_id_{0};

  std::mutex recycle_mtx_;
  // Recycled Chunk_id list
  std::forward_list<ChunkID> recycled_chunk_id_;

  // Pointer to the first chunk address.
  char* chunk_base_{nullptr};

  std::shared_ptr<noodle::MetricRegistry> alloc_metric_registry_;

  // Number of chunks recycled by the allocator when refcount is zero
  std::shared_ptr<noodle::AtomicCounter> allocator_recycled_chunks_counter_;

  // Metric registry prefix for allocator-related metrics
  std::string allocator_error_metric_prefix_;

  std::unordered_map<std::string, std::shared_ptr<noodle::AtomicCounter>>
      allocator_error_metrics_map_;

  class GCContext;
};

}  // namespace mtcache
