#pragma once

#include "common/cache_error.h"

#include <noodle/base/result.h>

#include <atomic>
#include <cstdint>
#include <functional>

namespace mtcache {

using ChunkID = uint64_t;

struct AllocatorStats {
  // Total number of bytes of data that is allocated, including the bytes that
  // has been freed.
  size_t num_allocated_bytes = 0;
  // Number of bytes of data that is freed. For JeAllocator, freed bytes can
  // be reused immediately. But for LogAllocator, freed bytes can not be reused
  // until the chunk is gc-ed.
  size_t num_freed_bytes = 0;
  // Number of bytes of data occupied. For JeAllocator, the value is equal to
  // num_allocated_bytes - num_freed_bytes. For LogAllocator, this is the
  // total bytes of non-empty chunks.
  size_t num_occupied_bytes = 0;
};

// AllocatorType is the enum definition of all allocator types
// supported by MTCache.
enum class AllocatorType : uint8_t {
  kLogBasedAllocator = 0,
  kPoolBasedAllocator = 1,
  kJeAllocator = 2,
  kMaxCode
};

// Every log chunk has its own meta
struct ChunkMeta {
  // unique identifer for the chunk
  ChunkID id = 0;
  // Number of bytes of data that is allocated to the upper layer. When a chunk
  // is in writing, the value is the actual allocated bytes. When a chunk is
  // sealed, the value is always equal to kLogChunkSize. When a chunk is freed
  // or after GC, the value is 0.
  std::atomic<size_t> num_allocated_bytes = 0;
  // number of bytes of data marked as garbage in a chunk
  std::atomic<size_t> num_freed_bytes = 0;
  // the reference counter for the chunk
  std::atomic<uint64_t> ref_cnt{0};
};

// Every pool chunk has its own meta
struct PoolChunkMeta {
  // unique identifer for the chunk
  ChunkID id = 0;
  // number of memory objects are allocated
  std::atomic<size_t> num_alloc_objects = 0;
};

// This is the abstract allocator class used in cache module.
// Pool-based allocator (used for DRAM cache) and log-based allocator (used
// for PMEM allocator) should inherit from this class. The child classes can
// add implementation-specific functions. But the common functions in this
// class must be included.
class CacheAllocator {
 public:
  virtual ~CacheAllocator() {}

  // Allocate `len` length memory object from the underlying device(DRAM or
  // PMEM)
  // Different from `malloc`, alignment of the returned pointer is
  // implementation-defined.
  // For log-based allocator, this function will
  // increase the chunk's ref count this data record comes from.
  //
  // For PMEM allocator, the function will allocate (len + 4) bytes because
  // it needs extra 4B to store the checksum.
  // See ./base.h for more details.
  //
  // noodle::Result::IsOk() returns true on success.
  // Errors::kAllocatorOutOfSpace is returned when the remaining space isn't
  // large enough for the request. For log-based allocator, the caller is
  // very likely to `Free` part of memory objects, call `GC` and then retry.
  virtual noodle::Result<char*, CacheError> Allocate(size_t len) = 0;

  // Check whether `ptr` was allocated from this allcator.
  virtual bool Contains(const char* ptr) const = 0;

  // Free the data record pointed by ptr.
  //
  // For pool-based allocator, this function will actually free the memory
  // occupied.
  // For log-based allocator, this function will
  //   1. mark this data record as garbage and
  //   2. decrease the ref count of the chunk this data record belongs to
  // The space will be reclaimed by garbage collection later.
  //
  // IMPORTANT NOTE:
  // For log-based allocator, users should follow the paradigm,
  // Allocate => [Write User Data] => Seal => Free
  // Different from pool-based allocator, there is one extra step named `Seal`
  // and should be executed ASAP after data writing completed. The reason is
  // only sealed(immutable) objects are movable. Too many unmovable objects
  // vastly reduce GC efficiency.
  //
  // noodle::Result::IsOk() returns true on success.
  virtual noodle::Result<void, CacheError> Free(char* ptr, size_t len) = 0;

  // Write crc32 at `[ptr + len, ptr + len + 4)` and persist this address
  // pointed by `ptr`.
  // See ./base.h for details.
  virtual noodle::Result<void, CacheError> Seal(char* ptr, size_t len,
                                                uint32_t crc32) = 0;

  // Persist this address pointed by `ptr`.
  // See ./base.h for details.
  virtual noodle::Result<void, CacheError> Seal(char* ptr) = 0;

  // Expose overall stats of the allocator
  //
  // noodle::Result::IsOk() returns true on success.
  virtual noodle::Result<AllocatorStats, CacheError> GetStats() const = 0;

  // Return max space size the allocator can use in bytes
  //
  // noodle::Result::IsOk() returns true on success.
  virtual noodle::Result<size_t, CacheError> Capacity() const = 0;
};

// Pool based allocator.
// TODO(dongbenchao.dbc) We could use glibc allocator or TcMalloc first and
// replace it with our own pool allocator in the future.
class PoolBasedMemoryAllocator : public CacheAllocator {
 public:
  virtual ~PoolBasedMemoryAllocator() {}

  // Functions for pool-based allocator are listed below.
};

// The abstract interface for a log-based memory allocator
//
// The allocator can allocate variable-size memory objects in DRAM or PMEM.
// Compared to slab-based memory allocator e.g. ptmalloc,
// LogBasedMemoryAllocator may need to move data in order to defragment.
// There is no thread pool built-in for defragmentation. It is the upper layer's
// duty to submit tasks. A set of helper functions are provided.
class LogBasedMemoryAllocator : public CacheAllocator {
 public:
  virtual ~LogBasedMemoryAllocator() {}

  // Functions for log-based allocator are listed below.

  // Mark the previously allocated memory object(*ptr) immutable and movable
  // After calling `Seal`, the upper layer cannot modify the object again, and
  // then the allocator will do essential flush and append checksum.
  // The method serves two purposes:
  // 1. Persistence
  // 2. GC

  // Iterate over all recyclable chunk meta in descending num_freed_bytes order
  // and invoke `func` on each of chunk meta
  // The upper layer may use the method to pick up GC candidates.
  //
  // noodle::Result::IsOk() returns true on success.
  virtual noodle::Result<void, CacheError> IterateRecyclableChunkMeta(
      const std::function<bool(const ChunkMeta* meta)>& func) const = 0;

  // Retrieve specific chunk's meta by ChunkID and invoke `func` on it
  // The upper layer may use the method to impl better GC policy.
  //
  // noodle::Result::IsOk() returns true on success.
  virtual noodle::Result<void, CacheError> RetrieveChunkMeta(
      ChunkID chunk_id,
      const std::function<void(const ChunkMeta* meta)>& func) = 0;

  // Move all valid data of input chunks to new chunks in order to defragment
  // It's worth noting that a input chunk may not be recycled inside this
  // method, because there may be other holders of the chunk. Such chunks will
  // be recycled later when the last holder releases its hold by calling `Free`.
  //
  // noodle::Result::IsOk() returns true on success.
  virtual noodle::Result<void, CacheError> GC(const ChunkID* chunk_id_arr,
                                              size_t arr_len) = 0;
};

// A listener interface for pointer replacement
//
// The upper layer should pass its impl of LogBasedAllocatorGCEventListener to
// the concrete impl of LogBasedMemoryAllocator via either constrcutor
// or setter method. When the allocator starts GC, only valid memory objects are
// copied to new place. Users of 'old_ptr' should replace it with 'new_ptr' upon
// notification via the 'OnGCCopy' method. Eventually, there will be no pointer
// pointing to invalid or old memory objects and the space that is GC-ed can be
// reclaimed safely.
class LogBasedAllocatorGCEventListener {
 public:
  virtual ~LogBasedAllocatorGCEventListener() = default;

  // During pointer replacement phase of GC, the impl of
  // LogBasedMemoryAllocator will call this method on each of valid memory
  // objects it wants to move. The method is supposed to complete 2 things:
  // 1. Create a new ref object for `new_ptr`.
  // 2. Replace the upper layer's old ref object with new one, therefore
  // following user requests will not access the old memory object whose ref
  // counter will become zero then trigger LogBasedMemoryAllocator's `Free`
  // method eventually.
  //
  // noodle::Result::IsOk() returns true on success.
  virtual noodle::Result<void, CacheError> OnGCCopy(const char* old_ptr,
                                                    const char* new_ptr) = 0;
};

}  // namespace mtcache
