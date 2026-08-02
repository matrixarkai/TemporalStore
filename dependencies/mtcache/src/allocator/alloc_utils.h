#pragma once

#include "allocator/allocator.h"
#include "common/cache_error.h"

#include <noodle/base/result.h>

#include <string>
#include <vector>

namespace mtcache {

// ParseAllocatorType is a utility function to parse allocator type
// from config option to enum AllocatorType.
AllocatorType ParseAllocatorType(const std::string& allocator_type);

// Allocate memory object with user-defined alignment from DRAM.
// REQUIRES: `object_len` and `alignment` are both multiples of page
// size(normally 4KB).
noodle::Result<void*, CacheError> DramAllocateObject(size_t object_len,
                                                     size_t alignment);

// Allocate memory object align to the vm pre_allocated.
// Address is the location that new chunk will be mmap at.
noodle::Result<void*, CacheError> DramAllocateObject_v2(void* address,
                                                        size_t object_len);

// Free memory object previously returned by `DramAllocateObject`.
noodle::Result<void, CacheError> DramFreeObject(void* addr, size_t len);

// Allocate memory object with user-defined alignment by creating a file in PMem
// device.
// REQUIRES:
// 1. The folder of `filename` is valid and allowed to create and open a file.
// 2. `object_len` and `alignment` are both multiples of page size(normally
// 4KB).
noodle::Result<void*, CacheError> PMemAllocateObject(
    const std::string& filename, size_t object_len, size_t alignment);

// Allocate memory object specialized address by creating a file in PMem
// device.
// REQUIRES:
// 1. The folder of `filename` is valid and allowed to create and open a file.
// 2. `object_len` is multiples of page size(normally 4KB).
// 3. address is the location of pre Allocated vm. The chunk address is
// continuous there.
noodle::Result<void*, CacheError> PMemAllocateObject_v2(
    void* address, const std::string& filename, size_t object_len);

// Free memory object previously returned by `PMemAllocateObject`.
noodle::Result<void, CacheError> PMemFreeObject(void* addr, size_t len);

// Allocate continuous virtual memory of size `len` with the alignment of
// `align`. This method is intended to "reserve" a large continuous virtual
// memory for later usage. Callers must NOT read or write to the returned
// address immediately. They should call `DramAllocateObject_v2` or
// `PMemAllocateObject_v2` with the returned VM of this method and then
// read/write to it.
//
// Note that the `align` must be multiple times of HUGE_PAGE_SIZE, which is 2MB
// in our settings.
//
// This function should be called at initialization time. The pre-allocated
// VM should be freed by `PostFree`.
noodle::Result<void*, CacheError> PreAllocate(size_t len, size_t align);

// Free the pre-allocated VM in `PreAllocate`.
// This function should be called at destruction time.
noodle::Result<void, CacheError> PostFree(void* addr, size_t len);

// Flush the processor caches
void PMemFlush(void* addr, size_t len);

// Wait for any pmem stores to drain from HW buffers
void PMemDrain();

// PMemPersist = PMemFlush + PMemDrain
// PMem users are expected to invoke the function for most cases
void PMemPersist(void* addr, size_t len);

// Get ID to accessing thread-local resources
// Different from traditional thread ID, the thread local resource ID is
// recycled after current thread exits. Next thread may reuse the same
// thread-local resource ID.
//
// e.g. Thread A spawns: GetThreadLocalResourceID() returns 1
//      Thread B spawns: GetThreadLocalResourceID() returns 2
//      Thread A exits.
//      Thread C spawns: GetThreadLocalResourceID() returns 1(ID reuse)
//      Thread D spawns: GetThreadLocalResourceID() returns 3
int GetThreadLocalResourceID();

// Get every chunk's filename under data_path.
// If `expected_len` is greater than -1, check the chunk file's lenth and return
// only filenames whose length is equal to `expected_len`.
// If `invalid_fname` is not nullptr, set the filenames whose lenth is NOT
// equal to `expected_len` into `invalid_fname`.
std::vector<std::string> GetPmemFileName(
    const std::string& data_path, int64_t expected_len = -1,
    std::vector<std::string>* invalid_fname = nullptr);

// Open & mmap file for recovery
noodle::Result<void*, CacheError> PMemMapFile(void* addr,
                                              const std::string& filename,
                                              size_t object_len);

// Delete pmem file. Return true on success, false on failure.
bool DeletePmemFile(const std::string& path);

}  // namespace mtcache
