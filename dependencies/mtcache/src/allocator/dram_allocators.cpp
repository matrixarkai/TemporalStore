#include "allocator/dram_allocators.h"

#include "allocator/alloc_utils.h"
#include "common/logging.h"

namespace mtcache {

LogBasedMemoryAllocatorDram::LogBasedMemoryAllocatorDram(
    LogBasedAllocatorGCEventListener* gc_event_listener, size_t capacity_bytes,
    size_t gc_reserved_bytes, size_t max_thread_num,
    std::shared_ptr<noodle::MetricRegistry> registry)
    : LogBasedMemoryAllocatorBase(
          [](void* addr, ChunkID id, size_t object_len, size_t alignment) {
            return DramAllocateObject_v2(addr, object_len);
          },
          [](void* addr, size_t len) -> noodle::Result<void, CacheError> {
            // Can not free VM because log-based allocator reserved large range
            // of VM in advance and it will release it at destruction. If we
            // free it here, it will cause holes with the VM and make the
            // releasing process fail.
            //
            // TODO(dongbenchao) we may want to release the physical memory here
            // by calling `madvice(addr, len, MADV_DONTNEED)`
            LOG(FATAL) << "Can not free object in log-based allocator";
            return &Errors::kNotImplemented;
          },
          gc_event_listener, capacity_bytes, gc_reserved_bytes, max_thread_num,
          registry) {}

PoolBasedMemoryAllocatorDram::PoolBasedMemoryAllocatorDram(
    size_t capacity_bytes, size_t max_thread_num, size_t obj_len)
    : PoolBasedMemoryAllocatorBase(
          [](ChunkID id, size_t object_len, size_t alignment) {
            return DramAllocateObject(object_len, alignment);
          },
          DramFreeObject, capacity_bytes, max_thread_num, obj_len) {}

}  // namespace mtcache
