#pragma once

#include "allocator/log_allocator.h"
#include "allocator/pool_allocator.h"

namespace mtcache {

class LogBasedMemoryAllocatorDram : public LogBasedMemoryAllocatorBase {
 public:
  LogBasedMemoryAllocatorDram(
      LogBasedAllocatorGCEventListener* gc_event_listener,
      size_t capacity_bytes, size_t gc_reserved_bytes, size_t max_thread_num,
      std::shared_ptr<noodle::MetricRegistry> registry);

  ~LogBasedMemoryAllocatorDram() override = default;
};

class PoolBasedMemoryAllocatorDram : public PoolBasedMemoryAllocatorBase {
 public:
  PoolBasedMemoryAllocatorDram(size_t capacity_bytes, size_t max_thread_num,
                               size_t obj_len);

  ~PoolBasedMemoryAllocatorDram() override = default;
};

}  // namespace mtcache
