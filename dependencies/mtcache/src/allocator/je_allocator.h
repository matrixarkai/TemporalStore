#pragma once

#include "allocator.h"

#if !__SANITIZE_ADDRESS__
#include <jemalloc/jemalloc.h>
#endif
#include <noodle/metric/metric_registry.h>

namespace mtcache {

// JeAllocator is an allocator used by DRAM storage engine to allocate
// memory space through jemalloc.
// Unlike LogAllocator, JeAllocator does not need GC, so it is more simply
// than LogAllocator and does not have many complicated data structures.
// JeAllocator can only be used for DRAM.
class JeAllocator final : public CacheAllocator {
 public:
  JeAllocator(size_t capacity_bytes,
              std::shared_ptr<noodle::MetricRegistry> registry);

  ~JeAllocator() = default;

  noodle::Result<char*, CacheError> Allocate(size_t len) override;

  noodle::Result<void, CacheError> Free(char* ptr, size_t len) override;

  bool Contains(const char* ptr) const override;

  // This function is an empty function in JeAllocator because JeAllocator
  // is only used by DRAM storage and it does not have data structures like
  // `Chunk` in LogAllocator.
  noodle::Result<void, CacheError> Seal(char* ptr, size_t len,
                                        uint32_t crc32) override;

  noodle::Result<void, CacheError> Seal(char* ptr) override;

  noodle::Result<size_t, CacheError> Capacity() const override;

  noodle::Result<AllocatorStats, CacheError> GetStats() const override;

  AllocatorStats TEST_GetAllocMetrics() const;

 private:
  void RegisterMetrics(std::shared_ptr<noodle::MetricRegistry> registry);

  const size_t capacity_bytes_;

  // Metric counters for allocate/free stats gathered from all tls ctx.
  std::shared_ptr<noodle::AtomicCounter> allocated_bytes_counter_;
  std::shared_ptr<noodle::AtomicCounter> freed_bytes_counter_;
  std::shared_ptr<noodle::AtomicCounter> occupied_bytes_counter_;
  // Metric counters for errors.
  std::shared_ptr<noodle::AtomicCounter> no_space_counter_;
  std::shared_ptr<noodle::AtomicCounter> alloc_zero_counter_;
};

}  // namespace mtcache
