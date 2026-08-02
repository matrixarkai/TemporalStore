#pragma once

#include "allocator/dram_allocators.h"
#include "gc_controller.h"
#include "storage/async_writer.h"
#include "storage_engine.h"

#include <memory>

namespace mtcache {

// StorageEngineDram defines the storage engine that stores data in DRAM.
class StorageEngineDram : public StorageEngine {
 public:
  // Construct StorageEngineDram with a pointer (gc_cb) to CacheIntance.
  // The CacheIntance is used to replace old CacheBuffer with new CacheBuffer
  // for GCEventListener in LogBasedAllocator.
  // capacity specify the maximum space that the dram storage engine can use,
  // including allocator overhead.
  // registry is the metric registry that is passed through to the allocator and
  // gc instance to register allocator and GC related metrics.
  StorageEngineDram(uint64_t capacity, StorageEngine::GCCopyCallback* gc_cb,
                    std::shared_ptr<noodle::MetricRegistry> registry);

  ~StorageEngineDram() override = default;

  bool Start() override;

  bool Stop() override;

  noodle::Result<CacheBufferSharedPtr, CacheError> Get(
      const std::string& key) override;

  noodle::Result<CacheBufferSharedPtr, CacheError> Put(
      const std::string& key, folly::IOBuf value) override;

  folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>> AsyncPut(
      CacheBufferSharedPtr buffer, AsyncPutCb cb) override;

  noodle::Result<void, CacheError> Delete(CacheBufferPtr buffer) override;

  noodle::Result<void, CacheError> Reset() override;

  noodle::Result<void, CacheError> RecoverData(
      RecoverDataCallback* callback) override;

  // For unit test only!
  LogBasedMemoryAllocatorDram* TEST_GetLogAllocator() {
    return dynamic_cast<LogBasedMemoryAllocatorDram*>(allocator_.get());
  }

  PoolBasedMemoryAllocatorDram* TEST_GetPoolAllocator() {
    return dynamic_cast<PoolBasedMemoryAllocatorDram*>(allocator_.get());
  }

 private:
  AllocatorType allocator_type_;

  // Log-based allocator variables below
  // The GCEventListener for log-based allocator. Useful only when log-based
  // dram allocator is used.
  std::unique_ptr<LogBasedAllocatorGCEventListener> listener_;

  // The GC instance, use to mointor allocator's stats and trigger gc
  // Useful only when log-based dram allocator is used.
  std::unique_ptr<StorageGCController> gc_instance_;

  std::unique_ptr<CacheAllocator> allocator_;

  // async_writer_ is used to handle async data promotion from PMEM cache to
  // DRAM cache.
  std::unique_ptr<AsyncWriter> async_writer_;
};

}  // namespace mtcache
