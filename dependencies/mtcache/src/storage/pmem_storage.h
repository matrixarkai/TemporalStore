#pragma once

#include "allocator/pmem_allocators.h"
#include "pmem_dispatcher.h"
#include "storage_engine.h"

#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/concurrency/UnboundedQueue.h>

namespace mtcache {

class PmemAllocatorRecoverListenerImpl : public PmemAllocatorRecoverListener {
 public:
  PmemAllocatorRecoverListenerImpl(StorageEngine* engine,
                                   StorageEngine::RecoverDataCallback* cb,
                                   size_t num_estimate_items = 8)
      : engine_(engine), cb_(cb), record_map_(num_estimate_items) {}

  ~PmemAllocatorRecoverListenerImpl() override = default;

  // Free the duplicated records, call cb_ for each valid records and return
  // the number of valid records.
  uint64_t FinishRecover();

  // Return true if the input record was met before (duplicated).
  // Return false if the input record is met for the first time.
  bool OnScanRecord(const char* addr, size_t len) override;

 private:
  StorageEngine* engine_;

  StorageEngine::RecoverDataCallback* cb_;

  // Records got from pmem recovery. If the record is valid (only meet once
  // during recovery), the K-V in this map is cache key and record address.
  // If the record is invalid (meet at least twice), the K-V in this map is
  // cache key and nullptr.
  folly::ConcurrentHashMap<std::string_view, const char*> record_map_;

  // Duplicated record during scanning pmem chunks in recovery. These records
  // should be freed after finish scanning.
  folly::UMPSCQueue<const char*, true> dup_records_;
};

// StorageEnginePMEM defines the storage engine that stores data in PMEM.
class StorageEnginePMem : public StorageEngine {
 public:
  // Construct StorageEnginePMem with a pointer (gc_cb) to CacheIntance.
  // The CacheIntance is used to replace old CacheBuffer with new CacheBuffer
  // for GCEventListener in LogBasedAllocator.
  // capacity specifies the maximum space that the dram storage engine can use,
  // including allocator overhead.
  // pmem_path specifies the mounting point to mmap pmem in fs_dax mode.
  // registry is the metric registry that is passed through to the allocator and
  // gc instance to register allocator and GC related metrics.
  StorageEnginePMem(uint64_t capacity,
                    const std::vector<std::string>& pmem_paths,
                    StorageEngine::GCCopyCallback* gc_cb,
                    std::shared_ptr<noodle::MetricRegistry> registry);

  ~StorageEnginePMem() override = default;

  bool Start() override;

  bool Stop() override;

  // Join the PMEM writer executor(s) so that all writing-pmem tasks complete.
  // For test or benchmark only.
  void TEST_JoinPmemWriteExecutor();

  noodle::Result<CacheBufferSharedPtr, CacheError> Get(
      const std::string& key) override;

  [[deprecated("Use StorageEnginePMem::AsyncPut instead.")]] noodle::Result<
      CacheBufferSharedPtr, CacheError>
  Put(const std::string& key, folly::IOBuf value) override;

  folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>> AsyncPut(
      CacheBufferSharedPtr buffer, AsyncPutCb cb) override;

  noodle::Result<CacheBufferSharedPtr, CacheError> TEST_PutToNuma(
      const std::string& key, std::unique_ptr<folly::IOBuf> value,
      int32_t numa_id);

  [[deprecated("Use StorageEnginePMem::AsyncDelete instead.")]] noodle::Result<
      void, CacheError>
  Delete(CacheBufferPtr buffer) override;

  folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>>
  AsyncDelete(CacheBufferPtr buffer) override;

  noodle::Result<void, CacheError> Reset() override;

  noodle::Result<void, CacheError> RecoverData(
      RecoverDataCallback* callback) override;

  struct PmemRecoverStats TEST_GetRecoverStats();

  // For unit test only!
  LogBasedMemoryAllocatorPMem* TEST_GetLogAllocator(int numa_id) {
    if (allocator_type_ != AllocatorType::kLogBasedAllocator) {
      return nullptr;
    }
    return dynamic_cast<LogBasedMemoryAllocatorPMem*>(
        dispatcher_->TEST_GetAllocator(numa_id));
  }

  PoolBasedMemoryAllocatorPMem* TEST_GetPoolAllocator(int numa_id) {
    if (allocator_type_ != AllocatorType::kPoolBasedAllocator) {
      return nullptr;
    }
    return dynamic_cast<PoolBasedMemoryAllocatorPMem*>(
        dispatcher_->TEST_GetAllocator(numa_id));
  }

 private:
  AllocatorType allocator_type_;

  std::unique_ptr<PMemDispatcher> dispatcher_;

  // The GCEventListener for log-based allocator. Useful only when log-based
  // pmem allocator is used.
  std::unique_ptr<LogBasedAllocatorGCEventListener> listener_;

  std::shared_ptr<noodle::MetricRegistry> metric_registry_;

  // Total time consumed to do PMEM cache recovery in milliseconds.
  std::shared_ptr<noodle::AtomicGauge> recover_time_counter_;
  // Number of valid records recovered during PMEM recovery process.
  std::shared_ptr<noodle::AtomicCounter> recover_records_counter_;
  // Total bytes scanned during PMEM recovery.
  std::shared_ptr<noodle::AtomicGauge> recover_total_bytes_;
  // Total bytes of valid records in PMEM recovery.
  std::shared_ptr<noodle::AtomicGauge> recover_valid_bytes_;
  // Total bytes of freed records in PMEM recovery.
  std::shared_ptr<noodle::AtomicGauge> recover_freed_bytes_;
  // Total bytes of corrupted records in PMEM recovery.
  std::shared_ptr<noodle::AtomicGauge> recover_corrupted_bytes_;

  void RegisterStorageEngineMetrics();
};

}  // namespace mtcache
