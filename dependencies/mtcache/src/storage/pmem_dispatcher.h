#pragma once

#include "async_writer.h"
#include "storage/gc_controller.h"
#include "storage/storage_engine.h"

namespace mtcache {

class PMemDispatcher {
 public:
  PMemDispatcher(AllocatorType alloc_type, uint64_t pmem_capacity,
                 const std::vector<std::string>& pmem_paths,
                 ExecutorSharedPtr cb_executor,
                 const std::vector<ExecutorSharedPtr>& pmem_executors,
                 LogBasedAllocatorGCEventListener* gc_listener,
                 std::shared_ptr<noodle::MetricRegistry> registry);

  ~PMemDispatcher();

  bool Start();
  bool Stop();

  // Push AsyncWriteTasks to the appropriate AsyncWriter according to its
  // pmem address.
  // 1. If this task is a `Put` task, the dispatcher will choose a less-busy
  //    per-NUMA AsyncWriter to do this task.
  // 2. If this task is a `GC` task, the dispatcher will choose the NUMA node
  //    which contains the pmem address to be gc-ed to do this task.
  // 3. If this task is a `Delete` task, the dispatcher will choose the NUMA
  //    node which contains the pmem address to be deleted to do this task.
  folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>> PushTask(
      AsyncWriteTask&& task);

  // Get the allocator by pmem addr. Return nullptr if no allocator contains
  // addr. If addr is nullptr, return a random allocator from allocators_.
  CacheAllocator* GetAllocator(const char* addr);

  // Get all allocators for all NUMA. This method should only be used during
  // PMEM recovery.
  const std::vector<std::unique_ptr<CacheAllocator>>& GetAllocators();

  // Get all gc controllers for all the allocators. This method should only
  // be used during PMEM recovery.
  const std::vector<std::unique_ptr<StorageGCController>>& GetGCControllers();

  // Get the allocator by numa id. For unit test only.
  CacheAllocator* TEST_GetAllocator(int32_t numa_id) const;

  // Join the PMEM writer executor(s) so that all writing-pmem tasks complete.
  // For test or benchmark only.
  void TEST_JoinPmemWriteExecutor();

 private:
  // Decide which NUMA the `addr` belongs to.
  int32_t GetNumaIdByPmemAddr(const char* addr) const;

  AllocatorType alloc_type_;

  // Per-NUMA CacheAllocator, corresponding to per-NUMA AsyncWriter.
  std::vector<std::unique_ptr<CacheAllocator>> allocators_;

  // The GC instance, use to mointor allocator's stats and trigger gc.
  // Useful only when log-based pmem allocator is used.
  std::vector<std::unique_ptr<StorageGCController>> gc_ctls_;

  // Per-NUMA AsyncWriter. Each AsyncWriter has a CacheAllocator.
  std::vector<std::unique_ptr<AsyncWriter>> writers_;

  // TODO(dbc) For now we push `Put` PmemTasks to the AsyncWriters one by one.
  // We should update this to distribute tasks according to each NUMA's free
  // PMEM space/traffic.
  std::atomic<uint8_t> current_numa_{0};

  bool stopped_{true};
};

}  // namespace mtcache
