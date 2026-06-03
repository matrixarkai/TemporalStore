#pragma once

#include "allocator/log_allocator.h"
#include "cache_executor.h"

#include <noodle/metric/metric_registry.h>

#include <atomic>
#include <condition_variable>
#include <thread>

namespace mtcache {

class StorageGCController {
 public:
  // Constructor instantiate a StorageGCController instance
  // is_alaways_gc: if always perform gc
  // registry is the metric registry for GC controller to register
  // GC related metrics
  // If StorageGCController is created by a DRAM storage, then prefix is -1
  explicit StorageGCController(LogBasedMemoryAllocator* allocator,
                               bool force_gc,
                               std::shared_ptr<noodle::MetricRegistry> registry,
                               int numa_prefix = -1);

  // Destructor
  ~StorageGCController();

  // Enable the GC manager to monitor the statistics of the allocator and submit
  // the GC job
  void Start();

  // Disable the GC manager to monitor the statistics of the allocator and
  // submit the GC job
  void Stop();

  // Set if GC thread need to pause. Note that this function only stop
  // GCMonitor thread from creating new GC jobs. Existing GC jobs are not
  // affected.
  void SetPauseGC(bool pause);

  // Wait for all gc tasks to complete of this GCController.
  void WaitAllTaskComplete();

  int64_t TEST_GetNumGcCompleteChunks() {
    return complete_gc_chunks_counter_->GetValue();
  }

  int64_t TEST_GetNumGcCompleteTasks() {
    return complete_gc_tasks_counter_->GetValue();
  }

 private:
  // RegisterMetrics registers GC related metrics to the registry
  void RegisterMetrics(std::shared_ptr<noodle::MetricRegistry> registry,
                       int prefix);

  // Determine if we need to trigger a GC now
  bool NeedGc();

  // storage_gc_manager thread periodically check the alloc stats
  void GcMonitor();

  // Select and submit the chunks to perform GC
  void PickSubmitChunks();

  // Perform a GC job by a thread from the gc_workers_pool_
  void GcJob(std::vector<ChunkID> chunks);

  bool EnableGc() { return enable_gc_.load(std::memory_order_acquire); }

  void SetEnableGc(bool val);

 private:
  // this thread is used to mointor the allocator stats and submit gc tasks
  std::thread storage_gc_manager_;

  // Enable running storage_gc_manager_. This variable is only accessed by
  // `storage_gc_manager_` thread so it need not to be atomic.
  std::atomic<bool> enable_gc_;

  // Always scan chunks' statistics to pick up gc-able chunks. This variable
  // is only accessed by `storage_gc_manager_` thread so it need not to be
  // atomic.
  bool force_gc_;

  std::atomic<bool> pause_gc_;

  // The mutex for `gc_monitor_cv_`
  std::mutex gc_monitor_mtx_;

  // The condition variable that controls whether to exit gc monitor thread.
  // Without this cv, the `storage_gc_manager_` thread can also check the
  // status of `enable_gc_` every `FLAGS_gc_check_interval` and then decide
  // whether to exit. But in this way it will wait for at most
  // `FLAGS_gc_check_interval` after calling StorageGCController::Stop(). And
  // it will cost much time when running UT.
  std::condition_variable gc_monitor_cv_;

  // this thread pool is used to execute gc tasks from storage_gc_manager_
  ExecutorSharedPtr gc_worker_pool_;

  // bind the storeage engine's allocator
  LogBasedMemoryAllocator* allocator_;

  // gc_metric_collector_ is the registered callback function to collect metrics
  std::function<void(const AllocatorStats&)> gc_metric_collector_;

  // GC related metrics
  std::shared_ptr<noodle::AtomicGauge> gc_occupied_bytes_gauge_;
  std::shared_ptr<noodle::AtomicGauge> gc_allocated_bytes_gauge_;
  std::shared_ptr<noodle::AtomicGauge> gc_freed_bytes_gauge_;
  std::shared_ptr<noodle::AtomicGauge> gc_frag_permill_;
  std::shared_ptr<noodle::AtomicCounter> complete_gc_chunks_counter_;
  std::shared_ptr<noodle::AtomicCounter> fly_gc_chunks_counter_;
  std::shared_ptr<noodle::AtomicCounter> complete_gc_tasks_counter_;
};  // namespace StorageGCController

}  // namespace mtcache
