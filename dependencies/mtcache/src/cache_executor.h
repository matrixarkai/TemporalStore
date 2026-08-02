#pragma once

#include <folly/Executor.h>

#include <mutex>
#include <vector>

namespace mtcache {

using ExecutorSharedPtr = std::shared_ptr<folly::Executor>;

// CacheExecutor is a tool class that manages all thread pools used by cache.
// Others classes in cache module should not create thread pools arbitrarily.
class CacheExecutor {
 public:
  // Common executor is a general-purpose executor. It has
  // FLAGS_common_executor_num_threads threads and not binds to any NUMA.
  //
  // Components that using CommonExecutor:
  //  * LoadingCached insert data from hdfs into cache.
  //  * Callback function that is called after async_writing PMEM
  //  * Writer and callbacker of DramAsyncPromotion that promote data from PMEM
  //    to DRAM
  static const ExecutorSharedPtr& GetCommonExecutor();

  // GC executor is used to do GC jobs of LogAllocator. It has
  // FLAGS_num_gc_workers threads. All LogAllocators shared one GCExecutor.
  static const ExecutorSharedPtr& GetGCExecutor();

  // PmemExecutors is a vector of executors where each executor binds to one
  // NUMA. The vector contains FLAGS_used_num_numa_nodes executors and
  // each executor has FLAGS_num_pmem_cache_per_numa_writer_threads threads.
  // e.g. If there are 2 NUMAs, then threads in GetPmemExecutors()[0] all run
  // at NUMA 0 and threads in GetPmemExecutors()[1] all run at NUMA1.
  //
  // PMEMDispachers use PMEMExecutors to run write_funcs that write data to
  // PMEM.
  static const std::vector<ExecutorSharedPtr>& GetPmemExecutors();

  // Stop and destroy all executors. This function must be called before exiting
  // program because the threads in the executors usually has a thread-local
  // IDIniter which will touch the static variable at destructor. If these
  // static executors are not destroyed before the static IDIniters, it will
  // cause invalid memory access when the executors are destroyed.
  static void DestroyAllExecutors();

 private:
  static ExecutorSharedPtr common_executor_;
  static ExecutorSharedPtr gc_executor_;
  static std::vector<ExecutorSharedPtr> pmem_executors_;
  static std::mutex mtx_;
};

}  // namespace mtcache
