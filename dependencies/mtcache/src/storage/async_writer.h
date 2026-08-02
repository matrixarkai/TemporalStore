#pragma once

#include "allocator/allocator.h"
#include "buffer/cache_buffer.h"
#include "cache_executor.h"

#include <folly/futures/Future.h>

#include <functional>

namespace mtcache {

// This class defines the structure of an async_write task.
//
// For PMEM, there are 3 kinds of tasks:
//   1. Put task: put data to PMEM
//   2. Delete task: write tombstone bit of a pmem cache record
//   3. GC task: log-based allocator copies data to new location in gc
// For DRAM, there is 1 kind of task:
//   1. Put task: put data to DRAM in async-promotion
//
// A AsyncWriteTask consists of:
//   1. write_func_: a write function which writes data to DRAM or PMEM.
//   2. cb_: a callback function which handles the results from write_func_.
//           For Put task, the callback returns a CacheBufferSharedPtr of the
//           inserted cache buffer. For Delete task, the callback returns an
//           empty shared_ptr. For GC task, the callback returns the
//           CacheBufferSharedPtr to the new buffer after GC.
//   3. addr_: the source/destination address of the write_func_ (see
//      comments for `addr_` for detail).
struct AsyncWriteTask {
  using WriteResult = noodle::Result<char*, CacheError>;
  using WriteFunc = std::function<WriteResult(CacheAllocator*)>;
  using CallbackFunc =
      std::function<noodle::Result<CacheBufferSharedPtr, CacheError>(
          WriteResult)>;

  // Write function that does the writing to PMEM.
  WriteFunc write_func_;

  // Callback function to handle the results from write_func_.
  CallbackFunc cb_;

  // The source/destination address of the write_func_.
  //
  // 1. PMEM write task:
  //   For `Put` tasks, the value should be nullptr, which means the dispatcher
  //   will choose the approriate NUMA node to run this job.
  //   For `GC` tasks, the value should be the pmem address to be gc-ed.
  //   For `Delete` tasks, the value should be the pmem address to be deleted.
  //
  // 2. DRAM write task:
  //   For `Put` tasks, the value should be nullptr
  const char* addr_;

  AsyncWriteTask(WriteFunc&& func, CallbackFunc&& cb)
      : write_func_(func), cb_(cb), addr_(nullptr) {}

  AsyncWriteTask(WriteFunc&& func, CallbackFunc&& cb, const char* addr)
      : write_func_(func), cb_(cb), addr_(addr) {}
};

// This class is a AsyncWriter for PMEM on a SINGLE NUMA node. Each NUMA node
// has its own AsyncWriter. A Global NUAM-aware dispatcher will choose the
// appropriate AsyncWriter to run the AsyncWriteTask.
class AsyncWriter {
 public:
  AsyncWriter(CacheAllocator* alloc, ExecutorSharedPtr writer,
              ExecutorSharedPtr callbacker);

  // Push a AsyncWriteTask to this AsyncWriter. Return a SemiFuture of this
  // task's callback function's return value. Usually the caller should ignore
  // the returned SemiFuture, except he wants to block until the async task is
  // done by the worker thread (e.g. UT may want to wait in this way).
  folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>>
  AsyncWrite(AsyncWriteTask task);

  // Stop the AsyncWriter. Note that this function will wait for all the
  // on-the-fly tasks to finish before exiting.
  void Stop();

  // Join the PMEM writer executor(s) so that all writing-pmem tasks complete.
  // For test or benchmark only.
  void TEST_JoinWriteExecutor();

 private:
  // The callback executor to do the callbacks of the PmemTasks. The Dispather
  // owns this callback executor.
  ExecutorSharedPtr callback_executor_;

  // The per-NUMA thread pool to run PmemTasks. The thread pool has
  // FLAGS_num_pmem_cache_per_numa_writer_threads threads in it. The value
  // can not be too large otherwise the PMEM write throughput may drop.
  ExecutorSharedPtr writer_executor_;

  // The CacheAllocator this AsyncWiter should use. Each NUMA node has a
  // CacheAllocator for the PMEM device on it. `PMemDispatcher` is responsible
  // for initializing and managing all the CacheAllocators.
  CacheAllocator* allocator_;

  // TODO(dbc) move the atomic counters to DramAsyncWriter and PmemAsyncWriter
  // and make them metric counters. We can monitor the workload of
  // writer_executor and callback_executor via these counters.
  //
  // The number of write tasks to be handled.
  std::atomic<uint64_t> fly_write_num_{0};
  // The number of callback tasks to be handled.
  std::atomic<uint64_t> fly_cb_num_{0};
};

}  // namespace mtcache
