#include "async_writer.h"

#include "common/logging.h"
#include "common/thread_pool/cpu_numa_thread_pool_executor.h"

#include <folly/futures/Future.h>

namespace mtcache {

AsyncWriter::AsyncWriter(CacheAllocator* alloc, ExecutorSharedPtr writer,
                         ExecutorSharedPtr callbacker)
    : allocator_(alloc) {
  DCHECK(alloc != nullptr);
  writer_executor_ = std::move(writer);
  callback_executor_ = std::move(callbacker);
}

folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>>
AsyncWriter::AsyncWrite(AsyncWriteTask task) {
  DCHECK(writer_executor_ != nullptr);
  DCHECK(callback_executor_ != nullptr);
  fly_write_num_.fetch_add(1, std::memory_order_acquire);
  return folly::via(writer_executor_.get(),
                    [this, write = std::move(task.write_func_)]() {
                      auto res = write(allocator_);
                      fly_cb_num_.fetch_add(1, std::memory_order_acquire);
                      fly_write_num_.fetch_sub(1, std::memory_order_release);
                      return res;
                    })
      .via(callback_executor_.get())
      .thenValue(
          [this, cb = std::move(task.cb_)](AsyncWriteTask::WriteResult arg) {
            auto res = cb(std::move(arg));
            fly_cb_num_.fetch_sub(1, std::memory_order_release);
            return res;
          });
}

void AsyncWriter::Stop() {
  // Wait till all the flying tasks of this AsyncWriter to be finished.
  // We cannot stop the writer_executor_ or callback_executor_ here because
  // other AsycnWriter instances may still be using the executor.
  while ((fly_write_num_.load(std::memory_order_relaxed) != 0) ||
         (fly_cb_num_.load(std::memory_order_relaxed) != 0)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

void AsyncWriter::TEST_JoinWriteExecutor() {
  dynamic_cast<CPUNumaThreadPoolExecutor*>(writer_executor_.get())->join();
}

}  // namespace mtcache
