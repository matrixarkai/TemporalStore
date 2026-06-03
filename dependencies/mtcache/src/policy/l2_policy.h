#pragma once

#include "access_record_callback.h"
#include "arc.h"
#include "cache_instance.h"
#include "l1_interface.h"

#include <folly/MPMCQueue.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace mtcache {

// This class contains logic related to cache move from L1(DRAM/PMEM) to L2(SSD)
// This class hold a buffer/window for managing the cache in the move
// This class is responsible for throttling L2 cache write. Avoid the influence
// of writing on device reading
class L2CachePolicy : public AccessRecordCallback {
 public:
  L2CachePolicy(L1CacheInterface* l1_cache, CacheInstance* l2_cache,
                std::shared_ptr<ReplacementArc> arc_policy,
                int access_interval_ms, int tail_interval_ms,
                int write_interval_ms, size_t access_buffer_capacity,
                size_t tail_batch_size, size_t write_buffer_capacity,
                std::shared_ptr<noodle::MetricRegistry> l2_registry);
  ~L2CachePolicy() { Stop(); }

  // Start/Stop background working thread
  void Start();
  void Stop();

  // Implement AccessRecordCallback
  void OnAccess(AccessRecordType type, const std::string& key) override;

  // Callback for EvictionHandler
  void OnEvict(CacheBufferSharedPtr cache_buffer);

  void RegisterRemoveL2PolicyHandler(
      std::function<void(CacheBufferSharedPtr)> func) {
    remove_l2_policy_func_ = func;
  }

  // Pause and continue the cache migration process.
  // These two method should ONLY used be used by unit tests.
  void TEST_Pause();
  void TEST_Continue();

  // wait all task sleep
  void TEST_WaitAllTaskSleep();

 private:
  // Consume AccessRecord
  void DoAccess(AccessRecordType type, const std::string& key);

  // Put a cache buffer to l2 cache write waiting list
  void PutQueue(CacheBufferSharedPtr cache_buffer);

  // Fetch a cache buffer from l2 cache write waiting list
  CacheBufferSharedPtr FetchQueue();

  // Write Data to L2 Cache
  void DoOneWrite(CacheBufferSharedPtr buffer);

  // consume access record
  void AccessLoop();

  // Write thread needs to execute this function.
  // Fetch the tail cache buffer from l1 cache, and put it to l2 cache write
  // waiting list.
  void TailLoop();

  // Write thread needs to execute this function.
  // Fetch cache buffer from l2 cache write waiting list, and write it to l2
  // cache. The goal to achieve reasonable write throughput while minimizing the
  // impact on reads
  void WriteLoop();

  // After excluding the control logic, the real ACCESS processing logic
  void AccessTaskInternal();

  // After excluding the control logic, the real TAIL processing logic
  void TailTaskInternal();

  // After excluding the control logic, the real WRITE processing logic
  void WriteTaskInternal();

  L1CacheInterface* l1_cache_;
  CacheInstance* l2_cache_;
  std::shared_ptr<ReplacementArc> arc_policy_;

  int access_interval_ms_{1};
  int tail_interval_ms_{1};
  int write_interval_ms_{1};

  size_t tail_batch_size_;

  using AccessBuffer = std::pair<AccessRecordType, std::string>;
  folly::MPMCQueue<AccessBuffer> access_queue_;
  folly::MPMCQueue<CacheBufferSharedPtr> write_buffer_queue_;

  std::unique_ptr<std::thread> access_thread_;
  std::unique_ptr<std::thread> tail_thread_;
  std::unique_ptr<std::thread> write_thread_;

  // mutex and cv for thread control
  std::mutex access_mutex_;
  std::condition_variable access_cv_;

  std::mutex tail_mutex_;
  std::condition_variable tail_cv_;

  std::mutex write_mutex_;
  std::condition_variable write_cv_;

  // Control Status of all tasks.
  std::atomic<bool> stopped_{false};
  std::atomic<bool> paused_{false};

  // Status information about background task
  bool tail_data_from_fetch_list_{true};

  // This function is called when enqueue to the write buffer failed. During
  // eviction of L1, we insert the cache entry to the L2 before write queue
  // actually performs the eviction to avoid cache miss.
  // However, in the case we failed to enqueue the write queue, we need to
  // remove the previously inserted cache entry in the L2 to guarantee the 
  // correctness.
  std::function<void(CacheBufferSharedPtr)> remove_l2_policy_func_;

  // l2_registry_ is the metric registry where the l2cache registers metrics
  std::shared_ptr<noodle::MetricRegistry> l2_registry_;

  std::shared_ptr<noodle::AtomicCounter> access_callback_counter_;
  std::shared_ptr<noodle::AtomicCounter> pull_success_counter_;
  std::shared_ptr<noodle::AtomicCounter> pull_fail_counter_;
  std::shared_ptr<noodle::AtomicCounter> write_exist_counter_;
  std::shared_ptr<noodle::AtomicCounter> write_success_counter_;
  std::shared_ptr<noodle::AtomicCounter> write_fail_counter_;
  std::shared_ptr<noodle::AtomicCounter> write_enqueue_fail_counter_;

  std::shared_ptr<noodle::AtomicGauge> access_buffer_size_gauge_;
  std::shared_ptr<noodle::AtomicGauge> write_buffer_size_gauge_;

  std::shared_ptr<noodle::SampleSetTimeSummary> write_latency_summary_;
};

}  // namespace mtcache
