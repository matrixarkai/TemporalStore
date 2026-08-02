#include "l2_policy.h"

// flag to enable/disable async consume access record
DECLARE_bool(l2_policy_async_on_access);
// flag to store/drop the cache data from eviction handler
DECLARE_bool(l2_policy_use_eviction_handler);
DECLARE_int32(cache_latency_summary_time_window);
DECLARE_bool(cache_collect_latency_summary);

namespace mtcache {

L2CachePolicy::L2CachePolicy(
    L1CacheInterface* l1_cache, CacheInstance* l2_cache,
    std::shared_ptr<ReplacementArc> arc_policy, int access_interval_ms,
    int tail_interval_ms, int write_interval_ms, size_t access_buffer_capacity,
    size_t tail_batch_size, size_t write_buffer_capacity,
    std::shared_ptr<noodle::MetricRegistry> l2_registry)
    : l1_cache_(l1_cache),
      l2_cache_(l2_cache),
      arc_policy_(arc_policy),
      access_interval_ms_(access_interval_ms),
      tail_interval_ms_(tail_interval_ms),
      write_interval_ms_(write_interval_ms),
      tail_batch_size_(tail_batch_size),
      access_queue_(access_buffer_capacity),
      write_buffer_queue_(write_buffer_capacity),
      l2_registry_(std::move(l2_registry)) {
  CHECK_NOTNULL(l1_cache_);
  CHECK_NOTNULL(l2_cache_);
  CHECK_NOTNULL(arc_policy_);

  CHECK_NOTNULL(l2_registry_);

  access_callback_counter_ = l2_registry_->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId("l2.access.call"),
      std::make_shared<noodle::AtomicCounter>());
  pull_success_counter_ = l2_registry_->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId("l2.pull.success"),
      std::make_shared<noodle::AtomicCounter>());
  pull_fail_counter_ = l2_registry_->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId("l2.pull.fail"),
      std::make_shared<noodle::AtomicCounter>());
  write_exist_counter_ = l2_registry_->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId("l2.write.exist"),
      std::make_shared<noodle::AtomicCounter>());
  write_success_counter_ = l2_registry_->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId("l2.write.success"),
      std::make_shared<noodle::AtomicCounter>());
  write_fail_counter_ = l2_registry_->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId("l2.write.fail"),
      std::make_shared<noodle::AtomicCounter>());
  write_enqueue_fail_counter_ =
      l2_registry_->MustRegister<noodle::AtomicCounter>(
          noodle::MetricId("l2.write.enqueue_fail"),
          std::make_shared<noodle::AtomicCounter>());

  access_buffer_size_gauge_ = l2_registry_->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId("l2.access.buffer_size"),
      std::make_shared<noodle::AtomicGauge>());
  write_buffer_size_gauge_ = l2_registry_->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId("l2.write.buffer_size"),
      std::make_shared<noodle::AtomicGauge>());

  write_latency_summary_ =
      l2_registry_->MustRegister<noodle::SampleSetTimeSummary>(
          noodle::MetricId("write.latency"),
          std::make_shared<noodle::SampleSetTimeSummary>(
              noodle::TimeUnit::kMs,
              std::make_shared<noodle::TimeWindowSampleSet>(
                  noodle::Time::FromSec(
                      FLAGS_cache_latency_summary_time_window))));
}

void L2CachePolicy::Start() {
  CHECK(write_thread_ == nullptr);
  CHECK(tail_thread_ == nullptr);

  stopped_ = false;

  access_thread_.reset(new std::thread([&]() { AccessLoop(); }));
  write_thread_.reset(new std::thread([&]() { WriteLoop(); }));
  tail_thread_.reset(new std::thread([&]() { TailLoop(); }));
}

void L2CachePolicy::Stop() {
  if (stopped_) {
    return;
  }
  stopped_ = true;
  access_cv_.notify_all();
  tail_cv_.notify_all();
  write_cv_.notify_all();

  if (access_thread_) {
    access_thread_->join();
    access_thread_.reset();
  }

  if (write_thread_) {
    write_thread_->join();
    write_thread_.reset();
  }

  if (tail_thread_) {
    tail_thread_->join();
    tail_thread_.reset();
  }

  LOG(INFO) << "L2CachePolicy Stopped";
}

void L2CachePolicy::OnAccess(AccessRecordType type, const std::string& key) {
  VLOG(10) << "OnAccess "
           << " type=" << static_cast<int>(type) << " key=" << key;
  if (FLAGS_l2_policy_async_on_access) {
    // Ignore the return value. Drop the access record if the queue is full.
    access_queue_.write(std::make_pair(type, key));
    access_cv_.notify_one();
  } else {
    DoAccess(type, key);
  }
}

void L2CachePolicy::OnEvict(CacheBufferSharedPtr cache_buffer) {
  if (FLAGS_l2_policy_use_eviction_handler) {
    PutQueue(cache_buffer);
  }
}

void L2CachePolicy::DoAccess(AccessRecordType type, const std::string& key) {
  switch (type) {
    case AccessRecordType::kPut:
      VLOG(10) << "Process Access Put key=" << key;
      arc_policy_->Put(key);
      break;
    case AccessRecordType::kGet:
      VLOG(10) << "Process Access Get key=" << key;
      arc_policy_->Get(key);
      break;
    case AccessRecordType::kDelete:
      VLOG(10) << "Process Access Delete key=" << key;
      arc_policy_->Delete(key);
      break;
    default:
      LOG(ERROR) << "Unknow Access type";
  }
  access_callback_counter_->Increase();
}

void L2CachePolicy::PutQueue(CacheBufferSharedPtr cache_buffer) {
  auto ret = write_buffer_queue_.write(std::move(cache_buffer));
  if (!ret) {
    write_enqueue_fail_counter_->Increase();
    remove_l2_policy_func_(cache_buffer);
  }
}

CacheBufferSharedPtr L2CachePolicy::FetchQueue() {
  CacheBufferSharedPtr cache_buffer;

  // read nonblocking
  if (write_buffer_queue_.read(cache_buffer)) {
    return cache_buffer;
  } else {
    return nullptr;
  }
}

void L2CachePolicy::AccessLoop() {
  LOG(INFO) << "Start access consumer thread.";

  std::unique_lock<std::mutex> lock(access_mutex_);
  while (!stopped_) {
    if (!paused_) {
      AccessTaskInternal();
    }

    if (stopped_) break;
    access_cv_.wait_for(lock, std::chrono::milliseconds(access_interval_ms_));
  }

  LOG(INFO) << "Stop access consumer thread.";
};

void L2CachePolicy::TailLoop() {
  LOG(INFO) << "Start L1 Cache Tail thread.";

  std::unique_lock<std::mutex> lock(tail_mutex_);
  while (!stopped_) {
    if (!paused_) {
      TailTaskInternal();
    }

    if (stopped_) break;
    tail_cv_.wait_for(lock, std::chrono::milliseconds(tail_interval_ms_));
  }

  LOG(INFO) << "Stop L1 Cache Tail thread.";
}

void L2CachePolicy::WriteLoop() {
  LOG(INFO) << "Start L2 Cache Write thread.";

  std::unique_lock<std::mutex> lock(write_mutex_);
  while (!stopped_) {
    if (!paused_) {
      WriteTaskInternal();
    }

    if (stopped_) break;
    write_cv_.wait_for(lock, std::chrono::milliseconds(write_interval_ms_));
  }

  LOG(INFO) << "Stop L2 Cache Write thread.";
}

void L2CachePolicy::AccessTaskInternal() {
  while (!stopped_) {
    AccessBuffer buffer;
    // Read the record until the queue is empty
    if (access_queue_.read(buffer)) {
      auto type = buffer.first;
      auto key = buffer.second;

      VLOG(10) << "Async process Access type=" << static_cast<int>(type)
               << " key=" << key;
      DoAccess(type, key);
    } else {
      break;
    }

    access_buffer_size_gauge_->SetValue(access_queue_.size());
  }
}

void L2CachePolicy::TailTaskInternal() {
  // TODO(xiongmu): warm-up scenario
  std::vector<std::string> tail;
  // according to ZFS L2ARC, get tail data from active/fetch lists one by one
  if (tail_data_from_fetch_list_) {
    VLOG(10) << "Tail from fetch list";
    tail = arc_policy_->GetFetchTail(tail_batch_size_);
  } else {
    VLOG(10) << "Tail from active list";
    tail = arc_policy_->GetActiveTail(tail_batch_size_);
  }
  tail_data_from_fetch_list_ = !tail_data_from_fetch_list_;

  // try get cache data
  for (auto& cache_key : tail) {
    VLOG(10) << "tail cache_key=" << cache_key;
    auto cache_buffer = l1_cache_->GetBypassReplacementPolicy(cache_key);
    if (cache_buffer) {
      VLOG(10) << "Put Cache to write waiting list, cache_key=" << cache_key;
      PutQueue(cache_buffer);
      pull_success_counter_->Increase();
    } else {
      VLOG(10) << "GetBypassReplacementPolicy Failed, cache_key=" << cache_key;
      pull_fail_counter_->Increase();
    }
  }
}

void L2CachePolicy::DoOneWrite(CacheBufferSharedPtr buffer) {
  auto key = buffer->Key();
  auto value = folly::IOBuf::wrapBufferAsValue(buffer->Data(), buffer->Size());

  VLOG(10) << "Write to L2 Cache, key=" << key
           << " value_size=" << buffer->Size();
  if (!l2_cache_->Peek(key)) {
    auto res = l2_cache_->Put(key, std::move(value));
    if (res.IsOk()) {
      VLOG(10) << "Write to L2 Cache success, key=" << key;
      write_success_counter_->Increase();
    } else {
      LOG(ERROR) << "error msg: " << res.GetError()->GetMessage();
      write_fail_counter_->Increase();
    }
  } else {
    VLOG(10) << "Cache key already exist in L2 Cache, key=" << key;
    write_exist_counter_->Increase();
  }
}

void L2CachePolicy::WriteTaskInternal() {
  auto buffer = FetchQueue();
  while (buffer != nullptr && !stopped_) {
    if (FLAGS_cache_collect_latency_summary) {
      auto timer = write_latency_summary_->NewTimer();
      DoOneWrite(std::move(buffer));
    } else {
      DoOneWrite(std::move(buffer));
    }

    buffer = FetchQueue();
    write_buffer_size_gauge_->SetValue(write_buffer_queue_.size());
  }
}

void L2CachePolicy::TEST_Pause() {
  DLOG(INFO) << "TEST_Pause";
  paused_ = true;
}

void L2CachePolicy::TEST_Continue() {
  LOG(INFO) << "TEST_Continue";
  paused_ = false;
}

void L2CachePolicy::TEST_WaitAllTaskSleep() {
  // locking successful means that the task is sleeping
  std::unique_lock<std::mutex> lock1(access_mutex_);
  LOG(INFO) << "Access task sleep";
  std::unique_lock<std::mutex> lock2(tail_mutex_);
  LOG(INFO) << "Tail task sleep";
  std::unique_lock<std::mutex> lock3(write_mutex_);
  LOG(INFO) << "Write task sleep";
}

};  // namespace mtcache
