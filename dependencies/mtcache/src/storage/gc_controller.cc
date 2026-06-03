#include "gc_controller.h"

#include "common/logging.h"

// when the remaining capacity is less than free_mem_min, we trigger GC
DECLARE_uint64(free_mem_min);
// when the fragmentation rate of the overall pool is larger than
// fragmentation_ratio_max, we trigger GC
DECLARE_int32(fragmentation_ratio_max);
// Interval GC_magnager re-checks the stats of the allocator
DECLARE_int32(gc_check_interval);
DECLARE_int32(num_gc_workers);

namespace mtcache {

StorageGCController::StorageGCController(
    LogBasedMemoryAllocator* allocator, bool force_gc,
    std::shared_ptr<noodle::MetricRegistry> registry, int numa_prefix) {
  CHECK(registry != nullptr);
  CHECK(allocator != nullptr);
  allocator_ = allocator;
  SetEnableGc(false);
  force_gc_ = force_gc;
  SetPauseGC(false);
  gc_worker_pool_ = CacheExecutor::GetGCExecutor();
  // We will start multiple GC instances for PMEM storage engine,
  // which will cause MetricId conflicts. Hence, we use the combination
  // of the GC instance ID and the metrics name as MetricId.
  RegisterMetrics(registry, numa_prefix);
}

void StorageGCController::RegisterMetrics(
    std::shared_ptr<noodle::MetricRegistry> registry, int prefix) {
  // If StorageGCController is created by a DRAM storage, then prefix is -1
  std::string cur_prefix = "";
  if (prefix >= 0) {
    cur_prefix += "numa";
    cur_prefix += std::to_string(prefix);
  }

  gc_occupied_bytes_gauge_ = registry->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId(cur_prefix + "_occupied_bytes"),
      std::make_shared<noodle::AtomicGauge>());
  gc_allocated_bytes_gauge_ = registry->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId(cur_prefix + "_allocated_bytes"),
      std::make_shared<noodle::AtomicGauge>());
  gc_freed_bytes_gauge_ = registry->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId(cur_prefix + "_freed_bytes"),
      std::make_shared<noodle::AtomicGauge>());
  gc_frag_permill_ = registry->MustRegister<noodle::AtomicGauge>(
      noodle::MetricId(cur_prefix + "_fragments_permill"),
      std::make_shared<noodle::AtomicGauge>());
  complete_gc_chunks_counter_ = registry->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId(cur_prefix + "_gc_complete_chunks_count"),
      std::make_shared<noodle::AtomicCounter>());
  fly_gc_chunks_counter_ = registry->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId(cur_prefix + "_gc_fly_chunks_count"),
      std::make_shared<noodle::AtomicCounter>());
  complete_gc_tasks_counter_ = registry->MustRegister<noodle::AtomicCounter>(
      noodle::MetricId(cur_prefix + "_gc_complete_tasks_count"),
      std::make_shared<noodle::AtomicCounter>());
}

StorageGCController::~StorageGCController() {}

void StorageGCController::Start() {
  SetEnableGc(true);
  // Create a monitoring thread for allocator statistics.
  // Note that `enable_gc_` must be set to true before creating this monitor
  // thread. Otherwise this thread will exit immediately.
  storage_gc_manager_ = std::thread([&]() { GcMonitor(); });
}

void StorageGCController::Stop() {
  CHECK(EnableGc());
  // no more gc job will be submitted
  SetEnableGc(false);
  // `enable_gc_` must be set to false before join this monitor
  // thread, Otherwise it will never stop.
  storage_gc_manager_.join();
  WaitAllTaskComplete();
}

void StorageGCController::SetEnableGc(bool val) {
  std::lock_guard<std::mutex> lk(gc_monitor_mtx_);
  enable_gc_.store(val, std::memory_order_release);
  if (!val) {
    gc_monitor_cv_.notify_all();
  }
}

void StorageGCController::SetPauseGC(bool pause) {
  pause_gc_.store(pause, std::memory_order_release);
}

void StorageGCController::WaitAllTaskComplete() {
  while (fly_gc_chunks_counter_->GetValue() > 0) {
    // busy wait
  }
}

bool StorageGCController::NeedGc() {
  if (pause_gc_.load(std::memory_order_acquire)) {
    // GC is paused for now. Maybe StorageEngine is doing PMEM recovery.
    return false;
  }

  if (force_gc_) {
    return true;
  }

  // Get the stats of log allocators to determine whether or not GC is needed
  // now.
  auto get_stats_res = allocator_->GetStats();
  auto get_capacity_res = allocator_->Capacity();
  if (get_stats_res.IsOk() && get_capacity_res.IsOk()) {
    auto stats = get_stats_res.Get();
    VLOG(4)
        << "StorageGCController::NeedGc collected metrics, num_occupied_bytes:"
        << stats.num_occupied_bytes
        << ", num_allocated_bytes:" << stats.num_allocated_bytes
        << ", num_freed_bytes:" << stats.num_freed_bytes;
    // Update the related metrics
    gc_occupied_bytes_gauge_->SetValue(stats.num_occupied_bytes);
    gc_allocated_bytes_gauge_->SetValue(stats.num_allocated_bytes);
    gc_freed_bytes_gauge_->SetValue(stats.num_freed_bytes);

    auto capacity = get_capacity_res.Get();
    // avaible memory capacity for new object allocations
    auto avaible_capacity = capacity - stats.num_occupied_bytes;
    if (avaible_capacity < FLAGS_free_mem_min) {
      return true;
    }
    // fragmentation_ratio = the number of bytes occupied by the deleted objects
    // / the total number of bytes occupied by valid objects and deleted objects
    if (stats.num_occupied_bytes == 0) return false;
    double cur_frag_ratio_ =
        (stats.num_occupied_bytes -
         (stats.num_allocated_bytes - stats.num_freed_bytes)) /
        static_cast<double>(stats.num_occupied_bytes);

    int64_t frag_permill = cur_frag_ratio_ * 1000;
    gc_frag_permill_->SetValue(frag_permill);
    if (cur_frag_ratio_ * 100 > FLAGS_fragmentation_ratio_max) {
      return true;
    }
  } else {
    LOG(ERROR) << "Get alloctor stats fail ";
  }
  return false;
}

void StorageGCController::PickSubmitChunks() {
  // clear previous gc chunkds id formation
  std::vector<ChunkID> id_vec;
  // step 1 -- pick chunks
  auto iter_meta_res =
      allocator_->IterateRecyclableChunkMeta([&id_vec](const ChunkMeta* meta) {
        if (meta->num_freed_bytes >
            kLogChunkSize * FLAGS_fragmentation_ratio_max / 100) {
          id_vec.emplace_back(meta->id);
          return true;
        } else {
          return false;
        }
      });
  if (!iter_meta_res.IsOk()) {
    // If step 1 fails, do not perform step 2.
    LOG(ERROR) << "IterateRecyclableChunkMeta() fail, error="
               << iter_meta_res.GetError()->GetMessage();
    return;
  }
  // No need to GC if there is only 1 garbage chunk
  if (id_vec.size() <= 1) return;
  // step 2 -- submit chunks to threadpool
  VLOG(4) << "PickSubmitChunks picked chunks number:" << id_vec.size()
          << ", allocator=" << allocator_;
  size_t sub_id_vec_size = id_vec.size() / FLAGS_num_gc_workers;
  // `left_chunks` has to be a signed integer, otherwise, `left_chunks--` may
  // cause underflow, and hence `len` in the following iterations is always one
  // more than the expected value, which results in `itr + len` beyond the valid
  // boundary.
  ssize_t left_chunks = id_vec.size() % FLAGS_num_gc_workers;
  auto itr = id_vec.cbegin();
  while (itr != id_vec.cend()) {
    size_t len = sub_id_vec_size + (left_chunks > 0 ? 1 : 0);
    if (len == 0) {
      // No chunks left for GC
      break;
    }
    GcJob(std::vector<ChunkID>(itr, itr + len));
    itr += len;
    left_chunks--;
  }
}

void StorageGCController::GcJob(std::vector<ChunkID> chunks) {
  fly_gc_chunks_counter_->Increase(chunks.size());
  auto f = [ this, chunk_ids = std::move(chunks) ] {
    // chunk_id_arr is valid until next gc, so chunk_id_arr's lifetime is longer
    // than gc jobs
    auto gc_res = allocator_->GC(chunk_ids.data(), chunk_ids.size());
    if (!gc_res.IsOk()) {
      LOG(ERROR) << "GC job execution failed, allocator=" << allocator_
                 << ", error: " << gc_res.GetError()->GetMessage();
    }
    complete_gc_chunks_counter_->Increase(chunk_ids.size());
    complete_gc_tasks_counter_->Increase(1);
    fly_gc_chunks_counter_->Decrease(chunk_ids.size());
  };
  gc_worker_pool_->add(f);
}

void StorageGCController::GcMonitor() {
  while (true) {
    if ((fly_gc_chunks_counter_->GetValue() == 0) && NeedGc()) {
      PickSubmitChunks();
    }
    // Break the `while` loop here rather than using `while (EnableGc())` at
    // the begining. Otherwise this thread will still sleep for
    // gc_check_interval ms before exit if the `nofity_all` signal is
    // sent during PickSubmitChunks().
    if (!EnableGc()) break;
    std::unique_lock<std::mutex> lk(gc_monitor_mtx_);
    if (gc_monitor_cv_.wait_for(
            lk, std::chrono::milliseconds(FLAGS_gc_check_interval),
            [this] { return !EnableGc(); })) {
      // break the while loop when this thread wakes up NOT due to timeout (
      // i.e. this thread wakes up because `notify_all` is sent or `EnableGc()`
      // returns false in spurious awakenings).
      break;
    }
  }
}

}  // namespace mtcache
