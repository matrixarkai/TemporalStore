#include "pmem_dispatcher.h"

#include "allocator/pmem_allocators.h"
#include "common/logging.h"
#include "common/numa_utils.h"

#include <gflags/gflags.h>

DECLARE_uint64(pool_based_allocator_obj_len);
DECLARE_uint64(cache_pmem_gc_reserved);
DECLARE_int32(cache_pmem_max_thread_num);
DECLARE_string(cache_pmem_flush_policy);
DECLARE_uint64(cache_pmem_mini_batch_flush_bytes);
DECLARE_bool(cache_force_gc);
DECLARE_int32(used_num_numa_nodes);

namespace mtcache {

static FlushPolicy ParseFlushPolicy(const std::string& policy) {
  if (policy == "NoFlush") {
    return FlushPolicy::kNoFlush;
  } else if (policy == "InstantFlush") {
    return FlushPolicy::kInstantFlush;
  } else if (policy == "MiniBatchFlush") {
    return FlushPolicy::kMiniBatchFlush;
  } else {
    LOG(FATAL) << "PMEM flush policy is not valid: policy=" << policy;
  }
  return FlushPolicy::kNoFlush;
}

PMemDispatcher::PMemDispatcher(
    AllocatorType alloc_type, uint64_t pmem_capacity,
    const std::vector<std::string>& pmem_paths, ExecutorSharedPtr cb_executor,
    const std::vector<ExecutorSharedPtr>& pmem_executors,
    LogBasedAllocatorGCEventListener* gc_listener,
    std::shared_ptr<noodle::MetricRegistry> registry) {
  // TODO(dongbenchao) alloc_type must be LogBasedAllocator because
  // PoolBasedAllocator does not support numa-aware feature now.
  CHECK(alloc_type == AllocatorType::kLogBasedAllocator);

  CHECK_EQ(FLAGS_used_num_numa_nodes, pmem_paths.size());
  CHECK_EQ(FLAGS_used_num_numa_nodes, pmem_executors.size());

  auto flush_policy = ParseFlushPolicy(FLAGS_cache_pmem_flush_policy);
  uint64_t capacity_per_numa = pmem_capacity / FLAGS_used_num_numa_nodes;

  alloc_type_ = alloc_type;
  if (alloc_type == AllocatorType::kLogBasedAllocator) {
    DCHECK(gc_listener != nullptr);
    DCHECK(registry != nullptr);
    // LogBaseAllocator should have at least 2 chunks because the AsyncWriter
    // needs at least one extra chunk.
    if (capacity_per_numa <= kLogChunkSize) {
      capacity_per_numa += kLogChunkSize;
    }

    for (int32_t numa_id = 0; numa_id < FLAGS_used_num_numa_nodes; ++numa_id) {
      auto log_allocator = std::make_unique<LogBasedMemoryAllocatorPMem>(
          pmem_paths[numa_id], flush_policy,
          FLAGS_cache_pmem_mini_batch_flush_bytes, gc_listener,
          capacity_per_numa, FLAGS_cache_pmem_gc_reserved,
          FLAGS_cache_pmem_max_thread_num, registry, numa_id);
      // create a cache gc controller and bind the allocator
      auto gc_ctl = std::make_unique<StorageGCController>(
          log_allocator.get(), FLAGS_cache_force_gc, registry, numa_id);
      gc_ctls_.push_back(std::move(gc_ctl));
      auto writer = std::make_unique<AsyncWriter>(
          log_allocator.get(), pmem_executors[numa_id], cb_executor);
      writers_.push_back(std::move(writer));
      allocators_.push_back(std::move(log_allocator));
    }
  } else {
    // TODO(dbc) not ready for Pool-based allocator
    // DCHECK(gc_listener == nullptr);
    // DCHECK(gc_registry == nullptr);
    //
    // for (int32_t numa_id = 0; numa_id < FLAGS_used_num_numa_nodes; ++numa_id)
    // {
    //   auto pool_allocator = std::make_unique<PoolBasedMemoryAllocatorPMem>(
    //       pmem_paths[numa_id], flush_policy, capacity_per_numa,
    //       FLAGS_cache_pmem_max_thread_num,
    //       FLAGS_pool_based_allocator_obj_len);
    //   allocators_.push_back(std::move(pool_allocator));
    // }
  }
}

PMemDispatcher::~PMemDispatcher() {
  DCHECK(stopped_);
  gc_ctls_.clear();
  writers_.clear();
  allocators_.clear();
}

bool PMemDispatcher::Start() {
  // gc_ctls_ is not empty only when AllocatorType is kLogBasedAllocator.
  for (const auto& gc_ctl : gc_ctls_) {
    gc_ctl->Start();
  }
  stopped_ = false;
  return true;
}

bool PMemDispatcher::Stop() {
  stopped_ = true;
  for (const auto& gc_ctl : gc_ctls_) {
    gc_ctl->Stop();
  }
  // AsyncWriter must be stopped after GCController because GCController
  // may push tasks to the writers_.
  for (auto& writer : writers_) {
    writer->Stop();
  }
  return true;
}

void PMemDispatcher::TEST_JoinPmemWriteExecutor() {
  for (auto& writer : writers_) {
    writer->TEST_JoinWriteExecutor();
  }
}

folly::SemiFuture<noodle::Result<CacheBufferSharedPtr, CacheError>>
PMemDispatcher::PushTask(AsyncWriteTask&& task) {
  DCHECK(!stopped_);
  if (task.addr_ == nullptr) {
    uint8_t numa_id = current_numa_.fetch_add(1, std::memory_order_relaxed);
    // This task is a normal write task which does not care which NUMA the data
    // will be placed. So we can dispatch this task according to the runtime
    // load of differenct NUMA nodes.
    numa_id %= static_cast<uint8_t>(writers_.size());
    return writers_[numa_id]->AsyncWrite(task);
  } else {
    // This task will write to `task.addr_` (GC task or Delete task). So
    // we must assign it to the NUMA node where `task.addr_` belongs to.
    //
    // TODO(dbc) Pool based allocator does not support numa for now.
    DCHECK(alloc_type_ != AllocatorType::kPoolBasedAllocator);
    int32_t numa_id = GetNumaIdByPmemAddr(task.addr_);
    DCHECK_GE(numa_id, 0)
        << "Invalid PMEM address in PMemDispatcher::PushTask, addr="
        << reinterpret_cast<const void*>(task.addr_);
    return writers_[numa_id]->AsyncWrite(task);
  }
}

int32_t PMemDispatcher::GetNumaIdByPmemAddr(const char* addr) const {
  for (int32_t i = 0; i < allocators_.size(); ++i) {
    if (allocators_[i]->Contains(addr)) {
      return i;
    }
  }
  return -1;
}

CacheAllocator* PMemDispatcher::GetAllocator(const char* addr) {
  if (addr == nullptr) {
    uint8_t numa_id = current_numa_.fetch_add(1, std::memory_order_relaxed);
    numa_id %= static_cast<uint8_t>(allocators_.size());
    return allocators_[numa_id].get();
  }
  int32_t numa = GetNumaIdByPmemAddr(addr);
  if (numa < 0) {
    return nullptr;
  }
  return allocators_[numa].get();
}

const std::vector<std::unique_ptr<CacheAllocator>>&
PMemDispatcher::GetAllocators() {
  return allocators_;
}

const std::vector<std::unique_ptr<StorageGCController>>&
PMemDispatcher::GetGCControllers() {
  return gc_ctls_;
}

CacheAllocator* PMemDispatcher::TEST_GetAllocator(int32_t numa_id) const {
  return allocators_[numa_id].get();
}

}  // namespace mtcache
