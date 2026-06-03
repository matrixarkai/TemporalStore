#include "cache_executor.h"

#include "common/numa_utils.h"
#include "common/thread_pool/cpu_numa_thread_pool_executor.h"

#include <gflags/gflags.h>

DECLARE_int32(common_executor_num_threads);
DECLARE_int32(num_pmem_cache_per_numa_writer_threads);
DECLARE_int32(num_gc_workers);
DECLARE_int32(used_num_numa_nodes);

namespace mtcache {

ExecutorSharedPtr CacheExecutor::common_executor_;
ExecutorSharedPtr CacheExecutor::gc_executor_;
std::vector<ExecutorSharedPtr> CacheExecutor::pmem_executors_;
std::mutex CacheExecutor::mtx_;

const ExecutorSharedPtr& CacheExecutor::GetCommonExecutor() {
  // It's fine to use a coarse-grained lock here because this function will
  // not be called frequently.
  std::lock_guard<std::mutex> lk(mtx_);
  if (common_executor_ == nullptr) {
    common_executor_ = std::static_pointer_cast<folly::Executor>(
        std::make_shared<CPUNumaThreadPoolExecutor>(
            FLAGS_common_executor_num_threads, true,
            std::make_shared<folly::NamedThreadFactory>(
                "CacheCommonThreadPool")));
  }
  return common_executor_;
}

const ExecutorSharedPtr& CacheExecutor::GetGCExecutor() {
  // It's fine to use a coarse-grained lock here because this function will
  // not be called frequently.
  std::lock_guard<std::mutex> lk(mtx_);
  if (gc_executor_ == nullptr) {
    gc_executor_ = std::static_pointer_cast<folly::Executor>(
        std::make_shared<CPUNumaThreadPoolExecutor>(
            FLAGS_num_gc_workers, true,
            std::make_shared<folly::NamedThreadFactory>("CacheGCThreadPool")));
  }
  return gc_executor_;
}

const std::vector<ExecutorSharedPtr>& CacheExecutor::GetPmemExecutors() {
  // It's fine to use a coarse-grained lock here because this function will
  // not be called frequently.
  std::lock_guard<std::mutex> lk(mtx_);
  if (pmem_executors_.size() == 0) {
    NumaInfo::Init();

    CHECK_GE(NumaInfo::GetMaxNumNumaNodes(), FLAGS_used_num_numa_nodes)
        << "Used num numa nodes can not be more than existing numa nodes";
    CHECK_LE(FLAGS_used_num_numa_nodes, std::numeric_limits<uint8_t>::max())
        << "Used numa nodes can not exceed UINT8_MAX";

    pmem_executors_.resize(FLAGS_used_num_numa_nodes,
                           ExecutorSharedPtr(nullptr));
    for (int32_t numa_id = 0; numa_id < FLAGS_used_num_numa_nodes; ++numa_id) {
      // Init the writer thread pool for `numa_id`.
      auto worker = std::make_shared<CPUNumaThreadPoolExecutor>(
          FLAGS_num_pmem_cache_per_numa_writer_threads,
          false,  // disable dynamicNumThreads as it may affect CPU affinity
          std::make_shared<folly::NamedThreadFactory>("PmemNuma" +
                                                      std::to_string(numa_id)));

      const auto& cpu_ids = NumaInfo::GetCpuCoresOfNumaNode(numa_id);
      CHECK(cpu_ids.size() > 0)
          << "Can not set NUMA affinity with empty cpu_ids, numa_id="
          << numa_id;
      std::stringstream cpu_numa_ss;
      for (const auto n : cpu_ids) {
        cpu_numa_ss << n << ", ";
      }
      LOG(INFO) << "Binding PmemAsyncWriteExecutor of NUMA=" << numa_id
                << " to CPU cores: " << cpu_numa_ss.str();
      auto res = worker->setCpuAffinity(cpu_ids);
      CHECK(res == 0) << "Fail to bind PMEM writer thread pool to NUMA node "
                      << numa_id;
      pmem_executors_[numa_id] =
          std::static_pointer_cast<folly::Executor>(worker);
    }
  }
  return pmem_executors_;
}

void CacheExecutor::DestroyAllExecutors() {
  std::lock_guard<std::mutex> lk(mtx_);
  common_executor_.reset();
  gc_executor_.reset();
  pmem_executors_.clear();
}

}  // namespace mtcache
