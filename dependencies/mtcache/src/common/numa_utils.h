#pragma once

#include "common/logging.h"

#include <memory>
#include <vector>

namespace mtcache {

// NumaInfo is an interface to query for NUMA information at runtime.
class NumaInfo {
 public:
  // Initialize NumaInfo.
  static void Init();

  // Returns the number of cores (including hyper-threaded) on this machine
  static int GetNumAllCores() {
    DCHECK(initialized_);
    return num_cores_;
  }

  // Returns the maximum number of cores that will be online in the system,
  // including any offline cores or cores that could be added via hot-plugging.
  static int GetNumOnlineCores() {
    DCHECK(initialized_);
    return max_num_cores_;
  }

  // Returns the core that the current thread is running on. Always in range
  // [0, GetNumOnlineCores()). Note that the thread may be migrated to a
  // different core at any time by the scheduler, so the caller should not
  // assume the answer will remain stable.
  static int GetCurrentCpuCore();

  // Returns the maximum number of NUMA nodes that will be online in the
  // system, including any that may be offline or disabled.
  static int GetMaxNumNumaNodes() {
    DCHECK(initialized_);
    return max_num_numa_nodes_;
  }

  // Returns the NUMA node of the core provided. 'core' must be in the range
  // [0, GetNumOnlineCores()).
  static int GetNumaNodeOfCpuCore(int core) {
    DCHECK(initialized_);
    CHECK_LE(0, core);
    CHECK_LT(core, max_num_cores_);
    return core_to_numa_node_[core];
  }

  // Returns the cores in a NUMA node. 'node' must be in the range
  // [0, GetNumOnlineCores()).
  static const std::vector<int>& GetCpuCoresOfNumaNode(int node) {
    DCHECK(initialized_);
    CHECK_LE(0, node);
    CHECK_LT(node, max_num_numa_nodes_);
    return numa_node_to_cores_[node];
  }

  /// Returns the cores in the same NUMA node as 'core'. 'core' must be in the
  /// range [0, GetNumOnlineCores()).
  static const std::vector<int>& GetCpuCoresOfSameNumaNode(int core) {
    DCHECK(initialized_);
    CHECK_LE(0, core);
    CHECK_LT(core, max_num_cores_);
    return GetCpuCoresOfNumaNode(GetNumaNodeOfCpuCore(core));
  }

  // Returns the index of the given core within the vector returned by
  // GetCoresOfNumaNode() and GetCoresOfSameNumaNode(). 'core' must be in the
  // range [0, GetNumOnlineCores()).
  static int GetNumaNodeCoreIdx(int core) {
    DCHECK(initialized_);
    CHECK_LE(0, core);
    CHECK_LT(core, max_num_cores_);
    return numa_node_core_idx_[core];
  }

  // Pin a thread to a specific cpu core
  static void BindThreadToCpuCore(int core);

 private:
  // Initialize NUMA-related state - called from Init();
  static void NumaTopology();

  // Initialize 'numa_node_to_cores_' based on 'max_num_numa_nodes_' and
  // 'core_to_numa_node_'. Called from InitNuma();
  static void CheckNumaNodeToCores();

  static bool initialized_;
  static int num_cores_;
  static int max_num_cores_;

  // Maximum possible number of NUMA nodes.
  static int max_num_numa_nodes_;

  // Vector with 'max_num_cores_' entries, each of which is the NUMA node of
  // that core.
  static std::vector<int> core_to_numa_node_;

  // Vector with 'max_num_numa_nodes_' entries, each of which is a vector of
  // the cores belonging to that NUMA node.
  static std::vector<std::vector<int>> numa_node_to_cores_;

  // Vector with 'max_num_cores_' entries, each of which is the index of that
  // core in its NUMA node.
  static std::vector<int> numa_node_core_idx_;

  // Example: if we have 2 NUMA sockets and each socket has 12 cores
  // NUMA topology:
  // node 0 cpus: 0 1 2 3 4 5 6 7 8 9 10 11
  // node 1 cpus: 12 13 14 15 16 17 18 19 20 21 22 23
  // core_to_numa_node_ = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  //                       1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
  // numa_node_to_cores_ = {{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
  //          {11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23}}
  // numa_node_core_idx_ = {0, 1 , 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
  //                        0, 1 , 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
};

}  // namespace mtcache
