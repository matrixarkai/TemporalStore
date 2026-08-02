#include "numa_utils.h"

#include <folly/Format.h>
#include <sys/sysinfo.h>

#include <filesystem>
#include <fstream>
#include <pthread.h>
#include <sched.h>
#include <string>

// The number of NUMA nodes used by the system
DECLARE_int32(used_num_numa_nodes);

namespace mtcache {

bool NumaInfo::initialized_ = false;
int NumaInfo::num_cores_ = 1;
int NumaInfo::max_num_cores_ = 1;
int NumaInfo::max_num_numa_nodes_;
std::vector<int> NumaInfo::core_to_numa_node_;

std::vector<std::vector<int>> NumaInfo::numa_node_to_cores_;
std::vector<int> NumaInfo::numa_node_core_idx_;

void NumaInfo::Init() {
  if (initialized_) return;
  num_cores_ = get_nprocs();
  max_num_cores_ = get_nprocs_conf();
  if (num_cores_ < max_num_cores_) {
    LOG(WARNING)
        << "The number of processors currently available in the system "
           "is less than the number of processors configured by the "
           "operating system";
    max_num_cores_ = num_cores_;
  }
  NumaTopology();
  initialized_ = true;
}

void NumaInfo::NumaTopology() {
  DCHECK(!initialized_);
  // Use the NUMA info in the /sys filesystem. which is part of the Linux ABI:
  // see https://www.kernel.org/doc/Documentation/ABI/stable/sysfs-devices-node
  // and
  // https://www.kernel.org/doc/Documentation/ABI/testing/sysfs-devices-system-cpu
  // The filesystem entries are only present if the kernel was compiled with
  // NUMA support.
  core_to_numa_node_.resize(max_num_cores_);

  if (!std::filesystem::is_directory("/sys/devices/system/node")) {
    LOG(WARNING) << "/sys/devices/system/node is not present - no NUMA support";
    // Assume a single NUMA node.
    max_num_numa_nodes_ = 1;
    std::fill(core_to_numa_node_.begin(), core_to_numa_node_.end(), 0);

    CheckNumaNodeToCores();
    return;
  }
  // Search for node subdirectories - node0, node1, node2, etc to determine
  // possible NUMA nodes.
  std::filesystem::directory_iterator dir_it("/sys/devices/system/node");
  max_num_numa_nodes_ = 0;
  for (; dir_it != std::filesystem::directory_iterator(); ++dir_it) {
    const std::string filename = dir_it->path().filename().string();
    if (filename.find("node") == 0) ++max_num_numa_nodes_;
  }
  if (max_num_numa_nodes_ == 0) {
    LOG(WARNING) << "Could not find nodes in /sys/devices/system/node";
    max_num_numa_nodes_ = 1;
  }

  CHECK_LE(FLAGS_used_num_numa_nodes, max_num_numa_nodes_);

  // Check which NUMA node each core belongs to based on the existence of a
  // symlink to the node subdirectory.
  for (int core = 0; core < max_num_cores_; ++core) {
    bool found_numa_node = false;
    for (int node = 0; node < max_num_numa_nodes_; ++node) {
      char path[50];
      snprintf(path, sizeof(path), "/sys/devices/system/cpu/cpu%d/node%d", core,
               node);
      if (std::filesystem::exists(path)) {
        core_to_numa_node_[core] = node;
        found_numa_node = true;
        break;
      }
    }
    if (!found_numa_node) {
      LOG(WARNING) << "Could not determine NUMA node for core " << core
                   << " from /sys/devices/system/cpu/";
      core_to_numa_node_[core] = 0;
    }
  }
  CheckNumaNodeToCores();
}

void NumaInfo::CheckNumaNodeToCores() {
  DCHECK(!initialized_);
  CHECK(numa_node_to_cores_.empty());
  numa_node_to_cores_.resize(max_num_numa_nodes_);
  numa_node_core_idx_.resize(max_num_cores_);
  for (int core = 0; core < max_num_cores_; ++core) {
    std::vector<int>* cores_of_node =
        &numa_node_to_cores_[core_to_numa_node_[core]];
    numa_node_core_idx_[core] = cores_of_node->size();
    cores_of_node->push_back(core);
  }
}

int NumaInfo::GetCurrentCpuCore() {
  DCHECK(initialized_);
  int cpu = sched_getcpu();
  if (cpu < 0) return 0;
  if (cpu >= max_num_cores_) {
    LOG_FIRST_N(WARNING, 5)
        << "sched_getcpu() return value " << cpu
        << ", which is greater than get_nprocs_conf() return value "
        << max_num_cores_ << ", now is " << get_nprocs_conf();
    cpu %= max_num_cores_;
  }
  return cpu;
}

void NumaInfo::BindThreadToCpuCore(int core) {
  DCHECK(initialized_);
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    LOG(WARNING) << "can't bind core %d!" << core;
  }
}

}  // namespace mtcache
