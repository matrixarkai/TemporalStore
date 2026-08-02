// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "blockcache/flags.h"

#include <gflags/gflags.h>

#include <string>

// =============================================
// Cache related configurations
// =============================================

DEFINE_bool(enable_blockcache, false, "Whether enable blockcache in bcache2");

// The capacity of usable dram cache. This capacity is used only to store
// user data (i.e. KEY and VALUE).
DEFINE_uint64(blockcache_dram_capacity, (1LLU * 1024 * 1024 * 1024),
              "The capacity of usable DRAM (bytes) for data cache");

// Options:
//   - FIFO => First-In First-Out replacement policy
//   - SLRU => Segmented Least Recently Used replacement policy
DEFINE_string(blockcache_dram_replacement_policy, "SLRU",
              "The cache replacement policy for DRAM cache instance");
// The capacity of usable pmem cache. This capacity is used only to store
// user data (KEY and VALUE).
// If set to 0, then pmem cached is disable.
DEFINE_uint64(blockcache_pmem_capacity, (0LLU * 1024 * 1024 * 1024),
              "The capacity of usable PMEM (bytes) for data cache");

// data_cache_pmem_path and meta_cache_pmem_path defines the pmem device path
// used in the data cache and meta_cache.
// It may contain multiple paths and use ";" to separate them. The number of
// paths must be equal to FLAGS_used_num_numa_nodes.
// Note that the paths defined here must be in the order of NUMA nodes, e.g:
// if there are two paths for data_cache_pmem_path: /a and /b, then /a must be
// at the PMEM of the 1st NUMA node and /b must be at the 2nd NUMA node.
DEFINE_string(blockcache_pmem_path,
              "/mnt/pmem0/bcache2_data/blockcache_pmem;/mnt/pmem1/bcache2_data/blockcache_pmem",
              "Paths for data cache pmem storage");

// Options:
//   - FIFO => First-In First-Out replacement policy
//   - SLRU => Segmented Least Recently Used replacement policy
DEFINE_string(blockcache_pmem_replacement_policy, "SLRU",
              "The cache replacement policy for PMEM cache instance");

// Options:
//   - SideBySide => Cache items are inserted into DRAM or PMEM cache instance,
//                   based on whether the value size is greater than a
//                   threshold.
//   - Tiered => Cache items are inserted in DRAM cache instance initially, and
//               evicted from DRAM cache into PMEM cache if eviction handler is
//               enabled.
DEFINE_string(blockcache_dram_pmem_data_placement_type, "SideBySide",
              "The data placement type between DRAM instance and PMEM instance "
              "in unified cache");

DEFINE_uint64(blockcache_dram_pmem_data_placement_threshold, 256,
              "Threshold to determine whether a cache item should be placed in DRAM or "
              "PMEM cache instance if SideBySide data placement type is used. If the "
              "value size is smaller than the threshold, the item is placed into DRAM "
              "instance; otherwise, it is placed into PMEM instance.");

// Options:
//   - FIFO => First-In First-Out replacement policy
//   - SLRU => Segmented Least Recently Used replacement policy
DEFINE_string(blockcache_ssd_replacement_policy, "SLRU",
              "The cache replacement policy for SSD cache instance");

// The capacity for SSD cache.
DEFINE_uint64(blockcache_ssd_capacity, (10LLU * 1024 * 1024 * 1024),
              "The capacity of ssd data cache in bytes");

DEFINE_string(blockcache_ssd_path, "/opt/tiger/bcache2_data/data_cache_ssd",
              "Path for SSD data cache storage");

// SSD cache only mode. If this mode is enabled, DRAM and PMEM cache instances
// will NOT be used even if initalized. This mode should be used for testing
// purpose only.
DEFINE_bool(blockcache_ssd_instance_only, false,
            "DRAM/PMEM cache instances are NOT used if this option is set.");

DEFINE_string(blockcache_metric_id_prefix, "bcache2.blockcache",
              "Prefix of the metric ID for blockcache.");

DEFINE_string(blockcache_metric_tag_keys, "key1;key2", "Metric tag keys for blockcache.");

DEFINE_string(blockcache_metric_tag_values, "value1;value2", "Metric tag values for blockcache.");

DEFINE_bool(blockcache_enable_metrics, false, "Whether to enable metrics in blockcache.");

DEFINE_bool(blockcache_clear_ssd_folder, true,
            "Whether to clear ssd folder when blockcache stops.");
