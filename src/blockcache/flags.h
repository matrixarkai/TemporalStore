// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <gflags/gflags.h>

DECLARE_bool(enable_blockcache);
DECLARE_uint64(blockcache_dram_capacity);
DECLARE_string(blockcache_dram_replacement_policy);
DECLARE_uint64(blockcache_pmem_capacity);
DECLARE_uint64(blockcache_pmem_allocator_capacity);
DECLARE_string(blockcache_pmem_path);
DECLARE_string(blockcache_pmem_replacement_policy);
DECLARE_string(blockcache_dram_pmem_data_placement_type);
DECLARE_uint64(blockcache_dram_pmem_data_placement_threshold);
DECLARE_string(blockcache_ssd_replacement_policy);
DECLARE_uint64(blockcache_ssd_capacity);
DECLARE_string(blockcache_ssd_path);
DECLARE_bool(blockcache_ssd_instance_only);
DECLARE_string(blockcache_metric_id_prefix);
DECLARE_string(blockcache_metric_tag_keys);
DECLARE_string(blockcache_metric_tag_values);
DECLARE_bool(blockcache_enable_metrics);
DECLARE_bool(blockcache_clear_ssd_folder);
