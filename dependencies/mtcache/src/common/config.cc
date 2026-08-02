#include <gflags/gflags.h>

#include <string>

// This file defines the default value of config variables via google gflags.
//
// =============================================
// Cache related configurations
// =============================================

// The space reserved for garbage collection in log-based allocator used in
// DRAM cache. This value is only used when log-based dram allocator is used
// for DRAM cache.
DEFINE_uint64(cache_dram_gc_reserved, (2LLU * 1024 * 1024 * 1024),
              "The space reserved for GC in log-based DRAM allocator");

// Max number of threads for DRAM cache. Used in DRAM cache allocator to
// set number of thread-local contexts.
DEFINE_int32(cache_dram_max_thread_num, 2000,
             "Max number of threads for DRAM cache");

// Extra capacity used for allcator capacity, only used for DRAM and PMEM cache.
DEFINE_double(
    allocator_capacity_extra_ratio, 0.3,
    "The extra factor to compute the allocator_capacity from capacity");

// The space reserved for garbage collection in log-based allocator used in
// PMEM cache. This value is only used when log-based pmem allocator is used
// for PMEM cache.
DEFINE_uint64(cache_pmem_gc_reserved, (2LLU * 1024 * 1024 * 1024),
              "The space reserved for GC in log-based PMEM allocator");

// Max number of threads for PMEM cache. Used in PMEM cache allocator to
// set number of thread-local contexts.
DEFINE_int32(cache_pmem_max_thread_num, 2000,
             "Max number of threads for PMEM cache");

DEFINE_int32(used_num_numa_nodes, 1,
             "The number of NUMA nodes used by the system");

// The number of threads of the common_executor that runs tasks such as:
// * callback of LoadingCache (insert value into cache)
// * callback of AsyncPmemWrite (update cache index after copy data from dram
//   to pmem)
DEFINE_int32(common_executor_num_threads, 16,
             "Number of threads to run common tasks");

// Options:
//   - NoFlush => do not flush data
//   - InstantFlush => flush data instantly after each write
//   - MiniBatchFlush => flush data after at least `mini_batch_flush_bytes`
//                       data written
DEFINE_string(cache_pmem_flush_policy, "NoFlush",
              "Flush policy for pmem cache");

DEFINE_uint64(
    cache_pmem_mini_batch_flush_bytes, 4096,
    "Number of bytes accumulated to trigger flush. Useful only when flush"
    " policy is 'MiniBatchFlush'");

DEFINE_bool(
    cache_enable_eviction_handler, true,
    "Enable/disable eviction handler in unified cache. When enabled, "
    "cache items evicted from DRAM cache will be inserted into PMEM cache if "
    "Tiered data placement is used. Cache items evicted from DRAM/PMEM cache "
    "will be inserted into SSD cache if this config option is enabled, the "
    "data placement type is SidebySide, and `l2_policy_use_eviction_handler` "
    "config option is set to true.");

DEFINE_bool(
    cache_pmem_enable_async_write, true,
    "Enable/disable async write in pmem cache. When enabled, cache items are "
    "inserted into PMEM cache asynchronously. When disabled, cache items are "
    "inserted into PMEM cache synchronously.");

DEFINE_bool(mtcache_enable_ssd_promotion, true,
            "Enable/disable async promotion from ssd to dram/pmem cache.");

DEFINE_bool(mtcache_enable_pmem_promotion, true,
            "Enable/disable async promotion from pmem to dram cache.");

DEFINE_string(dram_allocator_type, "Log",
              "Allocator type for DRAM storage. Options: Log, Pool, Jemalloc");

DEFINE_string(pmem_allocator_type, "Log",
              "Allocator type for PMEM storage. Options: Log, Pool");

DEFINE_uint64(pool_based_allocator_obj_len, 1024,
              "Single object memory size in pool-based allocator");

// Cache metrics parameters, 'ti' means 'tech infra'
DEFINE_int32(cache_latency_summary_time_window, 30,
             "Time window (in seconds) during which the percentile of query "
             "latency of unified caches are maintained");

DEFINE_bool(cache_collect_latency_summary, false,
            "Enable/disable the collection of query latency of unified caches");

// GC parameters
DEFINE_uint64(free_mem_min, 2ULL * 1024 * 1024 * 1024,
              "when the remaining capacity is less than free_mem_min, we "
              "trigger GC");  // 2GB
// We calculate fragmentation ratio (percentage) at chunk-level
// fragmentation_ratio = the number of bytes occupied by the deleted objects /
// the total number of bytes occupied by valid objects and deleted objects
DEFINE_int32(fragmentation_ratio_max, 20,
             "when the fragmentation rate of the overall pool is larger than "
             "fragmentation_ratio_max, we trigger GC");
DEFINE_int32(num_gc_workers, 4,
             "Number of threads in the GC worker thread pool");
// Time unit: milliseconds
DEFINE_int32(gc_check_interval, 1000,  // One sec
             "Interval GC_magnager re-checks the stats of the allocator");
DEFINE_bool(cache_force_gc, false, "Force to trigger GC");

// 0:terarkdb,1:zonedstore
DEFINE_int32(ssd_engine_type, 1, "The type of ssd storage engine");

// L2 Cache
DEFINE_int32(max_arc_cache_item_number, 100000,
             "The maximum number of cache items for the ARC algorithm");

// The batch size of cache item number that the l2 cache fetches from the l1
// cache each time.
DEFINE_int32(l2_cache_tail_batch_size, 1000,
             "Batch size of cache item that l2 cache fetch from l1 cache");

// The queue size of access records buffer.
DEFINE_int32(l2_cache_access_buffer_capacity, 100000,
             "The buffer capacity of l2 cache access buffer.");

// The queue size of l2 cache write buffer.
DEFINE_int32(l2_cache_write_buffer_capacity, 10000,
             "The buffer capacity of l2 cache write waiting buffer.");

// The interval between two task to process access record , in millisecond.
DEFINE_int32(l2_cache_access_interval_ms, 1,
             "The interval between two l2 fetch data batch, in millisecond.");

// The interval between two task to fetch data from l1 cache, in millisecond.
DEFINE_int32(l2_cache_tail_interval_ms, 1000,
             "The interval between two l2 fetch data batch, in millisecond.");

// The interval between two task to write data to l2 cache, in millisecond.
DEFINE_int32(l2_cache_write_interval_ms, 1000,
             "The interval between two l2 write data batch, in millisecond.");

DEFINE_bool(
    l2_policy_async_on_access, true,
    "Use background threads to consume access records asynchronously. "
    "The advantage is that it does not affect the hot-path, and the "
    "disadvantage is that it the l2 policy may not to be updated in time. ");

DEFINE_bool(
    l2_policy_use_eviction_handler, false,
    "Use eviction handler to fill data in l2 cache write waiting list.");

// SLRU parameters
DEFINE_int32(hot_lru_pct, 20, "Ratio of hot items in cache");
DEFINE_int32(warm_lru_pct, 40, "Ratio of warm items in cache");
DEFINE_bool(cache_slru_ut, false,
            "We use cache_slru_ut to indicate that we are testing SLRU UT");
DEFINE_int32(slru_num_segments, 256, "Number of segments in SLRU");

DEFINE_uint64(pool_chunk_sz, 4 * (1 << 20) /* 4MB */,
              "The preload chunk size of the pool-based allocator.");

DEFINE_int32(
    pool_obj_cache_max_chunk_num, 2,
    "The maximum number of memory chunks preloaded by the object cache");

DEFINE_bool(cache_enable_pmem_data_recovery, false,
            "Whether to recover PMEM cache records when starting PMEM cache.");
DEFINE_bool(cache_enable_ssd_data_recovery, false,
            "Whether to recover SSD cache records when starting SSD cache.");
DEFINE_bool(cache_ssd_data_recovery_in_background, false,
            "Whether to recover SSD cache records in background to avoid "
            "blocking the starting process.");

DEFINE_int32(num_pmem_cache_per_numa_writer_threads, 3,
             "The thread number of pmem writer in each numa node");

// TODO(dbc) Do micro bench to see how many threads is most-effective
DEFINE_int32(num_threads_recover_pmem, 16,
             "Number of threads to recover PMEM cache");
