// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/cmd_manager.h"
#include "common/metrics.h"

namespace bcache2 {
namespace partition {

// partition
const char kMetricsLoadPartitionSuccess[] = "load.success";
const char kMetricsLoadPartitionFailed[] = "load.failed";
const char kMetricsLoadPartitionLatency[] = "load.latency";
const char kMetricsUnLoadPartitionSuccess[] = "unload.success";
const char kMetricsUnLoadPartitionLatency[] = "unload.latency";

struct EvicterMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> evict_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> evict_object_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> evict_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> evict_failed;
    std::unique_ptr<MetricsEnv::CounterHolder> evict_oom;

    void Init(MetricsManager* metrics_manager) {
        evict_qps = metrics_manager->Get<MetricsEnv::Counter>("evict.qps", {});
        evict_object_qps = metrics_manager->Get<MetricsEnv::Counter>("evict.object_qps", {});
        evict_latency = metrics_manager->Get<MetricsEnv::Histogram>("evict.latency", {});
        evict_failed = metrics_manager->Get<MetricsEnv::Counter>("evict.failed_qps", {});
        evict_oom = metrics_manager->Get<MetricsEnv::Counter>("evict.oom", {});
    }
};

struct SlotStoreMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> dump_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> blind_dump_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> dump_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> dump_throughput;
    std::unique_ptr<MetricsEnv::HistogramHolder> dump_page_size;
    std::unique_ptr<MetricsEnv::CounterHolder> load_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> load_failed_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> load_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> load_throughput;
    std::unique_ptr<MetricsEnv::CounterHolder> object_size_overlarge_count;

    void Init(MetricsManager* metrics_manager) {
        dump_qps = metrics_manager->Get<MetricsEnv::Counter>("slot.dump.qps", {});
        blind_dump_qps = metrics_manager->Get<MetricsEnv::Counter>("slot.blind_dump.qps", {});
        dump_latency = metrics_manager->Get<MetricsEnv::Histogram>("slot.dump.latency", {});
        dump_throughput = metrics_manager->Get<MetricsEnv::Counter>("slot.dump.throughput", {});
        dump_page_size = metrics_manager->Get<MetricsEnv::Histogram>("slot.dump.page_size", {});
        load_qps = metrics_manager->Get<MetricsEnv::Counter>("slot.load.qps", {});
        load_failed_qps = metrics_manager->Get<MetricsEnv::Counter>("slot.load.failed_qps", {});
        load_latency = metrics_manager->Get<MetricsEnv::Histogram>("slot.load.latency", {});
        load_throughput = metrics_manager->Get<MetricsEnv::Counter>("slot.load.throughput", {});
        object_size_overlarge_count =
            metrics_manager->Get<MetricsEnv::Counter>("slot.object_size.overlarge.count", {});
    }
};

struct IndexMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> slot_new_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> slot_get_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> slot_hit_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> slot_in_memory_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> gc_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> gc_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> gc_scan_item_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> gc_scan_throughput;
    std::unique_ptr<MetricsEnv::CounterHolder> gc_rewrite_item_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> gc_rewrite_throughput;
    std::unique_ptr<MetricsEnv::GuageHolder> gc_utility;
    std::unique_ptr<MetricsEnv::GuageHolder> avg_log_size;
    std::unique_ptr<MetricsEnv::GuageHolder> avg_log_item;
    std::unique_ptr<MetricsEnv::GuageHolder> valid_item_count;
    std::unique_ptr<MetricsEnv::GuageHolder> slot_count;
    std::unique_ptr<MetricsEnv::GuageHolder> dirty_slot_count;
    std::unique_ptr<MetricsEnv::GuageHolder> slot_hit_ratio;
    std::unique_ptr<MetricsEnv::GuageHolder> slot_in_memory_ratio;
    std::unique_ptr<MetricsEnv::HistogramHolder> load_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> write_throughput;
    std::unique_ptr<MetricsEnv::CounterHolder> dirty_slot_merge_count;
    std::unique_ptr<MetricsEnv::GuageHolder> index_oplog_gap;
    std::unique_ptr<MetricsEnv::HistogramHolder> slot_object_num;
    std::unique_ptr<MetricsEnv::HistogramHolder> slot_page_num;
    std::unique_ptr<MetricsEnv::HistogramHolder> slot_object_page_size;

    void Init(MetricsManager* metrics_manager) {
        slot_new_qps = metrics_manager->Get<MetricsEnv::Counter>("index.slot.new_qps", {});
        slot_get_qps = metrics_manager->Get<MetricsEnv::Counter>("index.slot.get_qps", {});
        slot_hit_qps = metrics_manager->Get<MetricsEnv::Counter>("index.slot.hit_qps", {});
        slot_in_memory_qps =
            metrics_manager->Get<MetricsEnv::Counter>("index.slot.in_memory_qps", {});
        gc_qps = metrics_manager->Get<MetricsEnv::Counter>("index.gc_qps", {});
        gc_latency = metrics_manager->Get<MetricsEnv::Histogram>("index.gc.latency", {});
        gc_scan_item_qps = metrics_manager->Get<MetricsEnv::Counter>("index.gc.scan_item_qps", {});
        gc_utility = metrics_manager->Get<MetricsEnv::Guage>("index.gc.utility", {});
        gc_scan_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("index.gc.scan_throughput", {});
        gc_rewrite_item_qps = metrics_manager->Get<MetricsEnv::Counter>("index.gc.rewrite_qps", {});
        gc_rewrite_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("index.gc.rewrite_throughput", {});
        avg_log_size = metrics_manager->Get<MetricsEnv::Guage>("index.log_size.avg", {});
        avg_log_item = metrics_manager->Get<MetricsEnv::Guage>("index.avg_log_item", {});
        valid_item_count = metrics_manager->Get<MetricsEnv::Guage>("index.valid_item_count", {});
        slot_count = metrics_manager->Get<MetricsEnv::Guage>("index.slot.count", {});
        dirty_slot_count = metrics_manager->Get<MetricsEnv::Guage>("index.slot.dirty_count", {});
        slot_hit_ratio = metrics_manager->Get<MetricsEnv::Guage>("index.slot.hit_ratio", {});
        slot_in_memory_ratio =
            metrics_manager->Get<MetricsEnv::Guage>("index.slot.in_memory_ratio", {});
        write_throughput = metrics_manager->Get<MetricsEnv::Counter>("index.write_throughput", {});
        load_latency = metrics_manager->Get<MetricsEnv::Histogram>("index.load_latency", {});
        dirty_slot_merge_count =
            metrics_manager->Get<MetricsEnv::Counter>("index.dirty_slot_merge_count", {});
        index_oplog_gap = metrics_manager->Get<MetricsEnv::Guage>("index.oplog_gap", {});
        slot_object_num = metrics_manager->Get<MetricsEnv::Histogram>("index.slot_object_num", {});
        slot_page_num = metrics_manager->Get<MetricsEnv::Histogram>("index.slot_page_num", {});
        slot_object_page_size =
            metrics_manager->Get<MetricsEnv::Histogram>("index.slot_object_page_size", {});
    }
};

struct PageStoreMetrics {
    std::unique_ptr<MetricsEnv::GuageHolder> zone_count;
    std::unique_ptr<MetricsEnv::HistogramHolder> load_latency;
    std::unique_ptr<MetricsEnv::HistogramHolder> page_size;
    std::unique_ptr<MetricsEnv::HistogramHolder> compressed_page_size;
    std::unique_ptr<MetricsEnv::HistogramHolder> compress_ratio;
    std::unique_ptr<MetricsEnv::CounterHolder> blockcache_get_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> blockcache_hit_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> persistent_read_qps;

    void Init(MetricsManager* metrics_manager) {
        zone_count = metrics_manager->Get<MetricsEnv::Guage>("page_store.zone_count", {});
        load_latency = metrics_manager->Get<MetricsEnv::Histogram>("page_store.load_latency", {});
        page_size = metrics_manager->Get<MetricsEnv::Histogram>("page_store.page_size", {});
        compressed_page_size =
            metrics_manager->Get<MetricsEnv::Histogram>("page_store.compressed_page_size", {});
        compress_ratio =
            metrics_manager->Get<MetricsEnv::Histogram>("page_store.compress_ratio", {});
        blockcache_get_qps =
            metrics_manager->Get<MetricsEnv::Counter>("page_store.blockcache_get_qps", {});
        blockcache_hit_qps =
            metrics_manager->Get<MetricsEnv::Counter>("page_store.blockcache_hit_qps", {});
        persistent_read_qps =
            metrics_manager->Get<MetricsEnv::Counter>("page_store.persistent_read_qps", {});
    }
};

struct ZoneMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> read_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> read_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> read_throughput;
    std::unique_ptr<MetricsEnv::CounterHolder> write_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> write_throughput;

    void Init(MetricsManager* metrics_manager, uint32_t zone_id) {
        std::string zone_id_str = std::to_string(zone_id);
        read_qps =
            metrics_manager->Get<MetricsEnv::Counter>("zone.read_qps", {{"zone_id", zone_id_str}});
        read_latency = metrics_manager->Get<MetricsEnv::Histogram>("zone.read_latency",
                                                                   {{"zone_id", zone_id_str}});
        read_throughput = metrics_manager->Get<MetricsEnv::Counter>("zone.read_throughput",
                                                                    {{"zone_id", zone_id_str}});
        write_qps =
            metrics_manager->Get<MetricsEnv::Counter>("zone.write_qps", {{"zone_id", zone_id_str}});
        write_throughput = metrics_manager->Get<MetricsEnv::Counter>("zone.write_throughput",
                                                                     {{"zone_id", zone_id_str}});
    }
};

struct StorageManagerMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> loop_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> loop_latency;
    std::unique_ptr<MetricsEnv::GuageHolder> max_memory;
    std::unique_ptr<MetricsEnv::GuageHolder> used_memory;
    std::unique_ptr<MetricsEnv::HistogramHolder> reclaim_oplog_latency;
    std::unique_ptr<MetricsEnv::HistogramHolder> reclaim_memory_latency;
    std::unique_ptr<MetricsEnv::HistogramHolder> reclaim_page_latency;
    std::unique_ptr<MetricsEnv::HistogramHolder> reclaim_index_latency;
    std::unique_ptr<MetricsEnv::HistogramHolder> page_compaction_latency;

    void Init(MetricsManager* metrics_manager) {
        loop_qps = metrics_manager->Get<MetricsEnv::Counter>("storage_manager.loop_qps", {});
        loop_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("storage_manager.loop_latency", {});
        reclaim_oplog_latency = metrics_manager->Get<MetricsEnv::Histogram>(
            "storage_manager.reclaim_oplog_latency", {});
        reclaim_memory_latency = metrics_manager->Get<MetricsEnv::Histogram>(
            "storage_manager.reclaim_memory_latency", {});
        reclaim_page_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("storage_manager.reclaim_page_latency", {});
        reclaim_index_latency = metrics_manager->Get<MetricsEnv::Histogram>(
            "storage_manager.reclaim_index_latency", {});
        page_compaction_latency = metrics_manager->Get<MetricsEnv::Histogram>(
            "storage_manager.page_compaction_latency", {});
        max_memory = metrics_manager->Get<MetricsEnv::Guage>("max_memory", {});
        used_memory = metrics_manager->Get<MetricsEnv::Guage>("used_memory", {});
    }
};

struct PageGcMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> gc_loop_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> gc_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> gc_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> gc_scan_slot_qps;
    std::unique_ptr<MetricsEnv::GuageHolder> total_bytes;
    std::unique_ptr<MetricsEnv::GuageHolder> used_bytes;
    std::unique_ptr<MetricsEnv::GuageHolder> page_store_total_bytes;
    std::unique_ptr<MetricsEnv::GuageHolder> page_store_used_bytes;
    std::unique_ptr<MetricsEnv::GuageHolder> oplogger_total_bytes;
    std::unique_ptr<MetricsEnv::GuageHolder> oplogger_used_bytes;
    std::unique_ptr<MetricsEnv::GuageHolder> utility;
    std::unique_ptr<MetricsEnv::GuageHolder> page_store_utility;
    std::unique_ptr<MetricsEnv::GuageHolder> oplogger_utility;
    std::unique_ptr<MetricsEnv::GuageHolder> current_zone_id;
    std::unique_ptr<MetricsEnv::CounterHolder> rewrite_throughput;
    std::unique_ptr<MetricsEnv::GuageHolder> dirty_slot_count;
    std::unique_ptr<MetricsEnv::HistogramHolder> pick_zone_latency;
    std::unique_ptr<MetricsEnv::HistogramHolder> scan_slots_latency;
    std::unique_ptr<MetricsEnv::HistogramHolder> dump_dirty_slots_latency;
    std::unique_ptr<MetricsEnv::HistogramHolder> rewrite_pages_latency;
    std::unique_ptr<MetricsEnv::HistogramHolder> gc_zone_latency;

    void Init(MetricsManager* metrics_manager) {
        gc_loop_qps = metrics_manager->Get<MetricsEnv::Counter>("page_gc.loop_qps", {});
        gc_qps = metrics_manager->Get<MetricsEnv::Counter>("page_gc.qps", {});
        gc_latency = metrics_manager->Get<MetricsEnv::Histogram>("page_gc.latency", {});
        gc_scan_slot_qps = metrics_manager->Get<MetricsEnv::Counter>("page_gc.scan_slot_qps", {});
        total_bytes = metrics_manager->Get<MetricsEnv::Guage>("page_gc.total_bytes", {});
        used_bytes = metrics_manager->Get<MetricsEnv::Guage>("page_gc.used_bytes", {});
        page_store_total_bytes =
            metrics_manager->Get<MetricsEnv::Guage>("page_gc.page_store_total_bytes", {});
        page_store_used_bytes =
            metrics_manager->Get<MetricsEnv::Guage>("page_gc.page_store_used_bytes", {});
        oplogger_total_bytes =
            metrics_manager->Get<MetricsEnv::Guage>("page_gc.oplogger_total_bytes", {});
        oplogger_used_bytes =
            metrics_manager->Get<MetricsEnv::Guage>("page_gc.oplogger_used_bytes", {});
        utility = metrics_manager->Get<MetricsEnv::Guage>("page_gc.utility", {});
        page_store_utility =
            metrics_manager->Get<MetricsEnv::Guage>("page_gc.page_store_utility", {});
        oplogger_utility = metrics_manager->Get<MetricsEnv::Guage>("page_gc.oplogger_utility", {});
        current_zone_id = metrics_manager->Get<MetricsEnv::Guage>("page_gc.current_zone_id", {});
        rewrite_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("page_gc.rewrite_throughput", {});
        dirty_slot_count = metrics_manager->Get<MetricsEnv::Guage>("page_gc.dirty_slot_count", {});
        pick_zone_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("page_gc.pick_zone_latency", {});
        scan_slots_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("page_gc.scan_slots_latency", {});
        dump_dirty_slots_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("page_gc.dump_dirty_slots_latency", {});
        rewrite_pages_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("page_gc.rewrite_pages_latency", {});
        gc_zone_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("page_gc.gc_zone_latency", {});
    }
};

struct ZoneGcMetrics {
    std::unique_ptr<MetricsEnv::GuageHolder> total_bytes;
    std::unique_ptr<MetricsEnv::GuageHolder> used_bytes;
    std::unique_ptr<MetricsEnv::GuageHolder> utility;

    void Init(MetricsManager* metrics_manager, uint32_t zone_id) {
        std::string zone_id_str = std::to_string(zone_id);
        total_bytes =
            metrics_manager->Get<MetricsEnv::Guage>("zone.total_bytes", {{"zone_id", zone_id_str}});
        used_bytes =
            metrics_manager->Get<MetricsEnv::Guage>("zone.used_bytes", {{"zone_id", zone_id_str}});
        utility =
            metrics_manager->Get<MetricsEnv::Guage>("zone.utility", {{"zone_id", zone_id_str}});
    }
};

// expire
const char kMetricsHitExpiredKeyCounter[] = "expirer.hit_expired_key";
const char kMetricsExpireKeyCounter[] = "expirer.expire_key.count";
const char kMetricsExpireKeyLatency[] = "expirer.expire_key.latecny";

struct AllocatorMetrics {
    std::unique_ptr<MetricsEnv::GuageHolder> alloced_size;
    std::unique_ptr<MetricsEnv::GuageHolder> alloc_cnt;
    std::unique_ptr<MetricsEnv::GuageHolder> dealloc_cnt;

    void Init(MetricsManager* metrics_manager, std::string allocator_name) {
        alloced_size = metrics_manager->Get<MetricsEnv::Guage>("allocator.alloced_size",
                                                               {{"allocator", allocator_name}});
        alloc_cnt = metrics_manager->Get<MetricsEnv::Guage>("allocator.alloc_cnt",
                                                            {{"allocator", allocator_name}});
        dealloc_cnt = metrics_manager->Get<MetricsEnv::Guage>("allocator.dealloc_cnt",
                                                              {{"allocator", allocator_name}});
    }
};

struct OploggerMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> write_kvlog_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> write_kvlog_throughput;
    std::unique_ptr<MetricsEnv::CounterHolder> write_page_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> write_page_throughput;
    std::unique_ptr<MetricsEnv::CounterHolder> write_del_log_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> write_del_log_throughput;
    std::unique_ptr<MetricsEnv::CounterHolder> write_ttl_log_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> write_ttl_log_throughput;
    std::unique_ptr<MetricsEnv::CounterHolder> read_page_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> read_page_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> read_page_throughput;
    std::unique_ptr<MetricsEnv::GuageHolder> current_item_size;

    void Init(MetricsManager* metrics_manager) {
        write_kvlog_qps = metrics_manager->Get<MetricsEnv::Counter>("oplogger.write_kvlog_qps", {});
        write_kvlog_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("oplogger.write_kvlog_throughput", {});
        write_page_qps = metrics_manager->Get<MetricsEnv::Counter>("oplogger.write_page_qps", {});
        write_page_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("oplogger.write_page_throughput", {});
        write_del_log_qps =
            metrics_manager->Get<MetricsEnv::Counter>("oplogger.write_del_log_qps", {});
        write_del_log_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("oplogger.write_del_log_throughput", {});
        write_ttl_log_qps =
            metrics_manager->Get<MetricsEnv::Counter>("oplogger.write_ttl_log_qps", {});
        write_ttl_log_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("oplogger.write_ttl_log_throughput", {});
        read_page_qps = metrics_manager->Get<MetricsEnv::Counter>("oplogger.read_page_qps", {});
        read_page_latency =
            metrics_manager->Get<MetricsEnv::Histogram>("oplogger.read_page_latency", {});
        read_page_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("oplogger.read_page_throughput", {});
        current_item_size =
            metrics_manager->Get<MetricsEnv::Guage>("oplogger.current_item_size", {});
    }
};

struct ReplicatorMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> loop_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> loop_latency;
    std::unique_ptr<MetricsEnv::CounterHolder> replay_oplog_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> replay_index_log_qps;
    std::unique_ptr<MetricsEnv::GuageHolder> oplog_gap;
    std::unique_ptr<MetricsEnv::GuageHolder> index_log_gap;
    std::unique_ptr<MetricsEnv::GuageHolder> index_log_lag_ms;
    std::unique_ptr<MetricsEnv::GuageHolder> oplog_lag_ms;

    void Init(MetricsManager* metrics_manager) {
        loop_qps = metrics_manager->Get<MetricsEnv::Counter>("replicator.loop_qps", {});
        loop_latency = metrics_manager->Get<MetricsEnv::Histogram>("replicator.loop_latency", {});
        replay_oplog_qps =
            metrics_manager->Get<MetricsEnv::Counter>("replicator.replay_oplog_qps", {});
        replay_index_log_qps =
            metrics_manager->Get<MetricsEnv::Counter>("replicator.replay_index_log_qps", {});
        oplog_gap = metrics_manager->Get<MetricsEnv::Guage>("replicator.oplog_gap", {});
        index_log_gap = metrics_manager->Get<MetricsEnv::Guage>("replicator.index_log_gap", {});
        index_log_lag_ms =
            metrics_manager->Get<MetricsEnv::Guage>("replicator.index_log_lag_ms", {});
        oplog_lag_ms = metrics_manager->Get<MetricsEnv::Guage>("replicator.oplog_lag_ms", {});
    }
};

struct PageCompactionMetrics {
    std::unique_ptr<MetricsEnv::CounterHolder> loop_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> scan_slot_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> compact_object_qps;
    std::unique_ptr<MetricsEnv::HistogramHolder> load_page_num;
    std::unique_ptr<MetricsEnv::CounterHolder> load_page_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> load_page_throughput;
    std::unique_ptr<MetricsEnv::HistogramHolder> dump_page_num;
    std::unique_ptr<MetricsEnv::CounterHolder> dump_page_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> dump_page_throughput;
    std::unique_ptr<MetricsEnv::HistogramHolder> compaction_ratio;

    void Init(MetricsManager* metrics_manager) {
        loop_qps = metrics_manager->Get<MetricsEnv::Counter>("page_compaction.loop_qps", {});
        scan_slot_qps =
            metrics_manager->Get<MetricsEnv::Counter>("page_compaction.scan_slot_qps", {});
        compact_object_qps =
            metrics_manager->Get<MetricsEnv::Counter>("page_compaction.compact_object_qps", {});
        load_page_num =
            metrics_manager->Get<MetricsEnv::Histogram>("page_compaction.load_page_num", {});
        load_page_qps =
            metrics_manager->Get<MetricsEnv::Counter>("page_compaction.load_page_qps", {});
        load_page_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("page_compaction.load_page_throughput", {});
        dump_page_num =
            metrics_manager->Get<MetricsEnv::Histogram>("page_compaction.dump_page_num", {});
        dump_page_qps =
            metrics_manager->Get<MetricsEnv::Counter>("page_compaction.dump_page_qps", {});
        dump_page_throughput =
            metrics_manager->Get<MetricsEnv::Counter>("page_compaction.dump_page_throughput", {});
        compaction_ratio =
            metrics_manager->Get<MetricsEnv::Histogram>("page_compaction.compaction_ratio", {});
    }
};

struct CmdMetrics {
    std::unique_ptr<RequestMetrics> req_metrics;
    std::unique_ptr<MetricsEnv::CounterHolder> read_limit_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> write_limit_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> object_get_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> object_hit_qps;
    std::unique_ptr<MetricsEnv::CounterHolder> object_in_memory_qps;
    std::unique_ptr<MetricsEnv::GuageHolder> object_hit_ratio;
    std::unique_ptr<MetricsEnv::GuageHolder> object_in_memory_ratio;

    void Init(MetricsManager* metrics_manager, const std::string& module_name,
              const std::string& cmd_name) {
        req_metrics.reset(new RequestMetrics(metrics_manager, "cmd",
                                             {{"module", module_name}, {"cmd", cmd_name}}));
        read_limit_qps = metrics_manager->Get<MetricsEnv::Counter>(
            "cmd.read_limit_qps", {{"module", module_name}, {"cmd", cmd_name}});
        write_limit_qps = metrics_manager->Get<MetricsEnv::Counter>(
            "cmd.write_limit_qps", {{"module", module_name}, {"cmd", cmd_name}});
        object_get_qps = metrics_manager->Get<MetricsEnv::Counter>(
            "cmd.object_get_qps", {{"module", module_name}, {"cmd", cmd_name}});
        object_hit_qps = metrics_manager->Get<MetricsEnv::Counter>(
            "cmd.object_hit_qps", {{"module", module_name}, {"cmd", cmd_name}});
        object_in_memory_qps = metrics_manager->Get<MetricsEnv::Counter>(
            "cmd.object_in_memory_qps", {{"module", module_name}, {"cmd", cmd_name}});
        object_hit_ratio = metrics_manager->Get<MetricsEnv::Guage>(
            "cmd.object_hit_ratio", {{"module", module_name}, {"cmd", cmd_name}});
        object_in_memory_ratio = metrics_manager->Get<MetricsEnv::Guage>(
            "cmd.object_in_memory_ratio", {{"module", module_name}, {"cmd", cmd_name}});
    }
};

struct CmdExecutorMetrics {
    std::vector<std::vector<std::unique_ptr<CmdMetrics>>> cmd_metrics_;
    std::vector<std::unique_ptr<ModuleMetrics>> module_metrics_;

    void Init(MetricsManager* metrics_manager) {
        auto& cmd_manager = CmdManager::Instance();
        const std::vector<CmdManager::ModuleInfo>& modules_info = cmd_manager.GetModuleInfos();
        for (size_t i = 0; i < modules_info.size(); i++) {
            if (!Module_IsValid(i)) {
                continue;
            }

            const CmdManager::ModuleInfo& module_info = modules_info[i];
            if (i >= cmd_metrics_.size()) {
                cmd_metrics_.resize(i + 1);
                module_metrics_.resize(i + 1);
            }
            if (module_info.module_metrics_factory_func) {
                module_metrics_[i].reset(module_info.module_metrics_factory_func(metrics_manager));
            }
            for (size_t j = 0; j < module_info.cmd_executors.size(); j++) {
                const CmdManager::CmdInfo& cmd_info = module_info.cmd_executors[j];
                cmd_metrics_[i].emplace_back(new CmdMetrics());
                cmd_metrics_[i].back()->Init(metrics_manager, module_info.name, cmd_info.name);
            }
        }
    }
};

struct SlotContextManagerMetrics {
    std::unique_ptr<MetricsEnv::HistogramHolder> slot_log_num;

    void Init(MetricsManager* metrics_manager) {
        slot_log_num = metrics_manager->Get<MetricsEnv::Histogram>("slot_context.slot_log_num", {});
    }
};

}  // namespace partition
}  // namespace bcache2
