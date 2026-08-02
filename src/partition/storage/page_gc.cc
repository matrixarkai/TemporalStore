// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/page_gc.h"

#include <gflags/gflags.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "common/coclosure.h"
#include "common/logging.h"
#include "partition/metrics.h"
#include "partition/partition.h"
#include "partition/partition_fiu.h"
#include "partition/storage/op_logger.h"

DECLARE_double(storage_gc_space_utility_threshold);
DECLARE_uint64(storage_gc_max_bytes_per_round);
DECLARE_uint64(storage_gc_max_slots_per_round);
DECLARE_uint64(storage_gc_zone_destroy_delay_ms);

namespace bcache2 {
namespace partition {

namespace {

size_t LowerAlign(size_t offset, size_t unit) { return offset / unit * unit; }

}  // namespace

PageGc::PageGc(Partition* partition, stream::Env* env, Index* index, PageStore* page_store,
               SlotStore* slot_store, OpLogger* op_logger, MetricsManager* metrics_manager)
    : partition_(partition),
      env_(env),
      index_(index),
      page_store_(page_store),
      slot_store_(slot_store),
      op_logger_(op_logger),
      metrics_manager_(metrics_manager) {
    metrics_.Init(metrics_manager);
}

PageGc::~PageGc() {}

Status PageGc::Gc() {
    metrics_.gc_loop_qps->get()->Increment();
    if (UNLIKELY(recycled_oplog_length_ == UINT64_MAX)) {
        recycled_oplog_length_ = op_logger_->Start();  // initially, no oplog to recycle
    }
    if (current_gc_zone_.zone_id == kInvalidZoneId) {
        Status status = PurgeCompactedZones();
        if (!status.ok()) {
            LOG_WARNING("Failed to purge compacted zones")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", status);
            return status;
        }
        PickNextZone();
    }
    if (current_gc_zone_.zone_id != kInvalidZoneId) {
        GcCurrentZone();
    }
    return Status::OK();
}

Status PageGc::PurgeCompactedZones() {
    Controller ctrl;
    SYNC_CALL(index_->Commit, &ctrl);  // commit the index before GC
    if (ctrl.status().IsStreamAbort()) {
        LOG_DEBUG("Index stream aborted")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Status", ctrl.status());
        return ctrl.status();
    }
    BYTE_ASSERT(ctrl.status().ok()) << ctrl.status();

    uint64_t now_ms = GetCurrentTimeInMs();
    auto pagestore_zones = page_store_->GetZoneInfos();
    for (const auto& item : pagestore_zones) {
        const storage::IndexLog::ZoneInfo& zone_info = item.second;
        // only GC recycled zones
        if (zone_info.state() != storage::IndexLog::RECYCLED ||
            zone_info.recycled_time_ms() + FLAGS_storage_gc_zone_destroy_delay_ms > now_ms) {
            LOG_DEBUG("Skip zone")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Zone", zone_info.ShortDebugString());
            continue;
        }
        uint32_t zone_used_bytes =
            index_->GetZoneStats().GetPageStoreUsedBytes(zone_info.zone_id());
        std::stringstream ss;
        ss << "PartitionId: " << partition_->GetPartitionID() << ", ZoneId: " << zone_info.zone_id()
           << ", UsedBytes: " << zone_used_bytes;
        // TODO(wangluping.502): just assert after zones truly gc
        if (zone_used_bytes != 0) {
            auto tmp = zone_info;
            tmp.set_state(storage::IndexLog::FROZEN);
            tmp.set_frozen_time_ms(GetCurrentTimeInMs());
            index_->UpdateZoneInfo(tmp.zone_id(), tmp);
            LOG_ERROR("Bug, zone is still in use, rollback to frozen state.")
                .put("Old ZoneInfo", ss.str())
                .put("New ZoneInfo", tmp.ShortDebugString());
            continue;
        }

        page_store_->DestroyZone(zone_info.zone_id());
        LOG_INFO("Destroy zone")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Zone", zone_info.ShortDebugString());
        PARTITION_FAULT_INJECT("page_gc/purge_compacted_zones/after_destroy_zone");
    }

    uint64_t start_log_id = op_logger_->Start();
    uint64_t end_log_id = recycled_oplog_length_;
    BYTE_ASSERT(start_log_id <= end_log_id);
    BYTE_ASSERT(end_log_id <= index_->GetDumpedLogId());
    // truncate oplog
    uint64_t zone_start = start_log_id;
    while (LowerAlign(zone_start, FLAGS_storage_zone_size) <
           LowerAlign(end_log_id, FLAGS_storage_zone_size)) {
        uint32_t zone_id = zone_start / FLAGS_storage_zone_size;
        uint64_t zone_end =
            LowerAlign(zone_start + FLAGS_storage_zone_size, FLAGS_storage_zone_size);
        uint32_t zone_used_bytes = index_->GetZoneStats().GetOpLoggerUsedBytes(zone_id);
        if (zone_used_bytes != 0) {
            break;
        }
        op_logger_->Truncate(zone_end);
        LOG_INFO("Truncate log")
            .put("PartitionId", partition_->GetPartitionID())
            .put("ZoneId", zone_id)
            .put("Start", zone_start)
            .put("End", zone_end);
        PARTITION_FAULT_INJECT("page_gc/purge_compacted_zones/after_oplog_truncate");
        zone_start = zone_end;
    }
    return Status::OK();
}

bool PageGc::PickNextZone() {
    ScopedLatency latency(metrics_.pick_zone_latency->get());
    GcZoneContext picked_zone;
    picked_zone.utility = 1.1;

    uint64_t pagestore_total_bytes = 0;
    uint64_t pagestore_used_bytes = 0;
    auto pagestore_zones = page_store_->GetZoneInfos();
    // only handle frozen zones
    for (const auto& item : pagestore_zones) {
        const storage::IndexLog::ZoneInfo& zone_info = item.second;
        if (zone_info.state() != storage::IndexLog::FROZEN) {
            LOG_DEBUG("Zone not frozen")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Zone", zone_info.ShortDebugString());
            continue;
        }
        zone_metrics_.resize(std::max(zone_metrics_.size(), zone_info.zone_id() + 1UL));
        if (zone_metrics_[zone_info.zone_id()] == nullptr) {
            zone_metrics_[zone_info.zone_id()].reset(new ZoneGcMetrics());
            zone_metrics_[zone_info.zone_id()]->Init(metrics_manager_, zone_info.zone_id());
        }

        uint32_t zone_used_bytes =
            index_->GetZoneStats().GetPageStoreUsedBytes(zone_info.zone_id());
        double utility = zone_info.total_bytes() != 0
                             ? static_cast<double>(zone_used_bytes) / zone_info.total_bytes()
                             : 0.0;
        LOG_DEBUG("Page store zone info")
            .put("PartitionId", partition_->GetPartitionID())
            .put("UsedBytes", zone_used_bytes)
            .put("Utility", utility)
            .put("Zone", zone_info.ShortDebugString());

        pagestore_total_bytes += zone_info.total_bytes();
        pagestore_used_bytes += zone_used_bytes;
        zone_metrics_[zone_info.zone_id()]->total_bytes->get()->Set(zone_info.total_bytes());
        zone_metrics_[zone_info.zone_id()]->used_bytes->get()->Set(zone_used_bytes);
        zone_metrics_[zone_info.zone_id()]->utility->get()->Set(utility);
        // select the one with the minimum utility
        if (utility < picked_zone.utility) {
            picked_zone.zone_id = zone_info.zone_id();
            picked_zone.page_log = false;
            picked_zone.utility = utility;
            picked_zone.used_bytes = zone_used_bytes;
            picked_zone.total_bytes = zone_info.total_bytes();
        }
    }

    double page_store_utility =
        pagestore_total_bytes > 0
            ? static_cast<double>(pagestore_used_bytes) / static_cast<double>(pagestore_total_bytes)
            : 0.0;
    metrics_.page_store_total_bytes->get()->Set(pagestore_total_bytes);
    metrics_.page_store_used_bytes->get()->Set(pagestore_used_bytes);
    metrics_.page_store_utility->get()->Set(page_store_utility);

    uint64_t oplog_total_bytes = 0;
    uint64_t oplog_used_bytes = 0;
    BYTE_ASSERT(recycled_oplog_length_ >= op_logger_->Start());
    uint64_t start_log_id = recycled_oplog_length_;
    uint64_t end_log_id = index_->GetDumpedLogId();
    BYTE_ASSERT(start_log_id <= end_log_id);
    uint64_t zone_start = start_log_id;
    while (LowerAlign(zone_start, FLAGS_storage_zone_size) <
           LowerAlign(end_log_id, FLAGS_storage_zone_size)) {
        uint64_t zone_end =
            LowerAlign(zone_start + FLAGS_storage_zone_size, FLAGS_storage_zone_size);
        oplog_total_bytes += zone_end - zone_start;
        uint32_t zone_used_bytes =
            index_->GetZoneStats().GetOpLoggerUsedBytes(zone_start / FLAGS_storage_zone_size);
        oplog_used_bytes += zone_used_bytes;
        zone_start = zone_end;
    }

    double oplogger_utility = oplog_total_bytes > 0 ? static_cast<double>(oplog_used_bytes) /
                                                          static_cast<double>(oplog_total_bytes)
                                                    : 0.0;
    metrics_.oplogger_total_bytes->get()->Set(oplog_total_bytes);
    metrics_.oplogger_used_bytes->get()->Set(oplog_used_bytes);
    metrics_.oplogger_utility->get()->Set(oplogger_utility);

    uint64_t total_bytes = pagestore_total_bytes + oplog_total_bytes;
    uint64_t used_bytes = pagestore_used_bytes + oplog_used_bytes;
    // Overall space utility
    double utility =
        total_bytes > 0 ? static_cast<double>(used_bytes) / static_cast<double>(total_bytes) : 0.0;
    metrics_.total_bytes->get()->Set(total_bytes);
    metrics_.used_bytes->get()->Set(used_bytes);
    metrics_.utility->get()->Set(utility);

    LOG_DEBUG("Total utility")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Utility", utility)
        .put("PageStoreTotalBytes", pagestore_total_bytes)
        .put("OplogTotalBytes", oplog_total_bytes)
        .put("PageStoreUsedBytes", pagestore_used_bytes)
        .put("OplogUsedBytes", oplog_used_bytes);
    LOG_INFO_SAMPLE("Total utility")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Utility", utility)
        .put("PageStoreTotalBytes", pagestore_total_bytes)
        .put("OplogTotalBytes", oplog_total_bytes)
        .put("PageStoreUsedBytes", pagestore_used_bytes)
        .put("OplogUsedBytes", oplog_used_bytes);
    if (utility >= FLAGS_storage_gc_space_utility_threshold) {
        LOG_DEBUG("High utility, no need gc")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Utility", utility)
            .put("Threshold", FLAGS_storage_gc_space_utility_threshold);
        return false;
    }
    // only check the first zone from start_log_id
    if (LowerAlign(start_log_id, FLAGS_storage_zone_size) <
        LowerAlign(end_log_id, FLAGS_storage_zone_size)) {
        uint32_t zone_used_bytes =
            index_->GetZoneStats().GetOpLoggerUsedBytes(start_log_id / FLAGS_storage_zone_size);
        uint64_t zone_end =
            LowerAlign(start_log_id + FLAGS_storage_zone_size, FLAGS_storage_zone_size);
        double utility = static_cast<double>(zone_used_bytes) / (zone_end - start_log_id);
        if (utility < picked_zone.utility) {
            picked_zone.zone_id = start_log_id / FLAGS_storage_zone_size;
            picked_zone.page_log = true;
            picked_zone.used_bytes = zone_used_bytes;
            picked_zone.total_bytes = zone_end - start_log_id;
            picked_zone.utility = utility;
        }
    }

    if (picked_zone.zone_id == kInvalidZoneId) {
        LOG_DEBUG("No zone pick")
            .put("PartitionId", partition_->GetPartitionID())
            .put("StartLogId", start_log_id)
            .put("EndLogId", end_log_id);
        return false;
    }

    current_gc_zone_ = std::move(picked_zone);
    current_gc_zone_.iterator = index_->NewSlotIterator();
    current_gc_zone_.completed = false;
    current_gc_zone_.gc_cost = TimeCost();
    LOG_INFO("Gc started")
        .put("PartitionId", partition_->GetPartitionID())
        .put("ZoneId", current_gc_zone_.zone_id)
        .put("PageLog", current_gc_zone_.page_log)
        .put("UsedBytes", current_gc_zone_.used_bytes)
        .put("TotalBytes", current_gc_zone_.total_bytes)
        .put("Utility", current_gc_zone_.utility);
    return true;
}

bool PageGc::GcCurrentZone() {
    metrics_.current_zone_id->get()->Set(current_gc_zone_.zone_id);
    metrics_.gc_qps->get()->Increment();
    ScopedLatency latency(metrics_.gc_latency->get());

    BYTE_ASSERT(current_gc_zone_.zone_id != kInvalidZoneId);
    std::vector<SlotPageIndex> read_indexes;
    uint64_t total_bytes = 0;
    bool dirty_slot = false;
    auto func = [this, &read_indexes, &dirty_slot, &total_bytes](uint64_t slot_id,
                                                                  SlotNode* slot) -> bool {
        LOG_DEBUG("Gc Iterator")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id);
        std::vector<PageIndex> cur_indexes;
        bool page_dirty = false;
        for (auto& page : slot->GetPages()) {
            if (page.dirty) {
                page_dirty = true;
                continue;
            }
            if (page.page_in_log != current_gc_zone_.page_log) {
                continue;
            }
            // check if this page is in the current zone
            if (current_gc_zone_.page_log &&
                page.address / FLAGS_storage_zone_size == current_gc_zone_.zone_id) {
                cur_indexes.emplace_back(page);
            } else if (!current_gc_zone_.page_log &&
                       ExtractZoneId(page.address) == current_gc_zone_.zone_id) {
                cur_indexes.emplace_back(page);
            }
        }

        if (page_dirty) {
            LOG_DEBUG("Push dirty slot")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", slot_id);
            dirty_slot = true;
        }
        for (const auto& index : cur_indexes) {
            total_bytes += index.page_size;
            read_indexes.emplace_back(slot_id, index);
        }
        metrics_.gc_scan_slot_qps->get()->Increment();
        return total_bytes < FLAGS_storage_gc_max_bytes_per_round;
    };

    if (current_gc_zone_.used_bytes == 0) {
        current_gc_zone_.completed = true;
        LOG_INFO("Gc empty zone directly")
            .put("PartitionId", partition_->GetPartitionID())
            .put("ZoneId", current_gc_zone_.zone_id)
            .put("PageLog", current_gc_zone_.page_log)
            .put("UsedBytes", current_gc_zone_.used_bytes)
            .put("TotalBytes", current_gc_zone_.total_bytes)
            .put("Utility", current_gc_zone_.utility);
    } else {
        TimeCost scan_cost;
        current_gc_zone_.iterator->Seek(
            current_gc_zone_.cursor);  // pick up from the last iteration
        current_gc_zone_.completed =
            !current_gc_zone_.iterator->Scan(FLAGS_storage_gc_max_slots_per_round, func);
        LOG_DEBUG("Scan index")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Cursor", current_gc_zone_.cursor)
            .put("SlotCnt", read_indexes.size());
        metrics_.scan_slots_latency->get()->Set(scan_cost.GetElapsedInUs());
    }

    TimeCost dump_dirty_slots_cost;
    // commit dirty slots first
    if (dirty_slot) {
        Controller ctrl;
        SYNC_CALL(op_logger_->Commit, &ctrl);
        if (ctrl.status().IsStreamAbort()) {
            LOG_ERROR("Commit oplog failed")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", ctrl.status());
            return false;
        }
    }
    PARTITION_FAULT_INJECT("page_gc/gc_current_zone/after_slot_store_commit_dirty_slots");
    metrics_.dump_dirty_slots_latency->get()->Set(dump_dirty_slots_cost.GetElapsedInUs());

    TimeCost rewrite_pages_cost;
    // rewrite non-dirty pages in this zone
    if (!read_indexes.empty()) {
        Controller ctrl;
        std::vector<PageInfo> pages;
        SYNC_CALL(slot_store_->LoadPages, &ctrl, read_indexes, &pages);
        if (!ctrl.status().ok()) {
            LOG_ERROR("Read pages failed in gc")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", ctrl.status())
                .put("ZoneId", current_gc_zone_.zone_id);
            return false;
        }

        ctrl.Reset();
        SYNC_CALL(slot_store_->DumpPages, &ctrl, &pages);
        if (!ctrl.status().ok()) {
            LOG_WARNING("Failed dump pages")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", ctrl.status());
            return false;
        }

        // update index
        for (size_t i = 0; i < pages.size(); ++i) {
            index_->UpdateSlotPages(pages[i].header.slot_id(), pages[i].header.oplog_sequence(),
                                    {pages[i]});
        }
        ctrl.Reset();
        SYNC_CALL(index_->Commit, &ctrl);
        if (!ctrl.status().ok()) {
            LOG_WARNING("Failed dump pages")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", ctrl.status());
            return false;
        }
    }
    PARTITION_FAULT_INJECT("page_gc/gc_current_zone/after_slot_store_load_dump_pages");
    metrics_.rewrite_pages_latency->get()->Set(rewrite_pages_cost.GetElapsedInUs());
    metrics_.rewrite_throughput->get()->Add(total_bytes);

    current_gc_zone_.cursor = current_gc_zone_.iterator->GetCursor();
    LOG_DEBUG("Update cursor")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Cursor", current_gc_zone_.cursor);

    if (current_gc_zone_.completed) {
        LOG_INFO("Gc finished")
            .put("PartitionId", partition_->GetPartitionID())
            .put("ZoneId", current_gc_zone_.zone_id)
            .put("PageLog", current_gc_zone_.page_log);
        metrics_.gc_zone_latency->get()->Set(current_gc_zone_.gc_cost.GetElapsedInUs());
        if (!current_gc_zone_.page_log) {
            // this is part of the page store, then just recycle this zone
            auto zone_used_bytes =
                index_->GetZoneStats().GetPageStoreUsedBytes(current_gc_zone_.zone_id);
            std::stringstream ss;
            ss << "PartitionId: " << partition_->GetPartitionID()
               << ", ZoneId: " << current_gc_zone_.zone_id << ", UsedBytes: " << zone_used_bytes;
            BYTE_ASSERT(zone_used_bytes == 0) << ss.str();
            page_store_->RecycledZone(current_gc_zone_.zone_id);
        } else {
            // this is part of the oplog, then advance `recycled_oplog_length_` and truncate this
            // part later
            uint64_t length =
                (current_gc_zone_.zone_id + 1) * static_cast<uint64_t>(FLAGS_storage_zone_size);
            BYTE_ASSERT(recycled_oplog_length_ >= op_logger_->Start());
            BYTE_ASSERT(length > recycled_oplog_length_);
            recycled_oplog_length_ = length;
        }
        current_gc_zone_.zone_id = kInvalidZoneId;
    }
    return current_gc_zone_.completed;
}

}  // namespace partition
}  // namespace bcache2
