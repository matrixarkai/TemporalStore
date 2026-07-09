// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/index/index.h"

#include <byte/include/assert.h>

#include <algorithm>
#include <iterator>
#include <string>
#include <utility>

#include "common/logging.h"
#include "partition/allocator_manager.h"
#include "partition/metrics.h"
#include "partition/partition.h"
#include "partition/partition_fiu.h"
#include "partition/storage/page_store.h"
#include "partition/storage/slot_context_manager.h"
#include "partition/storage/slot_store.h"
#include "stream/log_based_util.h"

DECLARE_double(index_gc_usage_trigger);
DECLARE_uint64(index_gc_max_num_per_round);
DECLARE_uint64(index_gc_bytes_threshold);

namespace bcache2 {
namespace partition {

Index::Index(Partition* partition, AllocatorManager* allocator_manager,
             SlotContextManager* slot_context_manager, MetricsManager* metrics_manager)
    : partition_(partition),
      slot_context_manager_(slot_context_manager),
      slot_node_allocator_(allocator_manager->GetAllocator(AllocatorType::kSlotNode)),
      overhead_allocator_(allocator_manager->GetAllocator(AllocatorType::kIndexOverhead)),
      slot_map_(std::less<uint64_t>(),
                Allocator::StlWrapper<std::pair<const uint64_t, SlotNode>>(slot_node_allocator_)) {
    metrics_.Init(metrics_manager);
}

Index::~Index() {
    for (auto& it : slot_map_) {
        it.second.ClearObjects(SlotNode::WriteOptions(slot_node_allocator_));
        it.second.ClearPages(SlotNode::WriteOptions(slot_node_allocator_));
        it.second.ClearObjectTtl(SlotNode::WriteOptions(slot_node_allocator_));
    }
    slot_map_.clear();
}

Status Index::Load() {
    ScopedLatency latency(metrics_.load_latency->get());
    uint64_t start_record_id = stream_->Stat().start_record_id;
    stream::ScopedIterator iter = stream_->NewIterator(start_record_id, UINT64_MAX);
    Status status;
    // replay all index logs
    while ((status = iter->Next()).ok()) {
        absl::string_view data = iter->Data();
        storage::IndexLog log;
        if (!log.ParseFromArray(data.data(), data.size())) {
            return Status::DataLoss("Parse index item failed");
        }
        Status status = ReplayIndexLog(log, data.size());
        if (!status.ok() && !status.IsZoneChanged()) {
            LOG_ERROR("Failed to replay index log")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", status)
                .put("Log", log.ShortDebugString());
            return status;
        }
    }

    if (!status.IsOutOfRange()) {
        return status;
    }

    gc_scan_iterator_ = stream_->NewIterator(start_record_id, UINT64_MAX);
    LOG_INFO("Index load success")
        .put("PartitionId", partition_->GetPartitionID())
        .put("StartRecordId", start_record_id)
        .put("StreamLength", stream_->Stat().length);
    return Status::OK();
}

Status Index::ReplayIndexLog(const storage::IndexLog& log, size_t log_size) {
    LOG_DEBUG("Replay index log")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Log", log.ShortDebugString());

    if (current_sequence_ != 0 && log.sequence() <= current_sequence_) {
        LOG_DEBUG("Skip already replayed index log")
            .put("PartitionId", partition_->GetPartitionID())
            .put("CurrentSequence", current_sequence_)
            .put("LogSequence", log.sequence())
            .put("Log", log.ShortDebugString());
        return Status::OK();
    }
    if (current_sequence_ != 0 && current_sequence_ + 1 != log.sequence()) {
        LOG_ERROR("Data loss")
            .put("PartitionId", partition_->GetPartitionID())
            .put("CurrentSequence", current_sequence_)
            .put("LogSequence", log.sequence())
            .put("Log", log.ShortDebugString());
        return Status::DataLoss("sequence not match");
    }

    if (UNLIKELY(log.has_meta_item())) {
        // we'll not fill meta and data log same time
        BYTE_ASSERT(log.item_size() == 0) << log.ShortDebugString();
        BYTE_ASSERT(log.object_item_size() == 0) << log.ShortDebugString();
        return ApplyIndexMetaLog(log);
    }

    SlotNode* slot = GetOrCreateSlotWithStorage(log.slot_id());
    if (slot->Loading()) {
        // slot is loading and LoadSlot is asynchronous, so we
        // can not apply index log to avoid the consistency problem of
        // object page and meta
        LOG_DEBUG("Slot is loading")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", log.slot_id());
        return Status::FailedPrecondition("Slot is loading");
    }

    // trim redundant oplog and set slot version
    slot_context_manager_->TrimSlotLogs(log.slot_id(), log.oplog_sequence());
    slot_context_manager_->SetSlotVersion(
        log.slot_id(),
        std::max(slot_context_manager_->GetSlotVersion(log.slot_id()), log.oplog_sequence()));

    // apply log
    if (log.item_size() > 0) {
        Status status = ApplyIndexPageLog(slot, log);
        BYTE_ASSERT(status.ok()) << status;  // no need rollback
    }
    if (log.object_item_size() > 0) {
        Status status = ApplyIndexObjectLog(slot, log);  // object meta (only TTL for now)
        BYTE_ASSERT(status.ok()) << status;              // no need rollback
    }

    // clear slot dirty flags if there is no more oplog
    if (slot_context_manager_->GetSlotLogs(log.slot_id()).empty() && slot->Dirty()) {
        ClearSlotDirty(log.slot_id());
    }
    TryClearSlot(log.slot_id(), slot);

    avg_log_size_ << log_size + stream::RecordHeaderLength(log_size);
    avg_log_item_ << log.item_size() + log.object_item_size();
    current_sequence_ = log.sequence();
    return Status::OK();
}

Status Index::ApplyIndexMetaLog(const storage::IndexLog& log) {
    bool zone_changed = meta_.zone_version() != log.meta_item().zone_version();
    meta_.CopyFrom(log.meta_item());
    current_sequence_ = log.sequence();
    return zone_changed ? Status::ZoneChanged("") : Status::OK();
}

Status Index::ApplyIndexObjectLog(SlotNode* slot, const storage::IndexLog& log) {
    LOG_CALL_DEBUG().put("SlotId", log.slot_id()).put("Log", log.ShortDebugString());
    BYTE_ASSERT(log.object_item_size() > 0) << log.ShortDebugString();

    int meta_logs_num = slot_context_manager_->GetSlotDirtyMetaLogsNum(log.slot_id());
    if (meta_logs_num > 0) {
        // there is the newer oplog to update the slot meta(i.e. slot meta is dirty)
        // so we can't modify slot meta
        BYTE_ASSERT(slot->Dirty()) << log.ShortDebugString();
        BYTE_ASSERT(slot->MetaDirty()) << log.ShortDebugString();
        current_sequence_ = log.sequence();
        return Status::OK();
    }

    if (slot->MetaDirty()) {
        // there is no more oplog to update the object meta,
        // so we can clear the meta dirty safely
        ClearSlotMetaDirty(log.slot_id());
    }

    for (const auto& pair : slot->GetObjectTtls()) {
        OnObjectTtlLogInvalidated(slot, pair.first);
    }
    slot->ClearObjectTtl(SlotNode::WriteOptions(slot_node_allocator_));

    for (int i = 0; i < log.object_item_size(); ++i) {
        const storage::IndexLog::ObjectItem& item = log.object_item(i);
        slot->SetObjectTtl(SlotNode::WriteOptions(slot_node_allocator_), item.object_id(),
                           item.ttl());
        OnObjectTtlLogValidated(slot, item.object_id());
    }

    return Status::OK();
}

Status Index::ApplyIndexPageLog(SlotNode* slot, const storage::IndexLog& log) {
    LOG_CALL_DEBUG().put("SlotId", log.slot_id()).put("Log", log.ShortDebugString());
    BYTE_ASSERT(log.item_size() > 0) << log.ShortDebugString();

    for (int i = 0; i < log.item_size(); ++i) {
        const storage::IndexLog::IndexItem& item = log.item(i);

        // NOTE: page_id is always 0 in oplog
        // BYTE_ASSERT(item.page_id() == 0);

        int object_pages_num =
            slot_context_manager_->GetSlotObjectDirtyPagesNum(log.slot_id(), item.object_id());
        if (object_pages_num > 0) {
            // there is the newer oplog to update the object page(i.e. object page is dirty)
            // so we can't modify page
            BYTE_ASSERT(slot->Dirty()) << log.ShortDebugString();
            continue;
        }

        PageIndex page;
        Status status = slot->FindPage(item.object_id(), item.page_id(), &page);
        if (status.ok()) {
            OnPageWillDelete(page);  // delete this page logically
        }
        if (item.deleted()) {
            slot->DeletePage(SlotNode::WriteOptions(slot_node_allocator_), item.object_id(),
                             item.page_id());
            continue;
        }

        page.object_id = item.object_id();
        page.model_id = item.model_id();
        page.page_id = item.page_id();
        page.address = item.address();
        page.page_size = item.size();
        page.page_in_log = item.in_log();
        page.dirty = false;  // there is no more oplog to update the page, so we can clear the page
                             // dirty safely
        OnPageAppended(page);  // add it back logically

        Layout::WriteOptions opts(slot_node_allocator_);
        opts.update_if_exist = true;
        status = slot->NewPage(opts, page);  // possibly replace this old page
        BYTE_ASSERT(status.ok()) << status;
    }

    return Status::OK();
}

void Index::Commit(Controller* ctrl, Closure<void>* callback) { stream_->Commit(ctrl, callback); }

void Index::TryGc(bool* dirty_slot, uint64_t* truncate_id) {
    *dirty_slot = false;
    ScopedLatency latency(metrics_.gc_latency->get());
    metrics_.gc_qps->get()->Increment();

    double total_length = stream_->Stat().usage_bytes;
    double valid_length =
        1.0 * valid_item_count_ / avg_log_item_.average(1.0) * avg_log_size_.average(1.0);
    gc_utility_ = total_length == 0 ? 1.0 : 1.0 * valid_length / total_length;

    // only if there are enough garbage items
    if (gc_utility_ > FLAGS_index_gc_usage_trigger ||
        total_length - valid_length < FLAGS_index_gc_bytes_threshold) {
        LOG_DEBUG("No need gc")
            .put("PartitionId", partition_->GetPartitionID())
            .put("total_index_log_length", total_length)
            .put("valid_index_log_length", valid_length);
        return;
    }

    for (uint64_t i = 0; i < FLAGS_index_gc_max_num_per_round; ++i) {
        Status status = gc_scan_iterator_->Next();
        if (!status.ok()) {
            if (!status.IsOutOfRange()) {
                LOG_WARNING("index gc scan failed")
                    .put("PartitionId", partition_->GetPartitionID())
                    .put("status", status.ToString());
            }
            return;
        }

        absl::string_view data = gc_scan_iterator_->Data();
        uint64_t log_id = gc_scan_iterator_->Id();
        storage::IndexLog log;
        if (!log.ParseFromArray(data.data(), data.size())) {
            LOG_ERROR("Index GC parse log failed")
                .put("PartitionId", partition_->GetPartitionID())
                .put("log_id", log_id)
                .put("data length", data.size());
            BYTE_ASSERT(false) << partition_->GetPartitionID();
        }

        metrics_.gc_scan_item_qps->get()->Increment();
        metrics_.gc_scan_throughput->get()->Add(data.size());
        LOG_DEBUG("Scan index log")
            .put("PartitionId", partition_->GetPartitionID())
            .put("LogId", log_id)
            .put("Size", data.size())
            .put("Log", log.ShortDebugString());

        if (UNLIKELY(log.has_meta_item())) {
            BYTE_ASSERT(log.meta_item().version() <= meta_.version())
                << meta_.ShortDebugString() << " " << log.ShortDebugString();
            if (log.meta_item().version() < meta_.version()) {  // ignore older meta versions
                *truncate_id = log_id + data.size();
                continue;
            }
            // the same version: do nothing but just generate a new meta item
            OnMetaUpdate();
            PARTITION_FAULT_INJECT("index_gc/try_gc/after_meta_update");
            continue;
        }

        auto slot_iter = slot_map_.find(log.slot_id());
        if (slot_iter == slot_map_.end()) {
            *truncate_id = log_id + data.size();
            continue;
        }

        SlotNode* slot = &(slot_iter->second);
        storage::IndexLog new_log;
        new_log.set_version(kIndexLogVersion);
        new_log.set_slot_id(log.slot_id());
        new_log.set_oplog_sequence(log.oplog_sequence());

        if (log.object_item_size() > 0) {
            if (slot->MetaDirty()) {
                *dirty_slot = true;
                LOG_DEBUG("Slot meta dirty")
                    .put("PartitionId", partition_->GetPartitionID())
                    .put("SlotId", log.slot_id());
            }
            for (int i = 0; i < log.object_item_size(); ++i) {
                if (log.object_item(i).ttl() !=
                    slot->GetObjectTtl(log.object_item(i).object_id())) {
                    // there are newer log entries, so this one can be discarded safely
                    continue;
                }
                // this object log should be kept
                storage::IndexLog::ObjectItem* rewrite_item = new_log.add_object_item();
                rewrite_item->CopyFrom(log.object_item(i));
            }
        }

        for (int i = 0; i < log.item_size(); ++i) {
            const storage::IndexLog::IndexItem& item = log.item(i);
            if (item.deleted()) {
                continue;
            }

            PageIndex page;
            Status status = slot->FindPage(item.object_id(), item.page_id(), &page);
            if (!status.ok()) {
                continue;
            }
            // the page is dirty, so this slot should be dumped later
            if (page.dirty) {
                *dirty_slot = true;
                LOG_DEBUG("Page dirty")
                    .put("PartitionId", partition_->GetPartitionID())
                    .put("SlotId", log.slot_id())
                    .put("ObjectId", static_cast<uint16_t>(page.object_id))
                    .put("PageId", page.page_id)
                    .put("Address", page.address);
                continue;
            }
            // mismatch? so this log is no longer relevant
            if (page.address != item.address() || page.page_size != item.size() ||
                page.page_in_log != item.in_log()) {
                continue;
            }

            storage::IndexLog::IndexItem* rewrite_item = new_log.add_item();
            rewrite_item->CopyFrom(item);
        }

        if (new_log.item_size() > 0 || new_log.object_item_size() > 0) {
            // rewrite log
            metrics_.gc_rewrite_item_qps->get()->Increment();
            metrics_.gc_rewrite_throughput->get()->Add(new_log.ByteSize());
            WriteLog(std::move(new_log));
        }
        PARTITION_FAULT_INJECT("index_gc/try_gc/after_rewrite_index_log");

        *truncate_id = log_id + data.size();
    }
}

void Index::Truncate(uint64_t truncate_id) { stream_->Truncate(truncate_id); }

// this slot must not be dirty
Status Index::EvictSlot(uint64_t slot_id) {
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr && slot->InMemory()) << slot_id;

    slot->ClearObjects(SlotNode::WriteOptions(slot_node_allocator_));
    slot->SetInMemory(false);
    LOG_DEBUG("Evict slot success")
        .put("PartitionId", partition_->GetPartitionID())
        .put("slot_id", slot_id);

    return Status::OK();
}

std::unique_ptr<Index::SlotIterator> Index::NewSlotIterator() {
    return std::unique_ptr<SlotIterator>(new SlotIterator(&slot_map_));
}

std::unique_ptr<Index::IndexLogIterator> Index::NewIndexLogIterator(uint64_t log_id) {
    return std::unique_ptr<IndexLogIterator>(
        new IndexLogIterator(stream_->NewIterator(log_id, UINT64_MAX)));
}

void Index::MarkSlotPageDirty(uint64_t slot_id, uint64_t log_id, uint64_t log_sequence,
                              const std::vector<PageInfo>& pages) {
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;
    for (const auto& item : pages) {
        BYTE_ASSERT(item.header.page_in_log()) << item.header.ShortDebugString();

        PageIndex page;
        Status status = slot->FindPage(item.header.object_id(), item.header.page_id(), &page);
        if (status.ok()) {
            OnPageWillDelete(page);
        }
        page.object_id = item.header.object_id();
        page.model_id = item.header.model_id();
        page.page_id = item.header.page_id();
        page.page_in_log = true;
        page.page_size = item.size;
        page.address = item.address;
        page.dirty = true;

        SlotNode::WriteOptions opts(slot_node_allocator_);
        opts.update_if_exist = true;
        status = slot->NewPage(opts, page);
        BYTE_ASSERT(status.ok()) << status;
        if (page.page_size != 0) {
            OnPageAppended(page);
        }
    }
    GatherDirtySlot(slot_id, slot, log_id, log_sequence);
}

void Index::MarkSlotObjectDeleted(uint64_t slot_id, uint64_t log_id, uint64_t log_sequence,
                                  uint16_t object_id) {
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;
    SlotNode::WriteOptions opt(slot_node_allocator_);
    for (auto& page : slot->GetPages()) {
        if (page.object_id == object_id) {
            OnPageWillDelete(page);  // this page is deleted logically
            // Actual page transforms to a mark-deleted page in oplog.
            page.page_size = 0;
            page.page_in_log = true;
            page.address = log_id;
            page.dirty = true;
            Status status = slot->UpdatePage(opt, page.object_id, page.page_id, page);
            BYTE_ASSERT(status.ok()) << status;
        }
    }

    slot->SetObjectTtl(opt, object_id, 0);  // 0 TTL
    OnObjectTtlLogInvalidated(slot, object_id);

    slot->SetMetaDirty(true);
    GatherDirtySlot(slot_id, slot, log_id, log_sequence);
}

void Index::GetSlotMarkedPages(uint64_t slot_id, std::vector<PageIndex>* pages) {
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;
    for (const auto& page : slot->GetPages()) {
        if (page.dirty) {
            pages->push_back(page);
        }
    }
}

void Index::GetSlotMarkedPagesAndMetas(uint64_t slot_id, std::vector<PageIndex>* pages,
                                       std::vector<storage::IndexLog::ObjectItem>* metas) {
    pages->clear();
    metas->clear();
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;
    for (const auto& page : slot->GetPages()) {
        if (page.dirty) {
            pages->push_back(page);
        }
    }
    if (slot->MetaDirty()) {
        for (auto& pair : slot->GetObjectTtls()) {
            storage::IndexLog::ObjectItem item;
            item.set_object_id(pair.first);
            item.set_ttl(pair.second);
            metas->push_back(item);
        }
    }
}

// invoked after log replay or slot dump completes
void Index::ClearSlotDirty(uint64_t slot_id) {
    LOG_DEBUG("Clear slot dirty")
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", slot_id);
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;
    SlotNode::WriteOptions opt(slot_node_allocator_);
    for (auto& page : slot->GetPages()) {
        page.dirty = false;
        Status status = slot->UpdatePage(opt, page.object_id, page.page_id, page);
        BYTE_ASSERT(status.ok()) << status;
    }
    slot->SetMetaDirty(false);
    slot->SetDirty(false);
    slot_context_manager_->PopDirtySlot(slot_id);
}

void Index::ClearSlotMetaDirty(uint64_t slot_id) {
    LOG_DEBUG("Clear slot meta dirty")
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", slot_id);
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;
    SlotNode::WriteOptions opt(slot_node_allocator_);
    slot->SetMetaDirty(false);
}

void Index::UpdateSlotPages(uint64_t slot_id, uint64_t log_sequence,
                            const std::vector<PageInfo>& pages) {
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;

    storage::IndexLog log;
    log.set_version(kIndexLogVersion);
    log.set_slot_id(slot_id);
    BYTE_ASSERT(log_sequence != 0) << log_sequence;
    log.set_oplog_sequence(log_sequence);
    UpdatePagesInternal(&log, slot_id, slot, log_sequence, pages);
    WriteLog(std::move(log));

    TryClearSlot(slot_id, slot);
}

void Index::UpdateSlotPagesAndMetas(uint64_t slot_id, uint64_t last_log_sequence,
                                    const std::vector<PageInfo>& pages,
                                    const std::vector<storage::IndexLog::ObjectItem>& metas) {
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;

    storage::IndexLog log;
    log.set_version(kIndexLogVersion);
    log.set_slot_id(slot_id);
    BYTE_ASSERT(last_log_sequence != 0) << slot_id << " " << last_log_sequence;
    log.set_oplog_sequence(last_log_sequence);
    UpdatePagesInternal(&log, slot_id, slot, last_log_sequence, pages);
    WriteObjectMeta(&log, slot_id, slot, metas, last_log_sequence);
    WriteLog(std::move(log));

    TryClearSlot(slot_id, slot);
}

void Index::SetDumpedLogId(uint64_t dumped_oplog_id) {
    meta_.set_start_oplog_id(dumped_oplog_id);
    OnMetaUpdate();
}

bool Index::GetZoneInfo(uint32_t zone_id, storage::IndexLog::ZoneInfo* zone_info) {
    auto it = meta_.zones().find(zone_id);
    if (it == meta_.zones().end()) {
        return false;
    }
    *zone_info = it->second;
    return true;
}

void Index::UpdateZoneInfo(uint32_t zone_id, storage::IndexLog::ZoneInfo zone_info) {
    meta_.set_zone_version(meta_.zone_version() + 1);
    (*meta_.mutable_zones())[zone_id] = std::move(zone_info);
    OnMetaUpdate();
}

void Index::DeleteZoneInfo(uint32_t zone_id) {
    meta_.mutable_zones()->erase(zone_id);
    meta_.set_zone_version(meta_.zone_version() + 1);
    OnMetaUpdate();
}

IndexInfo Index::GetInfo() const {
    IndexInfo info;
    *info.mutable_slot_node_alloc_stats() = std::move(slot_node_allocator_->GetStats());
    *info.mutable_overhead_alloc_stats() = std::move(overhead_allocator_->GetStats());
    info.set_slot_count(slot_map_.size());
    info.set_dirty_slot_count(slot_context_manager_->DirtySlotsNum());
    info.mutable_meta()->CopyFrom(meta_);
    *info.mutable_stream_info() = stream_->GetInfo();
    uint64_t durable_sequence = 0;
    if (info.stream_info().blob_infos_size() > 0) {
        durable_sequence = info.stream_info().blob_infos(
            info.stream_info().blob_infos_size() - 1).end_record_sequence();
    }
    info.set_current_sequence(current_sequence_ < durable_sequence
                                  ? current_sequence_
                                  : durable_sequence);
    return info;
}

Status Index::RestoreInfo(const IndexInfo& index_info) {
    return stream_->RestoreInfo(index_info.stream_info());
}

// mark this slot dirty, and update related components
void Index::GatherDirtySlot(uint64_t slot_id, SlotNode* slot, uint64_t log_id,
                            uint64_t log_sequence) {
    LOG_DEBUG("Mark slot dirty")
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", slot_id)
        .put("LogId", log_id)
        .put("LogSequence", log_sequence);
    if (!slot->Dirty()) {
        slot->SetDirty(true);
        slot_context_manager_->SetSlotFirstDirtyLogId(slot_id, log_id);
        slot_context_manager_->PushDirtySlot(slot_id);
        LOG_DEBUG("First mark slot dirty")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id);
    } else {
        metrics_.dirty_slot_merge_count->get()->Increment();
    }
    slot_context_manager_->SetSlotLastLogSequence(slot_id, log_sequence);
}

void Index::UpdatePagesInternal(storage::IndexLog* log, uint64_t slot_id, SlotNode* slot,
                                uint64_t log_sequence, const std::vector<PageInfo>& pages) {
    // write to the index log first
    for (size_t i = 0; i < pages.size(); ++i) {
        PageIndex page;
        Status status =
            slot->FindPage(pages[i].header.object_id(), pages[i].header.page_id(), &page);
        if (status.ok() && page.dirty) {
            // dirty, can't update
            // return directly due to UpdatePagesInternal is ATOMIC
            return;
        }

        storage::IndexLog::IndexItem* item = log->add_item();
        if (pages[i].size != 0) {
            item->set_object_id(pages[i].header.object_id());
            item->set_page_id(pages[i].header.page_id());
            item->set_address(pages[i].address);
            item->set_size(pages[i].size);
            item->set_in_log(pages[i].header.page_in_log());
            item->set_model_id(pages[i].header.model_id());
        } else {
            item->set_object_id(pages[i].header.object_id());
            item->set_page_id(pages[i].header.page_id());
            item->set_deleted(true);
            item->set_model_id(pages[i].header.model_id());
        }
    }

    LOG_DEBUG("Update page index")
        .put("PartitionId", partition_->GetPartitionID())
        .put("SlotId", slot_id)
        .put("OplogSequence", log_sequence)
        .put("PageIndex", log->ShortDebugString());

    // then do the actual update
    for (size_t i = 0; i < pages.size(); ++i) {
        PageIndex page;
        Status status =
            slot->FindPage(pages[i].header.object_id(), pages[i].header.page_id(), &page);
        BYTE_ASSERT(!page.dirty) << pages[i].header.object_id() << " " << pages[i].header.page_id();
        if (status.ok()) {
            OnPageWillDelete(page);
        }

        if (pages[i].size != 0) {
            page.object_id = pages[i].header.object_id();
            page.model_id = pages[i].header.model_id();
            page.page_id = pages[i].header.page_id();
            page.page_size = pages[i].size;
            page.page_in_log = pages[i].header.page_in_log();
            page.page_size = pages[i].size;
            page.address = pages[i].address;
            SlotNode::WriteOptions opts(slot_node_allocator_);
            opts.update_if_exist = true;
            status = slot->NewPage(opts, page);
            BYTE_ASSERT(status.ok()) << status;
            OnPageAppended(page);
        } else {
            slot->DeletePage(SlotNode::WriteOptions(slot_node_allocator_),
                             pages[i].header.object_id(), pages[i].header.page_id());
        }
    }
}

void Index::WriteObjectMeta(storage::IndexLog* log, uint64_t slot_id, SlotNode* slot,
                            const std::vector<storage::IndexLog::ObjectItem>& metas,
                            uint64_t log_sequence) {
    if (metas.empty()) {
        return;
    }

    for (const auto& item : metas) {
        log->add_object_item()->CopyFrom(item);
    }

    if (slot->MetaDirty()) {
        return;
    }

    for (const auto& item : metas) {
        BYTE_ASSERT(slot->GetObjectTtl(item.object_id()) == item.ttl())
            << "ttl not match, slot ttl=" << slot->GetObjectTtl(item.object_id())
            << ", ttl=" << item.ttl() << ", slot=" << slot_id;
    }
}

void Index::ReapMetrics() const {
    metrics_.slot_count->get()->Set(slot_map_.size());
    metrics_.dirty_slot_count->get()->Set(slot_context_manager_->DirtySlotsNum());
    metrics_.avg_log_size->get()->Set(avg_log_size_.average(1.0));
    metrics_.avg_log_item->get()->Set(avg_log_item_.average(1.0));
    metrics_.valid_item_count->get()->Set(valid_item_count_);
    metrics_.gc_utility->get()->Set(gc_utility_);

    double slot_get_qps = metrics_.slot_get_qps->get()->GetValue();
    if (LIKELY(slot_get_qps > 1.0)) {
        metrics_.slot_hit_ratio->get()->Set(metrics_.slot_hit_qps->get()->GetValue() /
                                            slot_get_qps);
        metrics_.slot_in_memory_ratio->get()->Set(metrics_.slot_in_memory_qps->get()->GetValue() /
                                                  slot_get_qps);
    }

    stream_->ReapMetrics();
}

void Index::TryClearSlot(uint64_t slot_id, SlotNode* slot) {
    if (!slot->Dirty() && slot->GetPageNum() == 0 && slot->GetObjectNum() == 0 && !slot->HasTtl()) {
        slot->ClearObjectTtl(SlotNode::WriteOptions(slot_node_allocator_));
        slot_map_.erase(slot_id);
        slot_context_manager_->ResetSlotContext(slot_id);
        LOG_DEBUG("Erase slot")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id);
    }
}

void Index::OnMetaUpdate() {
    meta_.set_version(meta_.version() + 1);
    meta_.set_timestamp_ms(GetCurrentTimeInMs());
    storage::IndexLog index_log;
    index_log.set_version(kIndexLogVersion);
    auto meta_item = index_log.mutable_meta_item();
    meta_item->CopyFrom(meta_);

    LOG_DEBUG("Update index meta")
        .put("PartitionId", partition_->GetPartitionID())
        .put("MetaVersion", meta_.version())
        .put("StartOplogId", meta_.start_oplog_id());
    WriteLog(std::move(index_log));
}

void Index::WriteLog(storage::IndexLog log) {
    log.set_sequence(++current_sequence_);  // increment the sequence
    log.set_timestamp_ms(GetCurrentTimeInMs());
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("Log", log.ShortDebugString());
    std::string log_data;
    BYTE_ASSERT(log.SerializeToString(&log_data)) << log_data.size();
    metrics_.write_throughput->get()->Add(log_data.size());
    if (!log.has_meta_item()) {
        // index meta log should not be counted
        avg_log_size_ << log_data.size() + stream::RecordHeaderLength(log_data.size());
        avg_log_item_ << log.item_size() + log.object_item_size();
    }
    uint64_t id = 0;
    stream_->Append(std::move(log_data), &id);
}

void Index::OnPageWillDelete(const PageIndex& page) {
    page.page_in_log
        ? zone_stats_.UpdateOpLoggerStat(page.address, -static_cast<int32_t>(page.page_size))
        : zone_stats_.UpdatePageStoreStat(page.address, -static_cast<int32_t>(page.page_size));
    auto zone_id =
        page.page_in_log ? page.address / FLAGS_storage_zone_size : ExtractZoneId(page.address);
    auto used_bytes = page.page_in_log ? zone_stats_.GetOpLoggerUsedBytes(zone_id)
                                       : zone_stats_.GetPageStoreUsedBytes(zone_id);
    valid_item_count_ -= (page.page_size != 0);
    LOG_DEBUG("Delete page")
        .put("PartitionId", partition_->GetPartitionID())
        .put("ObjectId", page.object_id)
        .put("PageId", page.page_id)
        .put("PageInLog", page.page_in_log)
        .put("ZoneId", zone_id)
        .put("PageAddress", page.address)
        .put("PageSize", page.page_size)
        .put("UsedBytes", used_bytes);
}

void Index::OnPageAppended(const PageIndex& page) {
    page.page_in_log
        ? zone_stats_.UpdateOpLoggerStat(page.address, static_cast<int32_t>(page.page_size))
        : zone_stats_.UpdatePageStoreStat(page.address, static_cast<int32_t>(page.page_size));
    auto zone_id =
        page.page_in_log ? page.address / FLAGS_storage_zone_size : ExtractZoneId(page.address);
    auto used_bytes = page.page_in_log ? zone_stats_.GetOpLoggerUsedBytes(zone_id)
                                       : zone_stats_.GetPageStoreUsedBytes(zone_id);
    valid_item_count_ += (page.page_size != 0);
    LOG_DEBUG("Append page")
        .put("PartitionId", partition_->GetPartitionID())
        .put("ObjectId", page.object_id)
        .put("PageId", page.page_id)
        .put("PageInLog", page.page_in_log)
        .put("ZoneId", zone_id)
        .put("PageAddress", page.address)
        .put("PageSize", page.page_size)
        .put("UsedBytes", used_bytes);
}

// returns true if the iterator hasn't reach the end
bool Index::SlotIterator::Scan(int count,
                               std::function<bool(uint64_t slot_id, SlotNode* slot)> func) {
    SlotMap::iterator it = slot_map_->lower_bound(last_key_);
    bool complete = false;
    while (count-- > 0 && !complete && it != slot_map_->end()) {
        complete = !func(it->first, &it->second);
        last_key_ = it->first + 1;
        ++it;
    }
    return it != slot_map_->end();
}

std::vector<std::pair<uint64_t, SlotNode*>> Index::SlotIterator::Scan(int count) {
    std::vector<std::pair<uint64_t, SlotNode*>> slots;
    Scan(count, [&slots](uint64_t slot_id, SlotNode* slot) -> bool {
        slots.emplace_back(slot_id, slot);
        return true;
    });
    return slots;
}

SlotMap::key_type Index::SlotIterator::GetCursor() const { return last_key_; }

void Index::SlotIterator::Seek(SlotMap::key_type cursor) { last_key_ = cursor; }

Status Index::IndexLogIterator::Next() {
    Status status = iter_->Next();
    if (status.ok()) {
        log_id_ = iter_->Id();
        log_size_ = iter_->Data().size();
        if (!log_.ParseFromArray(iter_->Data().data(), iter_->Data().size())) {
            return Status::InvalidArgument("Invalid protocol");
        }
    }
    return status;
}

}  // namespace partition
}  // namespace bcache2
