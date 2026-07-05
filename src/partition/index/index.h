// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif
#include <bvar/bvar.h>
#include <byte/container/intrusive_list.h>
#include <byte/governance/token_bucket.h>
#include <byte/include/macros.h>
#include <stdint.h>

#include <algorithm>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/metrics.h"
#include "common/ring_array.h"
#include "common/status.h"
#include "common/time.h"
#include "partition/index/page_index.h"
#include "partition/index/slot_node.h"
#include "partition/storage/page_store.h"
#include "partition/storage/zone.h"
#include "stream/stream.h"

DECLARE_int32(storage_zone_size);

namespace bcache2 {
namespace partition {

const uint32_t kIndexLogVersion = 1UL;

class Partition;
class AllocatorManager;
class SlotContextManager;

using DirtySlots = RingArray<uint64_t, Allocator::StlWrapper<uint64_t>>;
using SlotMap = std::map<uint64_t, SlotNode, std::less<uint64_t>,
                         Allocator::StlWrapper<std::pair<const uint64_t, SlotNode>>>;

class Index {
 public:
    struct Options {
        stream::Stream* stream = nullptr;
    };

    enum class AccessRecordType {
        kLru,
        kLfu,
    };

    class SlotIterator;
    class IndexLogIterator;

    Index(Partition* partition, AllocatorManager* allocator_manager,
          SlotContextManager* slot_context_manager, MetricsManager* metrics_manager);
    virtual ~Index();

    void UpdateAccessRecordType(AccessRecordType type) { access_record_type_ = type; }

    // replay log ATOMICALLY
    Status ReplayIndexLog(const storage::IndexLog& log, size_t log_size);

    // Persistence management
    void Init(Options opts) { stream_.reset(opts.stream); }

    Status Load();
    void Close(Closure<void>* callback) {
        if (stream_) {
            stream_->Close(callback);
        } else if (callback) {
            callback->Run();
        }
    }
    void Commit(Controller* ctrl, Closure<void>* callback);
    void TryGc(bool* dirty_slot, uint64_t* truncate_id);
    void Truncate(uint64_t id);

    // Slot management
    SlotNode* GetOrCreateSlotWithStorage(uint64_t slot_id) {
        return GetOrCreateSlotInternal(slot_id, /* in_memory = */ false);
    }
    SlotNode* GetOrCreateSlot(uint64_t slot_id) {
        return GetOrCreateSlotInternal(slot_id, /* in_memory = */ true);
    }
    SlotNode* GetSlot(uint64_t slot_id);
    Status EvictSlot(uint64_t slot_id);
    std::unique_ptr<SlotIterator> NewSlotIterator();
    std::unique_ptr<IndexLogIterator> NewIndexLogIterator(uint64_t log_id);
    stream::ScopedIterator NewRawStreamIterator(uint64_t start_id, uint64_t end_id) {
        return stream_->NewIterator(start_id, end_id);
    }
    void ReadRawStream(Controller* ctrl, uint64_t id, void* data, size_t size,
                       Closure<void>* callback) {
        stream_->Read(ctrl, id, data, size, callback);
    }
    std::vector<PageIndex> GetSlotPages(uint64_t slot_id, bool with_dirty);
    void TouchSlot(uint64_t slot_id);

    // Index dirty management
    void MarkSlotDataDirty(uint64_t slot_id, uint64_t log_id, uint64_t log_sequence);
    void MarkSlotPageDirty(uint64_t slot_id, uint64_t log_id, uint64_t log_sequence,
                           const std::vector<PageInfo>& pages);
    void MarkSlotObjectTtlDirty(uint64_t slot_id, uint64_t log_id, uint64_t log_sequence,
                                uint64_t object_id, uint64_t ttl);
    void MarkSlotObjectDeleted(uint64_t slot_id, uint64_t log_id, uint64_t log_sequence,
                               uint16_t object_id);
    void GetSlotMarkedPages(uint64_t slot_id, std::vector<PageIndex>* pages);
    void GetSlotMarkedPagesAndMetas(uint64_t slot_id, std::vector<PageIndex>* pages,
                                    std::vector<storage::IndexLog::ObjectItem>* metas);
    void ClearSlotDirty(uint64_t slot_id);
    void ClearSlotMetaDirty(uint64_t slot_id);
    void UpdateSlotPages(uint64_t slot_id, uint64_t log_sequence,
                         const std::vector<PageInfo>& pages);
    void UpdateSlotPagesAndMetas(uint64_t slot_id, uint64_t log_sequence,
                                 const std::vector<PageInfo>& pages,
                                 const std::vector<storage::IndexLog::ObjectItem>& metas);

    // Meta info management
    const storage::IndexLog::MetaItem& MetaInfo() const { return meta_; }
    uint64_t GetDumpedLogId() const { return meta_.start_oplog_id(); }
    void SetDumpedLogId(uint64_t dumped_oplog_id);
    uint64_t GetZoneVersion() { return meta_.zone_version(); }
    bool GetZoneInfo(uint32_t zone_id, storage::IndexLog::ZoneInfo* zone_info);
    void UpdateZoneInfo(uint32_t zone_id, storage::IndexLog::ZoneInfo zone_info);
    void DeleteZoneInfo(uint32_t zone_id);

    void OnMetaUpdate();
    void ReapMetrics() const;

    void UpdateConfig(const Config& config) { stream_->UpdateConfig(config.stream_config()); }

    IndexInfo GetInfo() const;
    Status RestoreInfo(const IndexInfo& index_info);

    const ZoneStats& GetZoneStats() const { return zone_stats_; }
    uint64_t LogLength() const { return stream_->Stat().length; }
    uint64_t CurrentSequence() const { return current_sequence_; }
    IndexMetrics* MutableMetrics() { return &metrics_; }

 private:
    Status ApplyIndexMetaLog(const storage::IndexLog& log);
    Status ApplyIndexObjectLog(SlotNode* slot, const storage::IndexLog& log);
    Status ApplyIndexPageLog(SlotNode* slot, const storage::IndexLog& log);
    SlotNode* GetOrCreateSlotInternal(uint64_t slot_id, bool in_memory);

    void GatherDirtySlot(uint64_t slot_id, SlotNode* slot, uint64_t log_id, uint64_t log_sequence);
    void UpdatePagesInternal(storage::IndexLog* log, uint64_t slot_id, SlotNode* slot,
                             uint64_t log_sequence, const std::vector<PageInfo>& pages);
    void WriteObjectMeta(storage::IndexLog* log, uint64_t slot_id, SlotNode* slot,
                         const std::vector<storage::IndexLog::ObjectItem>& metas,
                         uint64_t log_sequence);
    void TryClearSlot(uint64_t slot_id, SlotNode* slot);
    void WriteLog(storage::IndexLog log);

    uint32_t GenerateAccessRecord(uint32_t access_record) const;
    void OnPageWillDelete(const PageIndex& page);

    void OnPageAppended(const PageIndex& page);

    // Index ttl log item is/will be validated when index log replayed
    // or object ttl marked dirty(loggically).
    void OnObjectTtlLogValidated(SlotNode* slot, uint64_t object_id) {
        valid_item_count_ += (slot->GetObjectTtl(object_id) == 0);
    }

    // Deleting object invalidate the corresponding index log item,
    // part of which is logical (not dumped yet)
    void OnObjectTtlLogInvalidated(SlotNode* slot, uint64_t object_id) {
        valid_item_count_ -= (slot->GetObjectTtl(object_id) != 0);
    }

    std::vector<PageInfo> FilterPages(SlotNode* slot, const std::vector<PageInfo>& pages);

    Partition* partition_ = nullptr;
    SlotContextManager* slot_context_manager_ = nullptr;
    Allocator* slot_node_allocator_ = nullptr;
    Allocator* overhead_allocator_ = nullptr;

    SlotMap slot_map_;  // in-memory slot map
    std::unique_ptr<stream::Stream> stream_;

    ZoneStats zone_stats_;

    AccessRecordType access_record_type_ = AccessRecordType::kLru;

    storage::IndexLog::MetaItem meta_;

    bvar::IntRecorder avg_log_size_;
    bvar::IntRecorder avg_log_item_;
    uint64_t valid_item_count_ = 0;
    double gc_utility_ = 1.0;
    stream::ScopedIterator gc_scan_iterator_;

    uint64_t current_sequence_ = 0;

    IndexMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(Index);
};

class Index::SlotIterator {
 public:
    explicit SlotIterator(SlotMap* slot_map)
        : slot_map_(slot_map), last_key_(SlotMap::key_type()) {}

    bool Scan(int count, std::function<bool(uint64_t slot_id, SlotNode* slot)> func);
    std::vector<std::pair<uint64_t, SlotNode*>> Scan(int count);

    SlotMap::key_type GetCursor() const;
    void Seek(SlotMap::key_type cursor);

 private:
    SlotMap* slot_map_ = nullptr;
    SlotMap::key_type last_key_ = SlotMap::key_type();

    DISALLOW_COPY_AND_ASSIGN(SlotIterator);
};

class Index::IndexLogIterator {
 public:
    explicit IndexLogIterator(stream::ScopedIterator iter) : iter_(std::move(iter)) {}
    Status Next();
    uint64_t GetLogId() const { return log_id_; }
    size_t GetLogSize() const { return log_size_; }
    const storage::IndexLog& GetLog() const { return log_; }

 private:
    stream::ScopedIterator iter_;
    uint64_t log_id_ = 0;
    size_t log_size_ = 0;
    storage::IndexLog log_;

    DISALLOW_COPY_AND_ASSIGN(IndexLogIterator);
};

inline SlotNode* Index::GetSlot(uint64_t slot_id) {
    metrics_.slot_get_qps->get()->Increment();
    auto it = slot_map_.find(slot_id);
    if (it != slot_map_.end()) {
        metrics_.slot_hit_qps->get()->Increment();
        if (it->second.InMemory()) {
            // TODO(wangtai.10): maybe better
            metrics_.slot_in_memory_qps->get()->Increment();
        }
        return &it->second;
    }
    return nullptr;
}

inline std::vector<PageIndex> Index::GetSlotPages(uint64_t slot_id, bool with_dirty) {
    std::vector<PageIndex> pages;
    SlotNode* slot = GetSlot(slot_id);
    if (UNLIKELY(slot == nullptr)) {
        return pages;
    }
    for (const auto& page : slot->GetPages()) {
        if (page.page_size == 0) {
            continue;
        }

        if (page.dirty && !with_dirty) {
            continue;
        }

        pages.push_back(page);
    }
    return pages;
}

inline void Index::TouchSlot(uint64_t slot_id) {
    SlotNode* slot = GetSlot(slot_id);
    if (slot == nullptr) {
        return;
    }
    slot->SetLastUsed(SlotNode::WriteOptions(slot_node_allocator_),
                      GenerateAccessRecord(slot->GetLastUsed()));
}

inline void Index::MarkSlotDataDirty(uint64_t slot_id, uint64_t log_id, uint64_t log_sequence) {
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;
    GatherDirtySlot(slot_id, slot, log_id, log_sequence);
}

inline void Index::MarkSlotObjectTtlDirty(uint64_t slot_id, uint64_t log_id, uint64_t log_sequence,
                                          uint64_t object_id, uint64_t ttl) {
    SlotNode* slot = GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr) << slot_id;
    slot->SetObjectTtl(SlotNode::WriteOptions(slot_node_allocator_), object_id, ttl);
    OnObjectTtlLogValidated(slot, object_id);
    slot->SetMetaDirty(true);
    GatherDirtySlot(slot_id, slot, log_id, log_sequence);
}

inline SlotNode* Index::GetOrCreateSlotInternal(uint64_t slot_id, bool in_memory) {
    SlotNode* slot = GetSlot(slot_id);
    if (slot != nullptr) {
        return slot;
    }

    SlotNode& node = slot_map_[slot_id];
    node.SetInMemory(in_memory);
    metrics_.slot_new_qps->get()->Increment();
    return &node;
}

inline uint32_t Index::GenerateAccessRecord(uint32_t access_record) const {
    switch (access_record_type_) {
    case AccessRecordType::kLfu:
        // todo
        return access_record;
    case AccessRecordType::kLru:
    default:
        return GetCurrentTimeInSec();
    }
}

}  // namespace partition
}  // namespace bcache2
