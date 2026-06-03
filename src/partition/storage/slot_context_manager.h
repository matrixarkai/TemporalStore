// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/btree_map.h>
#include <stddef.h>

#include <algorithm>
#include <functional>
#include <list>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/allocator.h"
#include "common/controller.h"
#include "common/ring_array.h"
#include "partition/allocator_manager.h"
#include "protocol/storage.pb.h"

namespace bcache2 {
namespace partition {

struct SlotEvent {
    Controller* ctrl = nullptr;
    Closure<void>* callback = nullptr;

    SlotEvent(Controller* c, Closure<void>* cb) : ctrl(c), callback(cb) {}
};

struct LogItem {
    uint64_t log_id = 0;
    uint64_t log_sequence = 0;
    uint32_t log_size = 0;
    storage::OpLog::LogItem log;  // TODO(wangtai.10): group by key

    LogItem() {}
    LogItem(uint64_t log_id, uint64_t log_sequence, uint32_t log_size, storage::OpLog::LogItem log)
        : log_id(log_id), log_sequence(log_sequence), log_size(log_size), log(std::move(log)) {}
};

// organize logs related to this slot, OpLog
struct SlotContext {
    uint64_t slot_id = 0;
    uint64_t version = 0;

    uint64_t first_dirty_log_id = 0;
    byte::intrusive_list_node link;
    bool in_dirty_list = false;
    uint64_t last_log_sequence = 0;

    std::list<partition::LogItem, Allocator::StlWrapper<partition::LogItem>> logs;
    int meta_dirty_logs = 0;  // how many meta log in logs
    std::vector<int, Allocator::StlWrapper<int>>
        object_delete_logs;  // object_delete_logs[i] represents how many object_delete log of
                             // object i in logs
    std::vector<int, Allocator::StlWrapper<int>>
        object_dirty_logs;  // object_dirty_logs[i] represents how many page log or delete log of
                            // object i in logs

    std::vector<SlotEvent, Allocator::StlWrapper<SlotEvent>> foreground_events;
    std::vector<SlotEvent, Allocator::StlWrapper<SlotEvent>> background_events;
    // from Allocator to SlotContext automatically
    explicit SlotContext(Allocator* allocator)
        : logs(Allocator::StlWrapper<LogItem>(allocator)),
          object_delete_logs(Allocator::StlWrapper<int>(allocator)),
          object_dirty_logs(Allocator::StlWrapper<int>(allocator)),
          foreground_events(Allocator::StlWrapper<SlotEvent>(allocator)),
          background_events(Allocator::StlWrapper<SlotEvent>(allocator)) {}
};

using DirtySlotList = byte::intrusive_list<SlotContext, &SlotContext::link>;

class SlotContextManager {
 public:
    SlotContextManager(MetricsManager* metrics_manager, AllocatorManager* allocator_manager)
        : allocator_(allocator_manager->GetAllocator(AllocatorType::kSlotContext)),
          slot_contexts_(std::less<uint64_t>(),
                         Allocator::StlWrapper<std::pair<uint64_t, SlotContext>>(allocator_)) {
        metrics_.Init(metrics_manager);
    }
    ~SlotContextManager() = default;

    void SetSlotVersion(uint64_t slot_id, uint64_t version) {
        auto it = slot_contexts_.emplace(slot_id, allocator_).first;
        it->second.version = version;
    }

    uint64_t GetSlotVersion(uint64_t slot_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return 0;
        }
        return it->second.version;
    }

    void SetSlotFirstDirtyLogId(uint64_t slot_id, uint64_t first_dirty_log_id) {
        auto it = slot_contexts_.emplace(slot_id, allocator_).first;
        it->second.first_dirty_log_id = first_dirty_log_id;
    }

    uint64_t GetSlotFirstDirtyLogId(uint64_t slot_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return 0;
        }
        return it->second.first_dirty_log_id;
    }

    void SetSlotLastLogSequence(uint64_t slot_id, uint64_t last_log_sequence) {
        auto it = slot_contexts_.emplace(slot_id, allocator_).first;

        // we'll apply older oplog and mark slot dirty after load slot
        // so last_log_sequence is not strictly increasing now
        // TODO(wangtai.10): uncomment this line
        // BYTE_ASSERT(last_log_sequence >= it->second.last_log_sequence);

        it->second.last_log_sequence = last_log_sequence;
    }

    uint64_t GetSlotLastLogSequence(uint64_t slot_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return 0;
        }
        return it->second.last_log_sequence;
    }

    size_t AddSlotLog(uint64_t slot_id, uint64_t log_id, uint64_t log_sequence, uint32_t log_size,
                      const storage::OpLog::LogItem& log) {
        auto it = slot_contexts_.emplace(slot_id, allocator_).first;
        SlotContext* slot_context = &it->second;
        // for efficiency, logs must be order by log_id
        BYTE_ASSERT(slot_context->logs.empty() ||
                    log_sequence >= slot_context->logs.back().log_sequence);
        slot_context->logs.push_back(LogItem{log_id, log_sequence, log_size, log});

        if (log.object_deleted()) {
            if (log.object_id() >= slot_context->object_delete_logs.size()) {
                slot_context->object_delete_logs.resize(log.object_id() + 1);
            }
            ++slot_context->object_delete_logs[log.object_id()];
        }
        if (log.meta_log() || log.object_deleted()) {
            ++slot_context->meta_dirty_logs;
        }
        if (log.page_log() || log.object_deleted()) {
            if (log.object_id() >= slot_context->object_dirty_logs.size()) {
                slot_context->object_dirty_logs.resize(log.object_id() + 1);
            }
            ++slot_context->object_dirty_logs[log.object_id()];
        }

        metrics_.slot_log_num->get()->Set(slot_context->logs.size());
        return slot_context->logs.size();
    }

    partition::LogItem* MutableLastLog(uint64_t slot_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return nullptr;
        }

        return &it->second.logs.back();
    }

    const std::list<partition::LogItem, Allocator::StlWrapper<partition::LogItem>>& GetSlotLogs(
        uint64_t slot_id) {
        static std::list<partition::LogItem, Allocator::StlWrapper<partition::LogItem>> empty_guard(
            Allocator::StlWrapper<LogItem>(Allocator::DefaultAllocator()));

        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return empty_guard;
        }

        return it->second.logs;
    }

    size_t GetSlotLogNum(uint64_t slot_id) const {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return 0;
        }

        return it->second.logs.size();
    }

    int GetSlotDirtyMetaLogsNum(uint64_t slot_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return 0;
        }
        return it->second.meta_dirty_logs;
    }

    int GetSlotObjectDirtyPagesNum(uint64_t slot_id, uint8_t object_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return 0;
        }

        if (object_id >= it->second.object_dirty_logs.size()) {
            return 0;
        }
        return it->second.object_dirty_logs[object_id];
    }

    int GetSlotObjectDeleteNum(uint64_t slot_id, uint8_t object_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return 0;
        }

        if (object_id >= it->second.object_delete_logs.size()) {
            return 0;
        }
        return it->second.object_delete_logs[object_id];
    }

    // trim logs before log_sequence
    void TrimSlotLogs(uint64_t slot_id, uint64_t log_sequence) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return;
        }

        SlotContext* slot_context = &it->second;
        while (!slot_context->logs.empty()) {
            const LogItem& item = it->second.logs.front();
            if (item.log_sequence > log_sequence) {
                break;
            }

            if (item.log.meta_log() || item.log.object_deleted()) {
                BYTE_ASSERT(slot_context->meta_dirty_logs >= 1);
                --slot_context->meta_dirty_logs;
            }
            if (item.log.page_log() || item.log.object_deleted()) {
                BYTE_ASSERT(slot_context->object_dirty_logs[item.log.object_id()] >= 1);
                --slot_context->object_dirty_logs[item.log.object_id()];
            }
            if (item.log.object_deleted()) {
                BYTE_ASSERT(slot_context->object_delete_logs[item.log.object_id()] >= 1);
                --slot_context->object_delete_logs[item.log.object_id()];
            }
            slot_context->logs.pop_front();
        }
    }

    void ClearSlotLogs(uint64_t slot_id) { TrimSlotLogs(slot_id, UINT64_MAX); }

    void AddSlotEvent(uint64_t slot_id, Controller* ctrl,
                      bool background, Closure<void>* callback) {
        auto it = slot_contexts_.emplace(slot_id, allocator_).first;
        if (background) {
            it->second.background_events.emplace_back(ctrl, callback);
        } else {
            it->second.foreground_events.emplace_back(ctrl, callback);
        }
    }

    std::vector<SlotEvent, Allocator::StlWrapper<SlotEvent>> ExtractSlotEvents(uint64_t slot_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return {};
        }

        // we execute foreground_events first
        auto res = std::move(it->second.foreground_events);
        std::move(it->second.background_events.begin(), it->second.background_events.end(),
            std::back_inserter(res));
        it->second.background_events.clear();
        return res;
    }

    void ClearSlotEvents(uint64_t slot_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end()) {
            return;
        }

        it->second.foreground_events.clear();
        it->second.background_events.clear();
    }

    void PushDirtySlot(uint64_t slot_id) {
        auto it = slot_contexts_.find(slot_id);
        BYTE_ASSERT(it != slot_contexts_.end() && it->second.in_dirty_list == false);
        it->second.slot_id = slot_id;
        it->second.in_dirty_list = true;
        dirty_slots_num++;
        dirty_slot_list_.push_front(&it->second);
    }

    uint64_t GetLastDirtySlot() { return dirty_slot_list_.back().slot_id; }

    void PopLastDirtySlot() {
        dirty_slot_list_.back().in_dirty_list = false;
        dirty_slot_list_.pop_back();
        dirty_slots_num--;
    }

    void PopDirtySlot(uint64_t slot_id) {
        auto it = slot_contexts_.find(slot_id);
        if (it == slot_contexts_.end() || it->second.in_dirty_list == false) {
            return;
        }
        dirty_slot_list_.erase(&it->second);
        dirty_slots_num--;
        it->second.in_dirty_list = false;
        return;
    }

    bool DirtySlotsEmpty() { return dirty_slots_num == 0; }

    uint64_t DirtySlotsNum() { return dirty_slots_num; }

    void ResetSlotContext(uint64_t slot_id) { slot_contexts_.erase(slot_id); }

 private:
    Allocator* allocator_ = nullptr;
    std::map<uint64_t, SlotContext, std::less<uint64_t>,
             Allocator::StlWrapper<std::pair<const uint64_t, SlotContext>>>
        slot_contexts_;  // slot ID -> slot context
    DirtySlotList dirty_slot_list_;
    uint64_t dirty_slots_num = 0;

    SlotContextManagerMetrics metrics_;

    DISALLOW_COPY_AND_ASSIGN(SlotContextManager);
};

}  // namespace partition
}  // namespace bcache2
