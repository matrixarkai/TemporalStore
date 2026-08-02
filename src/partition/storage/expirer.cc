// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/expirer.h"

#include "partition/metrics.h"
#include "partition/storage/object_manager.h"

DECLARE_uint64(expirer_scan_in_memory_slots_per_round);
DECLARE_uint64(expirer_scan_slots_per_round);

namespace bcache2 {
namespace partition {

Expirer::Expirer(Index* index, ObjectManager* object_manager, OpLogger* op_logger,
                 MetricsManager* metrics_manager)
    : index_(index), object_manager_(object_manager), op_logger_(op_logger) {
    in_memory_iter_ = index_->NewSlotIterator();
    in_store_iter_ = index_->NewSlotIterator();
    expire_latency_ = metrics_manager->Get<MetricsEnv::Histogram>(kMetricsExpireKeyLatency, {});
    expire_counter_ = metrics_manager->Get<MetricsEnv::Counter>(kMetricsExpireKeyCounter, {});
}

Status Expirer::TryExpire() {
    ScopedLatency latency(expire_latency_->get());

    auto slots = in_memory_iter_->Scan(FLAGS_expirer_scan_in_memory_slots_per_round);
    if (slots.empty()) {  // start over
        in_memory_iter_ = index_->NewSlotIterator();
    }
    uint64_t now = GetCurrentTimeInMs();
    for (auto slot_node_iter = slots.begin(); slot_node_iter != slots.end(); ++slot_node_iter) {
        SlotNode* slot_node = slot_node_iter->second;
        if (!slot_node->InMemory()) {
            continue;
        }

        for (auto& object : slot_node->GetObjects()) {
            uint64_t ttl = slot_node->GetObjectTtl(object.ObjectId());
            if (ttl == 0 || ttl > now) {
                continue;
            }
            object_manager_->DoExpireObject(slot_node_iter->first, slot_node, &object);
            expire_counter_->get()->Increment();
        }
    }

    slots = in_store_iter_->Scan(FLAGS_expirer_scan_slots_per_round);
    if (slots.empty()) {
        in_store_iter_ = index_->NewSlotIterator();
    }
    for (size_t i = 0; i < slots.size(); ++i) {
        SlotNode* slot_node = index_->GetSlot(slots[i].first);

        if (slot_node->InMemory()) {
            continue;
        }
        // if one object in this slot expires, load the whole slot into memory and check further
        uint64_t min_ttl = slot_node->GetMinTtl();
        if (min_ttl == 0 || min_ttl > now) {
            continue;
        }

        Controller ctrl;
        SYNC_CALL(object_manager_->LoadObject, &ctrl, slots[i].first, true);
        if (!ctrl.status().ok()) {
            continue;
        }
        slot_node = index_->GetSlot(slots[i].first);
        if (!slot_node->InMemory()) {
            continue;
        }

        for (auto& object : slot_node->GetObjects()) {
            uint64_t ttl = slot_node->GetObjectTtl(object.ObjectId());
            if (ttl == 0 || ttl > now) {
                continue;
            }
            object_manager_->DoExpireObject(slots[i].first, slot_node, &object);
            expire_counter_->get()->Increment();
        }
    }

    // avoid replicator oos because of replaying oplog generated when no clients write arrives
    op_logger_->Commit(nullptr, nullptr);
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
