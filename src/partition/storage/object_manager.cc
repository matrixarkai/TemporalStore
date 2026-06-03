// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/storage/object_manager.h"

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/metrics.h"
#include "model/feature_model.h"
#include "model/hash_model.h"
#include "model/string_model.h"
#include "partition/allocator_manager.h"
#include "partition/metrics.h"
#include "partition/partition.h"
#include "partition/storage/evicter.h"
#include "partition/storage/slot_context_manager.h"
#include "partition/storage/slot_store.h"

namespace bcache2 {
namespace partition {

ObjectManager::ObjectManager(Partition* partition, Index* index, PageStore* page_store,
                             OpLogger* op_logger, SlotStore* slot_store,
                             SlotContextManager* slot_context_manager,
                             AllocatorManager* allocator_manager)
    : partition_(partition),
      index_(index),
      page_store_(page_store),
      op_logger_(op_logger),
      slot_store_(slot_store),
      slot_context_manager_(slot_context_manager),
      slot_node_allocator_(allocator_manager->GetAllocator(AllocatorType::kSlotNode)),
      model_allocator_(allocator_manager->GetAllocator(AllocatorType::kModel)) {}

ObjectManager::~ObjectManager() {}

// replay oplog
Status ObjectManager::Load() {
    auto iter = op_logger_->NewIterator(index_->GetDumpedLogId());
    LOG_INFO("Start replay oplog")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Start", index_->GetDumpedLogId());
    Status status;
    while ((status = iter->Next()).ok()) {
        const uint64_t log_id = iter->GetLogId();
        const uint32_t log_size = iter->GetSize();
        const auto& oplog = iter->GetLog();
        Status status = ReplayOplog(log_id, log_size, oplog);
        if (!status.ok()) {
            LOG_WARNING("Failed to replay oplog")
                .put("PartitionId", partition_->GetPartitionID())
                .put("Status", status)
                .put("Log", oplog.ShortDebugString());
            return status;
        }
    }
    if (!status.IsOutOfRange()) {
        LOG_ERROR("Replay oplog failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Error", status);
        return status;
    }
    LOG_INFO("Replay oplog ok")
        .put("PartitionId", partition_->GetPartitionID())
        .put("Current oplog sequence", op_logger_->CurrentSequence());
    return Status::OK();
}

Status ObjectManager::ReplayOplog(uint64_t log_id, uint32_t log_size, const storage::OpLog& oplog) {
    LOG_DEBUG("Replay oplog")
        .put("PartitionId", partition_->GetPartitionID())
        .put("LogId", log_id)
        .put("Log", oplog.ShortDebugString());
    if (op_logger_->CurrentSequence() != 0 && oplog.sequence() <= op_logger_->CurrentSequence()) {
        LOG_DEBUG("Skip already replayed oplog")
            .put("PartitionId", partition_->GetPartitionID())
            .put("CurrentSequence", op_logger_->CurrentSequence())
            .put("LogSequence", oplog.sequence())
            .put("Log", oplog.ShortDebugString());
        return Status::OK();
    }
    if (op_logger_->CurrentSequence() != 0 &&
        op_logger_->CurrentSequence() + 1 != oplog.sequence()) {
        LOG_ERROR("Data loss")
            .put("PartitionId", partition_->GetPartitionID())
            .put("CurrentSequence", op_logger_->CurrentSequence())
            .put("LogSequence", oplog.sequence())
            .put("Log", oplog.ShortDebugString());
        return Status::DataLoss("");
    }
    op_logger_->SetCurrentSequence(oplog.sequence());

    for (int i = 0; i < oplog.item_size(); ++i) {
        const auto& log_item = oplog.item(i);

        if (oplog.sequence() <= slot_context_manager_->GetSlotVersion(log_item.slot_id())) {
            LOG_DEBUG("Skip oplog")
                .put("Log", oplog.ShortDebugString())
                .put("SlotVersion", slot_context_manager_->GetSlotVersion(log_item.slot_id()));
            continue;
        }

        SlotNode* slot = index_->GetOrCreateSlot(log_item.slot_id());
        // replay page index part and mark dirty for flush index log
        // because the old page may have been GC
        if (log_item.page_log()) {
            index_->MarkSlotPageDirty(
                log_item.slot_id(), log_id, oplog.sequence(),
                {OpLogger::Log2Page(log_id, oplog.sequence(), log_size, log_item)});
        } else if (log_item.object_deleted()) {
            index_->MarkSlotObjectDeleted(log_item.slot_id(), log_id, oplog.sequence(),
                                          log_item.object_id());
        } else if (log_item.meta_log()) {
            index_->MarkSlotObjectTtlDirty(log_item.slot_id(), log_id, oplog.sequence(),
                                           log_item.object_id(), log_item.ttl());
        } else {
            // kv log
            index_->MarkSlotDataDirty(log_item.slot_id(), log_id, oplog.sequence());
        }
        if (!slot->InMemory()) {
            // slot not in memory, so just attach the oplog, which will be applied later
            slot_context_manager_->AddSlotLog(log_item.slot_id(), log_id, oplog.sequence(),
                                              log_size, log_item);
            continue;
        }

        Status status = ApplyOpLog(log_id, oplog.sequence(), log_size, log_item);
        if (!status.ok()) {
            LOG_ERROR("Failed to apply oplog")
                .put("PartitionId", partition_->GetPartitionID())
                .put("LogId", log_id)
                .put("LogSize", log_size)
                .put("Status", status)
                .put("LogItem", log_item.ShortDebugString());
            return status;
        }

        slot_context_manager_->AddSlotLog(log_item.slot_id(), log_id, oplog.sequence(), log_size,
                                          log_item);
    }
    return Status::OK();
}

// load all objects in a slot
void ObjectManager::LoadObject(Controller* ctrl, uint64_t slot_id, bool is_background_task,
                               Closure<void>* callback) {
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID()).put("SlotId", slot_id);
    SlotNode* slot = index_->GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr);
    if (slot->InMemory()) {
        ctrl->set_status(Status::OK());
        byte::InvokeInCurrentThread(callback);
        LOG_WARNING("No need to load")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id);
        return;
    }

    if (!slot->Loading()) {
        LOG_DEBUG("Start load slot")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id);
        slot->SetLoading(true);

        LoadContext* context = new LoadContext;
        context->ctrl = ctrl;
        context->slot_id = slot_id;
        context->callback = callback;
        slot_store_->LoadSlot(ctrl, slot_id, &context->object_pages,
                              NewClosure(this, &ObjectManager::OnLoadObjectDone, context));
    } else {
        LOG_DEBUG("Slot is loading")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id);
        slot_context_manager_->AddSlotEvent(slot_id, ctrl, is_background_task, callback);
    }
}

void ObjectManager::OnLoadObjectDone(LoadContext* context) {
    uint64_t slot_id = context->slot_id;
    LOG_CALL_DEBUG().put("PartitionId", partition_->GetPartitionID()).put("SlotId", slot_id);
    SlotNode* slot = index_->GetSlot(slot_id);
    BYTE_ASSERT(slot != nullptr);
    if (context->ctrl->status().ok()) {
        BYTE_ASSERT(!slot->InMemory());
        slot->SetInMemory(
            true);  // <- update its status temporarily to pass checks in `LoadObjectPages`
        Status status = LoadObjectPages(slot_id, std::move(context->object_pages));
        if (!status.ok()) {
            LOG_WARNING("Load object pages failed")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", slot_id)
                .put("Error", status.ToString());
            slot->ClearObjects(SlotNode::WriteOptions(slot_node_allocator_));
            context->ctrl->set_status(status);
        } else {
            LOG_DEBUG("Load object pages succ")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", slot_id);
        }
        slot->SetInMemory(false);
    } else {
        LOG_WARNING("OnLoadObjectDone failed")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id);
    }

    BYTE_ASSERT(!slot->InMemory());
    BYTE_ASSERT(slot->Loading());
    slot->SetInMemory(context->ctrl->status().ok());
    slot->SetLoading(false);

    const auto& slot_events = slot_context_manager_->ExtractSlotEvents(slot_id);
    Status status = context->ctrl->status();
    context->callback->Run();
    std::unique_ptr<LoadContext> context_release_guard(context);
    for (size_t i = 0; i < slot_events.size(); ++i) {  // wake up those awaiting callbacks
        slot_events[i].ctrl->set_status(status);
        slot_events[i].callback->Run();
    }
}

Status ObjectManager::LoadObjectPages(uint64_t slot_id,
                                      std::map<std::string, ObjectPages> object_pages) {
    for (auto& object : object_pages) {
        BYTE_ASSERT(object.second.size() > 0);
        Object obj;
        auto object_id = object.second[0].header.object_id();
        auto model_id = object.second[0].header.model_id();
        Status status = CreateObjectInternal(slot_id, object_id, object.first, model_id,
                                             std::move(object.second), &obj, false, false);
        if (!status.ok()) {
            return status;
        }
    }

    const auto& logs = slot_context_manager_->GetSlotLogs(slot_id);
    for (auto it = logs.begin(); it != logs.end(); ++it) {
        // apply pending logs in this slot
        Status status = ApplyOpLog(it->log_id, it->log_sequence, it->log_size, it->log);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status ObjectManager::ApplyOpLog(uint64_t log_id, uint64_t log_sequence, uint32_t log_size,
                                 const storage::OpLog::LogItem& log_item) {
    LOG_CALL_DEBUG()
        .put("PartitionId", partition_->GetPartitionID())
        .put("LogId", log_id)
        .put("LogSize", log_size)
        .put("LogData", log_item.ShortDebugString());
    if (log_item.page_log()) {
        SlotNode* slot = index_->GetSlot(log_item.slot_id());
        BYTE_ASSERT(slot != nullptr && slot->InMemory());
        uint64_t ttl = slot->GetObjectTtl(log_item.object_id());  // * keep its TTL
        // delete the old object
        Status status =
            slot->DeleteObject(SlotNode::WriteOptions(slot_node_allocator_), log_item.object_key());
        BYTE_ASSERT(status.ok() || status.IsNotFound()) << status;
        // re-create an object
        Object object;
        status = CreateObjectInternal(
            log_item.slot_id(), log_item.object_id(), log_item.object_key(), log_item.model_id(),
            {OpLogger::Log2Page(log_id, log_sequence, log_size, log_item)}, &object, false, false);
        BYTE_ASSERT(status.ok());
        slot->SetObjectTtl(SlotNode::WriteOptions(slot_node_allocator_), object.ObjectId(), ttl);
    } else if (log_item.object_deleted()) {
        SlotNode* slot = index_->GetSlot(log_item.slot_id());
        BYTE_ASSERT(slot != nullptr && slot->InMemory());
        uint64_t ttl = slot->GetObjectTtl(log_item.object_id());  // * keep its TTL
        Status status =
            slot->DeleteObject(SlotNode::WriteOptions(slot_node_allocator_), log_item.object_key());
        BYTE_ASSERT(status.ok() || status.IsNotFound()) << status;
        slot->SetObjectTtl(SlotNode::WriteOptions(slot_node_allocator_), log_item.object_id(), ttl);
    } else if (log_item.meta_log()) {
        // ttl already in index
    } else {
        return ApplyKvLog(log_item.slot_id(), log_item);
    }
    return Status::OK();
}

Status ObjectManager::ApplyKvLog(uint64_t log_id, const storage::OpLog::LogItem& log_item) {
    Object object;
    Status status = GetObject(log_item.slot_id(), log_item.object_key(), &object, false);
    LOG_DEBUG("ApplyKvLog")
        .put("PartitionId", partition_->GetPartitionID())
        .put("ObjectKey", log_item.object_key())
        .put("Key", log_item.key())
        .put("Value", log_item.value())
        .put("Ttl", log_item.ttl())
        .put("Deleted", log_item.deleted())
        .put("Status", status.ToString());
    BYTE_ASSERT(status.ok() || status.IsNotFound());
    if (status.IsNotFound()) {
        status =
            CreateObjectInternal(log_item.slot_id(), log_item.object_id(), log_item.object_key(),
                                 log_item.model_id(), {}, &object, false, false);
        BYTE_ASSERT(status.ok());
    }
    return model::ModelManager::Apply(object.ModelId(), object.RawModelBuf(), model_allocator_,
                                      log_item.key(), log_item.value(), log_item.cluster_id(),
                                      log_item.timestamp_ms(), log_item.deleted());
}

Status ObjectManager::GetObjectTtl(uint64_t slot_id, const absl::string_view& key, uint64_t* ttl) {
    SlotNode* slot = index_->GetSlot(slot_id);
    if (slot == nullptr) {
        return Status::NotFound("Key not exists");
    } else if (!slot->InMemory()) {
        return Status::FailedPrecondition("Slot not in memory");
    }

    Object object;
    Status status = slot->FindObject(key, &object);
    if (!status.ok()) {
        return status;
    }
    *ttl = slot->GetObjectTtl(object.ObjectId());
    return Status::OK();
}

Status ObjectManager::SetObjectTtl(uint64_t slot_id, const absl::string_view& key, uint64_t ttl) {
    SlotNode* slot = index_->GetSlot(slot_id);
    if (slot == nullptr) {
        return Status::NotFound("Key not exists");
    } else if (!slot->InMemory()) {
        return Status::FailedPrecondition("Slot not in memory");
    }

    Object object;
    Status status = slot->FindObject(key, &object);
    if (!status.ok()) {
        return status;
    }

    index_->TouchSlot(slot_id);

    op_logger_->WriteTtlLog(slot_id, key, object.ModelId(), object.ObjectId(), ttl);
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
