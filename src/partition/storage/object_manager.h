// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/base/closure.h>
#include <byte/include/assert.h>

#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/allocator.h"
#include "common/controller.h"
#include "common/scoped_invoker.h"
#include "model/model_manager.h"
#include "partition/index/index.h"
#include "partition/partition.h"
#include "partition/storage/op_logger.h"
#include "partition/storage/page_store.h"

namespace bcache2 {
namespace partition {

class SlotStore;
class AllocatorManager;
class SlotContextManager;

// TODO(wangtai.10): maybe converges in Index
class ObjectManager {
 public:
    ObjectManager(Partition* partition, Index* index, PageStore* page_store, OpLogger* op_logger,
                  SlotStore* slot_store, SlotContextManager* slot_context_manager,
                  AllocatorManager* allocator_manager);
    virtual ~ObjectManager();

    Status Load();

    Status ReplayOplog(uint64_t log_id, uint32_t log_size, const storage::OpLog& oplog);

    Status GetObject(uint64_t slot_id, const absl::string_view& key, Object* obj,
                     bool check_expire) {
        LOG_CALL_DEBUG()
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id)
            .put("Key", key);
        SlotNode* slot = index_->GetSlot(slot_id);
        if (slot == nullptr) {
            LOG_DEBUG("Slot not found")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", slot_id);
            return Status::NotFound("Key not exists due to slot not found");
        }

        if (!slot->InMemory()) {
            LOG_DEBUG("Slot not in memory")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", slot_id);
            return Status::FailedPrecondition("Slot not in memory");
        }

        index_->TouchSlot(slot_id);

        for (auto& object : slot->GetObjects()) {
            if (object.Key() != key) {
                continue;
            }
            if (check_expire && IsObjecttExpired(slot_id, slot, &object)) {
                LOG_DEBUG("Key expired")
                    .put("PartitionId", partition_->GetPartitionID())
                    .put("SlotId", slot_id)
                    .put("Key", key)
                    .put("Ttl", slot->GetObjectTtl(object.ObjectId()));
                return Status::NotFound("Key not exists due to expired");
            }
            *obj = object;
            return Status::OK();
        }
        LOG_DEBUG("Key not found")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id)
            .put("Key", key);
        return Status::NotFound("Key not exists");
    }

    Status NewObject(uint64_t slot_id, uint8_t model_id, const absl::string_view& key, Object* obj,
                     bool return_if_already_exists) {
        return CreateObjectInternal(slot_id, UINT8_MAX, key, model_id, {}, obj, true,
                                    return_if_already_exists);
    }

    Status DeleteObject(uint64_t slot_id, const absl::string_view& key) {
        SlotNode* slot = index_->GetSlot(slot_id);
        if (slot == nullptr) {
            return Status::NotFound("Key not exists due to slot not exists");
        } else if (!slot->InMemory()) {
            return Status::FailedPrecondition("Slot not in memory");
        }

        index_->TouchSlot(slot_id);

        Object object;
        Status status = slot->FindObject(key, &object);
        if (!status.ok()) {
            return status;
        }

        status = DoExpireObject(slot_id, slot, &object);
        if (status.ok()) {
            return Status::NotFound(
                "Key not exists due to expired");  // if it already expires, it's non-existent
        }

        op_logger_->DeleteObject(slot_id, object.ObjectId(), key, object.ModelId());
        slot->DeleteObject(SlotNode::WriteOptions(slot_node_allocator_), object.Key());
        return Status::OK();
    }

    void LoadObject(Controller* ctrl, uint64_t slot_id, bool is_background_task,
                    Closure<void>* callback);

    Status SetObjectTtl(uint64_t slot_id, const absl::string_view& key, uint64_t ttl);
    Status GetObjectTtl(uint64_t slot_id, const absl::string_view& key, uint64_t* ttl);

    // return OK iff this key is deleted/expired
    Status DoExpireObject(uint64_t slot_id, SlotNode* slot, Object* object) {
        BYTE_ASSERT(slot != nullptr);
        BYTE_ASSERT(object != nullptr);

        uint64_t ttl = slot->GetObjectTtl(object->ObjectId());
        LOG_DEBUG("TryExpireObject")
            .put("PartitionId", partition_->GetPartitionID())
            .put("Key", object->Key())
            .put("Ttl", ttl);
        if (LIKELY(ttl == 0 || ttl > GetCurrentTimeInMs())) {
            return Status::FailedPrecondition("Key not expired");
        }
        // expiration is deletion
        op_logger_->DeleteObject(slot_id, object->ObjectId(), object->Key(), object->ModelId());
        slot->DeleteObject(SlotNode::WriteOptions(slot_node_allocator_), object->Key());

        return Status::OK();
    }

    bool IsObjecttExpired(uint64_t slot_id, SlotNode* slot, Object* object) {
        BYTE_ASSERT(slot != nullptr);
        BYTE_ASSERT(object != nullptr);

        uint64_t ttl = slot->GetObjectTtl(object->ObjectId());
        if (LIKELY(ttl == 0 || ttl > GetCurrentTimeInMs())) {
            return false;
        }
        return true;
    }

 private:
    friend class Object;
    friend class StorageManager;

    struct LoadContext {
        Controller* ctrl = nullptr;
        uint64_t slot_id = 0;
        std::map<std::string, ObjectPages> object_pages;
        Closure<void>* callback = nullptr;
    };

    void OnLoadObjectDone(LoadContext* context);
    Status LoadObjectPages(uint64_t slot_id, std::map<std::string, ObjectPages> object_pages);
    Status ApplyOpLog(uint64_t log_id, uint64_t log_sequence, uint32_t log_size,
                      const storage::OpLog::LogItem& log_item);
    Status ApplyKvLog(uint64_t log_id, const storage::OpLog::LogItem& log_item);

    // pages -> an object
    Status CreateObjectInternal(uint64_t slot_id, uint8_t object_id, const absl::string_view& key,
                                uint16_t model_id, std::vector<PageInfo> pages, Object* object,
                                bool expire, bool return_if_already_exists) {
        LOG_CALL_DEBUG()
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id)
            .put("Key", key)
            .put("ModeId", model_id);
        SlotNode* slot = index_->GetOrCreateSlot(slot_id);
        if (!slot->InMemory()) {
            if (slot->GetPageNum() != 0 || slot->GetObjectNum() != 0 ||
                slot_context_manager_->GetSlotLogNum(slot_id) != 0) {
                return Status::FailedPrecondition("Slot not in memory");
            }
            slot->SetInMemory(true);
        }

        if (expire) {
            Object obj;
            Status status = slot->FindObject(key, &obj);
            if (status.ok() && !DoExpireObject(slot_id, slot, &obj).ok()) {
                if (return_if_already_exists) {
                    *object = obj;
                    return Status::OK();
                }
                LOG_DEBUG("Object already exist")
                    .put("PartitionId", partition_->GetPartitionID())
                    .put("SlotId", slot_id)
                    .put("Key", key);
                return Status::AlreadyExists("object already exist");
            }
        }
        // create an object and initialize it
        Status status = slot->NewObjectWithId(
            SlotNode::WriteOptions(slot_node_allocator_, return_if_already_exists), object_id,
            model_id, key, object);

        status = model::ModelManager::Init(model_id, object->RawModelBuf(), model_allocator_,
                                           std::move(pages));
        if (!status.ok()) {
            LOG_ERROR("Model init failed")
                .put("PartitionId", partition_->GetPartitionID())
                .put("SlotId", slot_id)
                .put("Key", key)
                .put("ModelId", model_id)
                .put("Error", status);
            slot->DeleteObject(SlotNode::WriteOptions(slot_node_allocator_), key);
            return status;
        }
        LOG_DEBUG("Create object")
            .put("PartitionId", partition_->GetPartitionID())
            .put("SlotId", slot_id)
            .put("Key", key);
        index_->TouchSlot(slot_id);
        return Status::OK();
    }

    Partition* partition_ = nullptr;
    Index* index_ = nullptr;
    PageStore* page_store_ = nullptr;
    OpLogger* op_logger_ = nullptr;
    SlotStore* slot_store_ = nullptr;
    SlotContextManager* slot_context_manager_ = nullptr;
    Allocator* slot_node_allocator_ = nullptr;
    Allocator* model_allocator_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(ObjectManager);
};

}  // namespace partition
}  // namespace bcache2
