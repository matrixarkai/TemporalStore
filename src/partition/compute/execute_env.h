// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/inlined_vector.h>
#include <absl/strings/string_view.h>
#include <byte/include/assert.h>
#include <byte/include/macros.h>

#include "common/module_config.h"
#include "common/slot.h"
#include "common/status.h"
#include "model/ips/ips_table_manager.h"
#include "model/model_context.h"
#include "model/schema.h"
#include "partition/metrics.h"
#include "partition/storage/object.h"
#include "partition/storage/object_manager.h"

namespace bcache2 {

namespace partition {

class CmdExecutor;
struct CmdContext;

}  // namespace partition

template <typename Model>
class ObjectHandle {
 public:
    ObjectHandle() {}

    Model* operator->() {
        model::SetModelContext(&context_);
        return object_.Model<Model>();
    }

    uint64_t Ttl() {
        uint64_t slot_id = context_.slot_id;
        uint64_t ttl;
        Status status = object_manager_->GetObjectTtl(slot_id, object_.Key(), &ttl);
        BYTE_ASSERT(status.IsOK()) << status;
        return ttl;
    }

    void SetTtl(uint64_t ttl) {
        uint64_t slot_id = context_.slot_id;
        Status status = object_manager_->SetObjectTtl(slot_id, object_.Key(), ttl);
        BYTE_ASSERT(status.IsOK()) << status;
    }

 private:
    friend class ExecuteEnv;

    void SetContext(partition::CmdContext* cmd_context, uint64_t partition_id, uint64_t slot_id,
                    partition::OpLogger* op_logger, TimeTracer* time_tracer) {
        context_.cmd_context = cmd_context;
        context_.partition_id = partition_id;
        context_.slot_id = slot_id;
        context_.object = &object_;
        context_.op_logger = op_logger;
        context_.time_tracer = time_tracer;
    }

    partition::Object object_;
    partition::ObjectManager* object_manager_ = nullptr;
    model::ModelContext context_;

    DISALLOW_COPY_AND_ASSIGN(ObjectHandle);
};

class ExecuteEnv {
 public:
    ExecuteEnv(partition::CmdContext* cmd_context, uint64_t partition_id,
               partition::OpLogger* op_logger, partition::ObjectManager* object_manager,
               TimeTracer* time_tracer, partition::CmdMetrics* cmd_metrics,
               ModuleMetrics* module_metrics, ModuleCustomConfig* custom_config)
        : cmd_context_(cmd_context),
          partition_id_(partition_id),
          op_logger_(op_logger),
          object_manager_(object_manager),
          time_tracer_(time_tracer),
          cmd_metrics_(cmd_metrics),
          module_metrics_(module_metrics),
          custom_config_(custom_config) {}

    uint64_t GetPartitionID() const { return partition_id_; }

    template <typename Model>
    Status GetObject(const absl::string_view& key, ObjectHandle<Model>* obj);

    template <typename Model>
    Status NewObject(const absl::string_view& key, ObjectHandle<Model>* obj);

    template <typename Model>
    Status GetOrNewObject(const absl::string_view& key, ObjectHandle<Model>* obj);

    Status DeleteObject(const absl::string_view& key);

    template <typename RealModuleMetrics>
    RealModuleMetrics* GetModuleMetrics() {
        return static_cast<RealModuleMetrics*>(module_metrics_);
    }

    template <typename RealModuleCustomConfig>
    RealModuleCustomConfig* GetModuleCustomConfig() {
        return static_cast<RealModuleCustomConfig*>(custom_config_);
    }

 private:
    friend class partition::CmdExecutor;

    partition::CmdContext* cmd_context_ = nullptr;
    uint64_t partition_id_ = 0;
    partition::OpLogger* op_logger_ = nullptr;
    partition::ObjectManager* object_manager_ = nullptr;
    TimeTracer* time_tracer_ = nullptr;
    partition::CmdMetrics* cmd_metrics_ = nullptr;
    ModuleMetrics* module_metrics_ = nullptr;
    ModuleCustomConfig* custom_config_ = nullptr;

    absl::InlinedVector<uint64_t, 4> off_memory_slots_;

    DISALLOW_COPY_AND_ASSIGN(ExecuteEnv);
};

template <typename Model>
inline Status ExecuteEnv::GetObject(const absl::string_view& key, ObjectHandle<Model>* obj) {
    uint64_t slot_id = hash_func(key.data(), key.size());
    obj->SetContext(cmd_context_, partition_id_, slot_id, op_logger_, time_tracer_);
    obj->object_manager_ = object_manager_;

    Status status = object_manager_->GetObject(slot_id, key, &obj->object_, true);
    if (status.IsFailedPrecondition()) {
        // object not in memory
        cmd_context_->object_in_memory = false;
        off_memory_slots_.push_back(slot_id);
        return status;
    }

    if (status.IsNotFound()) {
        cmd_context_->object_hit = false;
        return status;
    }

    return status;
}

template <typename Model>
inline Status ExecuteEnv::NewObject(const absl::string_view& key, ObjectHandle<Model>* obj) {
    uint64_t slot_id = hash_func(key.data(), key.size());
    obj->SetContext(cmd_context_, partition_id_, slot_id, op_logger_, time_tracer_);
    Status status = object_manager_->NewObject(slot_id, model::ModelManager::GetModelId<Model>(),
                                               key, &obj->object_, false);
    obj->object_manager_ = object_manager_;
    if (UNLIKELY(status.IsFailedPrecondition())) {
        off_memory_slots_.push_back(slot_id);
        return status;
    }
    return status;
}

template <typename Model>
inline Status ExecuteEnv::GetOrNewObject(const absl::string_view& key, ObjectHandle<Model>* obj) {
    uint64_t slot_id = hash_func(key.data(), key.size());
    obj->SetContext(cmd_context_, partition_id_, slot_id, op_logger_, time_tracer_);
    obj->object_manager_ = object_manager_;
    Status status = object_manager_->NewObject(slot_id, model::ModelManager::GetModelId<Model>(),
                                               key, &obj->object_, true);
    if (UNLIKELY(status.IsFailedPrecondition())) {
        off_memory_slots_.push_back(slot_id);
        return status;
    }
    return status;
}

inline Status ExecuteEnv::DeleteObject(const absl::string_view& key) {
    uint64_t slot_id = hash_func(key.data(), key.size());
    Status status = object_manager_->DeleteObject(slot_id, key);
    if (UNLIKELY(status.IsFailedPrecondition())) {
        off_memory_slots_.push_back(slot_id);
    }
    return status;
}

}  // namespace bcache2
