// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "partition/compute/cmd.h"

#include <byte/algorithm/crc64.h>

#include <utility>
#include <vector>

#include "common/logging.h"
#include "common/scoped_invoker.h"
#include "common/slot.h"
#include "model/hash_model.h"
#include "partition/compute/common_module.h"
// #include "partition/compute/feature_module.h"
#include "partition/compute/hash_module.h"
#include "partition/compute/set_module.h"
#include "partition/compute/string_module.h"
#include "partition/storage/object_manager.h"

namespace bcache2 {
namespace partition {

CmdExecutorManager::CmdExecutorManager(ObjectManager* object_manager,
                                       MetricsManager* metrics_manager)
    : object_manager_(object_manager) {
    auto& modules_api = ModuleManager::Ref().GetModuleApiTable();
    cmd_metrics_.resize(modules_api.size());
    for (size_t i = 0; i < modules_api.size(); i++) {
        for (size_t j = 0; j < modules_api[i].cmd_executors.size(); j++) {
            cmd_metrics_[i].emplace_back(new RequestMetrics(
                metrics_manager, "cmd",
                {{"module", modules_api[i].name}, {"cmd", modules_api[i].cmd_executors[j].name}}));
        }
    }
}

void CmdExecutorManager::ExecuteCmd(CmdContext* ctx, const CmdRequest* request,
                                    CmdResponse* response, Closure<void>* callback) {
    ScopedCallback done(callback);
    size_t module_index = 0, cmd_id = 0;
    Status status = ModuleManager::Ref().GetId(request, &module_index, &cmd_id);
    if (!status.ok()) {
        LOG_WARNING("Get module cmd failed")
            .put("Module", request->module_case())
            .put("Error", status.ToString());
        ctx->ctrl->set_status(status);
        return;
    }

    ModuleManager::ExecuteFunc preparer;
    ModuleManager::ExecuteFunc executor;
    status = ModuleManager::Ref().GetFunc(module_index, cmd_id, &preparer, &executor);
    if (!status.ok()) {
        LOG_WARNING("Get cmd func failed")
            .put("Module", module_index)
            .put("Cmd", cmd_id)
            .put("Error", status.ToString());
        ctx->ctrl->set_status(status);
        return;
    }

    ModuleManager::Options options;
    options.object_manager_ = object_manager_;

    ctx->metrics = cmd_metrics_[module_index][cmd_id].get();
    status = preparer(options, ctx, request, response);
    if (!status.ok()) {
        ctx->ctrl->set_status(status);
        return;
    }

    ctx->time_tracer.AddEvent("prepare_cmd");
    done.Release();
    CallExecutor(std::move(executor), std::move(options), ctx, request, response, callback);
}

void CmdExecutorManager::CallExecutor(ModuleManager::ExecuteFunc executor,
                                      ModuleManager::Options options, CmdContext* ctx,
                                      const CmdRequest* request, CmdResponse* response,
                                      Closure<void>* callback) {
    ScopedCallback done(callback);

    if (!ctx->ctrl->status().ok()) {
        LOG_WARNING("Load object failed")
            .put("SlotId", ctx->slot_id)
            .put("Key", ctx->key)
            .put("Error", ctx->ctrl->status().ToString());
        ctx->time_tracer.AddEvent("prepare_object");
        return;
    }

    Status status = object_manager_->GetObject(ctx->slot_id, ctx->key, &ctx->object, true);
    LOG_DEBUG("Get object")
        .put("SlotId", ctx->slot_id)
        .put("Key", ctx->key)
        .put("TraceId", ctx->ctrl->trace_id())
        .put("Error", status);
    if (status.IsFailedPrecondition()) {  // if this slot is not in memory, load this object and
                                          // then try again
        done.Release();
        object_manager_->LoadObject(
            ctx->ctrl, ctx->slot_id, false,
            NewClosure(this, &CmdExecutorManager::CallExecutor, std::move(executor),
                       std::move(options), ctx, request, response, callback));
        return;
    } else if (!status.IsNotFound() && !status.ok()) {
        LOG_WARNING("Get object failed")
            .put("SlotId", ctx->slot_id)
            .put("Key", ctx->key)
            .put("Error", status.ToString());
        ctx->time_tracer.AddEvent("prepare_object");
        ctx->ctrl->set_status(status);
        return;
    }
    // OK or NotFound, as some commands need to handle NotFound properly
    ctx->time_tracer.AddEvent("prepare_object");
    if (ctx->object && ctx->model_id != 0 && ctx->object.ModelId() != ctx->model_id) {
        ctx->ctrl->set_status(Status::Unmatched("Key type not match"));  // model mismatch
        return;
    }
    status = executor(options, ctx, request, response);
    response->mutable_response_status()->CopyFrom(status.ToRpcStatus());
}

}  // namespace partition
}  // namespace bcache2
