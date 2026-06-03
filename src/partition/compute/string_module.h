// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <byte/include/macros.h>

#include <string>

#include "common/controller.h"
#include "model/string_model.h"
#include "partition/cmd_context.h"
#include "partition/compute/cmd.h"
#include "partition/storage/object_manager.h"
#include "protocol/server.pb.h"

#define CMD_EXECUTOR_STRING_PREPARE_CTX_DEFAULT(cmd_type, cmd_name)           \
    CMD_EXECUTOR_PREPARE_CTX_DEFAULT(str, String, string, cmd_type, cmd_name, \
                                     model::ModelManager::GetModelId<StringModel>())
#define CMD_EXECUTOR_STRING_EXECUTE(cmd_type, cmd_name) \
    CMD_EXECUTOR_EXECUTE(str, String, string, cmd_type, cmd_name)

namespace bcache2 {
namespace partition {

using model::StringModel;

class ObjectManager;

CMD_EXECUTOR_STRING_PREPARE_CTX_DEFAULT(Set, set)
CMD_EXECUTOR_STRING_EXECUTE(Set, set) {
    LOG_CALL_DEBUG().put("SlotId", ctx->slot_id);
    if (!ctx->object) {
        Status status = options.object_manager_->NewObject(
            ctx->slot_id, model::ModelManager::GetModelId<StringModel>(), request->key(),
            &ctx->object, false);
        BYTE_ASSERT(status.ok()) << status;
    }
    StringModel* model = ctx->object.Model<StringModel>();
    uint64_t ttl = 0;
    // ttl remains
    Status status = options.object_manager_->GetObjectTtl(ctx->slot_id, request->key(), &ttl);
    if (!status.ok()) {
        return status;
    }
    model->SetValue(ctx, request->value(), ttl);

    return Status::OK();
}

CMD_EXECUTOR_STRING_PREPARE_CTX_DEFAULT(Get, get)
CMD_EXECUTOR_STRING_EXECUTE(Get, get) {
    if (!ctx->object) {
        return Status::NotFound("Key not found");
    }
    StringModel* model = ctx->object.Model<StringModel>();
    response->set_value(model->GetValue());
    return Status::OK();
}

CMD_EXECUTOR_STRING_PREPARE_CTX_DEFAULT(Setex, setex)
CMD_EXECUTOR_STRING_EXECUTE(Setex, setex) {
    uint64_t ttl_ms = request->ttl_ms() + GetCurrentTimeInMs();

    if (!ctx->object) {
        Status status = options.object_manager_->NewObject(
            ctx->slot_id, model::ModelManager::GetModelId<StringModel>(), request->key(),
            &ctx->object, false);
        BYTE_ASSERT(status.ok()) << status;
    }
    StringModel* model = ctx->object.Model<StringModel>();

    model->SetValue(ctx, request->value(), ttl_ms);
    options.object_manager_->SetObjectTtl(ctx->slot_id, request->key(), ttl_ms);
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
