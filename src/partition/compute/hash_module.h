// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/flat_hash_map.h>
#include <byte/base/closure.h>
#include <byte/include/macros.h>

#include "common/controller.h"
#include "model/hash_model.h"
#include "partition/cmd_context.h"
#include "partition/compute/cmd.h"
#include "partition/storage/object_manager.h"
#include "protocol/server.pb.h"

#define CMD_EXECUTOR_HASH_PREPARE_CTX_DEFAULT(cmd_type, cmd_name)          \
    CMD_EXECUTOR_PREPARE_CTX_DEFAULT(hash, Hash, hash, cmd_type, cmd_name, \
                                     model::ModelManager::GetModelId<HashModel>())
#define CMD_EXECUTOR_HASH_EXECUTE(cmd_type, cmd_name) \
    CMD_EXECUTOR_EXECUTE(hash, Hash, hash, cmd_type, cmd_name)

namespace bcache2 {
namespace partition {

using model::HashModel;

class ObjectManager;

CMD_EXECUTOR_HASH_PREPARE_CTX_DEFAULT(Set, set)
CMD_EXECUTOR_HASH_EXECUTE(Set, set) {
    if (!ctx->object) {
        Status status = options.object_manager_->NewObject(
            ctx->slot_id, model::ModelManager::GetModelId<HashModel>(), request->key(),
            &ctx->object, false);
        BYTE_ASSERT(status.ok()) << status;
    }
    HashModel* model = ctx->object.Model<HashModel>();

    model->OrSet().Set(ctx, request->field(), request->value());
    return Status::OK();
}

CMD_EXECUTOR_HASH_PREPARE_CTX_DEFAULT(Get, get)
CMD_EXECUTOR_HASH_EXECUTE(Get, get) {
    if (!ctx->object) {
        return Status::NotFound("Key not found");
    }
    HashModel* model = ctx->object.Model<HashModel>();

    return model->OrSet().Get(request->field(), response->mutable_value());
}

CMD_EXECUTOR_HASH_PREPARE_CTX_DEFAULT(Del, del)
CMD_EXECUTOR_HASH_EXECUTE(Del, del) {
    if (!ctx->object) {
        return Status::NotFound("Key not found");
    }
    HashModel* model = ctx->object.Model<HashModel>();

    return model->OrSet().Del(ctx, request->field());
}

}  // namespace partition
}  // namespace bcache2
