// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/flat_hash_map.h>
#include <byte/base/closure.h>
#include <byte/include/macros.h>

#include "common/controller.h"
#include "common/slot.h"
#include "partition/cmd_context.h"
#include "partition/compute/cmd.h"
#include "partition/storage/object_manager.h"
#include "protocol/server.pb.h"

#define CMD_EXECUTOR_COMMON_PREPARE_CTX_DEFAULT(cmd_type, cmd_name) \
    CMD_EXECUTOR_PREPARE_CTX_DEFAULT(bcache2, Common, common, cmd_type, cmd_name, 0)
#define CMD_EXECUTOR_COMMON_EXECUTE(cmd_type, cmd_name) \
    CMD_EXECUTOR_EXECUTE(bcache2, Common, common, cmd_type, cmd_name)

namespace bcache2 {
namespace partition {

class ObjectManager;

CMD_EXECUTOR_COMMON_PREPARE_CTX_DEFAULT(DelObject, del_object)
CMD_EXECUTOR_COMMON_EXECUTE(DelObject, del_object) {
    auto& key = request->key();
    uint64_t slot_id = hash_func(key.data(), key.size());
    return options.object_manager_->DeleteObject(slot_id, key);
}

CMD_EXECUTOR_COMMON_PREPARE_CTX_DEFAULT(Expire, expire)
CMD_EXECUTOR_COMMON_EXECUTE(Expire, expire) {
    if (!ctx->object) {
        return Status::NotFound("Key not found");
    }
    uint64_t ttl_ms = request->ttl_ms() + GetCurrentTimeInMs();
    return options.object_manager_->SetObjectTtl(ctx->slot_id, request->key(), ttl_ms);
}

CMD_EXECUTOR_COMMON_PREPARE_CTX_DEFAULT(Ttl, ttl)
CMD_EXECUTOR_COMMON_EXECUTE(Ttl, ttl) {
    if (!ctx->object) {
        return Status::NotFound("Key not found");
    }
    uint64_t ttl = 0;
    Status status = options.object_manager_->GetObjectTtl(ctx->slot_id, request->key(), &ttl);
    if (!status.ok()) {
        return status;
    }

    uint64_t currentTimeMs = GetCurrentTimeInMs();
    if (ttl > 0) {
        if (ttl <= currentTimeMs) {
            return Status::NotFound("Key not found");
        } else {
            response->set_ttl_ms(ttl - currentTimeMs);
        }
    }
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
