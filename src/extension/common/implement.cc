// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/cmd_manager.h"
#include "common/macros.h"
#include "extension/common/interface.pb.h"
#include "extension/modules.pb.h"
#include "partition/compute/execute_env.h"
#include "partition/storage/object.h"

namespace bcache2 {
namespace common2 {

Status DelObject(ExecuteEnv* env, const DelObjectRequest& request, DelObjectResponse* response) {
    return env->DeleteObject(request.key());
}
REGISTER_FUNCTION(COMMON, DEL_OBJECT, DelObject, Write);

Status Expire(ExecuteEnv* env, const ExpireRequest& request, ExpireResponse* response) {
    ObjectHandle<partition::Object> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }
    uint64_t ttl = request.ttl_ms() + GetCurrentTimeInMs();
    object.SetTtl(ttl);
    return Status::OK();
}
REGISTER_FUNCTION(COMMON, EXPIRE, Expire, Write);

Status Ttl(ExecuteEnv* env, const TtlRequest& request, TtlResponse* response) {
    ObjectHandle<partition::Object> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    uint64_t ttl = object.Ttl();
    uint64_t currentTimeMs = GetCurrentTimeInMs();
    if (ttl > 0) {
        if (ttl <= currentTimeMs) {
            return Status::NotFound("key not found");
        } else {
            response->set_ttl_ms(ttl - currentTimeMs);
        }
    }
    return Status::OK();
}
REGISTER_FUNCTION(COMMON, TTL, Ttl, Read);

}  // namespace common2
}  // namespace bcache2
