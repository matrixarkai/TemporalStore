// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/cmd_manager.h"
#include "common/macros.h"
#include "extension/set/interface.pb.h"
#include "model/raw_model.h"
#include "partition/compute/execute_env.h"

namespace bcache2 {
namespace set {

Status SAdd(ExecuteEnv* env, const SAddRequest& request, SAddResponse* response) {
    ObjectHandle<model::RawModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    object->OrSet()->Put(nullptr, request.member(), "");
    return Status::OK();
}
REGISTER_FUNCTION(SET, SADD, SAdd, Write);

Status SMembers(ExecuteEnv* env, const SMembersRequest& request, SMembersResponse* response) {
    ObjectHandle<model::RawModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    for (auto it = object->OrSet()->Begin(); it != object->OrSet()->End(); ++it) {
        response->add_members(it.First());
    }

    return Status::OK();
}
REGISTER_FUNCTION(SET, SMEMBERS, SMembers, Read);

}  // namespace set
}  // namespace bcache2
