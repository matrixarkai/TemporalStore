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

    uint64_t added = 0;
    for (int i = 0; i < request.members_size(); ++i) {
        if (object->OrSet()->Find(request.members(i)) == object->OrSet()->End()) {
            ++added;
        }
        object->OrSet()->Put(nullptr, request.members(i), "");
    }
    response->set_added(added);
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

Status SRem(ExecuteEnv* env, const SRemRequest& request, SRemResponse* response) {
    ObjectHandle<model::RawModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        response->set_removed(0);
        return Status::OK();
    }

    uint64_t removed = 0;
    for (int i = 0; i < request.members_size(); ++i) {
        if (object->OrSet()->Find(request.members(i)) != object->OrSet()->End()) {
            object->OrSet()->Del(nullptr, request.members(i));
            ++removed;
        }
    }
    response->set_removed(removed);
    return Status::OK();
}
REGISTER_FUNCTION(SET, SREM, SRem, Write);

Status SCard(ExecuteEnv* env, const SCardRequest& request, SCardResponse* response) {
    ObjectHandle<model::RawModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        response->set_len(0);
        return Status::OK();
    }

    response->set_len(object->OrSet()->Size());
    return Status::OK();
}
REGISTER_FUNCTION(SET, SCARD, SCard, Read);

Status SIsMember(ExecuteEnv* env, const SIsMemberRequest& request,
                 SIsMemberResponse* response) {
    ObjectHandle<model::RawModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        response->set_exist(false);
        return Status::OK();
    }

    response->set_exist(object->OrSet()->Find(request.member()) != object->OrSet()->End());
    return Status::OK();
}
REGISTER_FUNCTION(SET, SISMEMBER, SIsMember, Read);

}  // namespace set
}  // namespace bcache2
