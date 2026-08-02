// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/strings/numbers.h>
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


#define CMD_EXECUTOR_HASH_FUNC_ACRONYM(is_executor, func_name, cmd_type, cmd_name, enum_case)      \
    Status func_name(const ModuleManager::Options& options, CmdContext* ctx,                       \
                     const hash::cmd_type##Request* request,                                      \
                     hash::cmd_type##Response* response);                                         \
    Status CMD_EXECUTOR_NAME_INTERNAL(func_name)(const ModuleManager::Options& options,            \
                                                 CmdContext* ctx, const CmdRequest* request,       \
                                                 CmdResponse* response) {                          \
        return func_name(                                                                          \
            options, ctx, &request->hash_request().cmd_name##_request(),                           \
            response->mutable_hash_response()->mutable_##cmd_name##_response());                   \
    }                                                                                              \
    INITIALIZE({                                                                                   \
        ModuleManager::Ref().RegisterModuleCmdExecutor(                                           \
            static_cast<size_t>(CmdRequest::kHashRequest),                                        \
            static_cast<size_t>(hash::HashModuleRequest::enum_case), #cmd_type, #cmd_name,        \
            [](const CmdRequest* request) {                                                        \
                return static_cast<size_t>(request->hash_request().cmd_case());                    \
            },                                                                                     \
            CMD_EXECUTOR_NAME_INTERNAL(func_name), is_executor);                                   \
    });                                                                                            \
    Status func_name(const ModuleManager::Options& options, CmdContext* ctx,                       \
                     const hash::cmd_type##Request* request,                                      \
                     hash::cmd_type##Response* response)

#define CMD_EXECUTOR_HASH_PREPARE_CTX_ACRONYM(cmd_type, cmd_name, enum_case)                      \
    CMD_EXECUTOR_HASH_FUNC_ACRONYM(false, CMD_EXECUTOR_PREPARER_NAME(Hash, cmd_type), cmd_type,    \
                                   cmd_name, enum_case) {                                         \
        auto& key = request->key();                                                               \
        ctx->slot_id = hash_func(key.data(), key.size());                                         \
        ctx->key = absl::string_view(key.data(), key.size());                                     \
        ctx->model_id = model::ModelManager::GetModelId<HashModel>();                             \
        return Status::OK();                                                                      \
    }

#define CMD_EXECUTOR_HASH_EXECUTE_ACRONYM(cmd_type, cmd_name, enum_case)                          \
    CMD_EXECUTOR_HASH_FUNC_ACRONYM(true, CMD_EXECUTOR_EXECUTOR_NAME(Hash, cmd_type), cmd_type,     \
                                   cmd_name, enum_case)

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


CMD_EXECUTOR_HASH_PREPARE_CTX_ACRONYM(MGet, mget, kMgetRequest)
CMD_EXECUTOR_HASH_EXECUTE_ACRONYM(MGet, mget, kMgetRequest) {
    HashModel* model = ctx->object ? ctx->object.Model<HashModel>() : nullptr;
    for (const auto& field : request->fields()) {
        std::string value;
        if (model != nullptr && model->OrSet().Get(field, &value).ok()) {
            response->add_exists(true);
            response->add_values(value);
        } else {
            response->add_exists(false);
            response->add_values("");
        }
    }
    return Status::OK();
}

CMD_EXECUTOR_HASH_PREPARE_CTX_ACRONYM(MSet, mset, kMsetRequest)
CMD_EXECUTOR_HASH_EXECUTE_ACRONYM(MSet, mset, kMsetRequest) {
    if (request->fields_size() != request->values_size()) {
        return Status::InvalidArgument("fields and values size mismatch");
    }
    if (!ctx->object) {
        Status status = options.object_manager_->NewObject(
            ctx->slot_id, model::ModelManager::GetModelId<HashModel>(), request->key(),
            &ctx->object, false);
        BYTE_ASSERT(status.ok()) << status;
    }
    HashModel* model = ctx->object.Model<HashModel>();
    for (int i = 0; i < request->fields_size(); ++i) {
        model->OrSet().Set(ctx, request->fields(i), request->values(i));
    }
    return Status::OK();
}

CMD_EXECUTOR_HASH_PREPARE_CTX_ACRONYM(GetAll, getall, kGetallRequest)
CMD_EXECUTOR_HASH_EXECUTE_ACRONYM(GetAll, getall, kGetallRequest) {
    if (!ctx->object) {
        return Status::NotFound("Key not found");
    }
    HashModel* model = ctx->object.Model<HashModel>();
    model->OrSet().Scan([&](const std::string& field, const std::string& value) {
        response->add_fields(field);
        response->add_values(value);
        return true;
    });
    return Status::OK();
}

CMD_EXECUTOR_HASH_PREPARE_CTX_DEFAULT(Len, len)
CMD_EXECUTOR_HASH_EXECUTE(Len, len) {
    if (!ctx->object) {
        return Status::NotFound("Key not found");
    }
    HashModel* model = ctx->object.Model<HashModel>();
    response->set_len(static_cast<uint64_t>(model->OrSet().Size()));
    return Status::OK();
}

CMD_EXECUTOR_HASH_PREPARE_CTX_ACRONYM(IncrBy, incrby, kIncrbyRequest)
CMD_EXECUTOR_HASH_EXECUTE_ACRONYM(IncrBy, incrby, kIncrbyRequest) {
    if (!ctx->object) {
        Status status = options.object_manager_->NewObject(
            ctx->slot_id, model::ModelManager::GetModelId<HashModel>(), request->key(),
            &ctx->object, false);
        BYTE_ASSERT(status.ok()) << status;
    }
    HashModel* model = ctx->object.Model<HashModel>();
    int64_t current = 0;
    std::string raw;
    Status get_status = model->OrSet().Get(request->field(), &raw);
    if (get_status.ok() && !absl::SimpleAtoi(raw, &current)) {
        return Status::InvalidArgument("hash value is not an integer");
    }
    const int64_t next = current + request->increment();
    model->OrSet().Set(ctx, request->field(), std::to_string(next));
    response->set_value(next);
    return Status::OK();
}

}  // namespace partition
}  // namespace bcache2
