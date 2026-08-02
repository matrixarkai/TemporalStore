// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include <stdlib.h>

#include "common/cmd_manager.h"
#include "common/macros.h"
#include "extension/hash/interface.pb.h"
#include "model/hash_model.h"
#include "partition/compute/execute_env.h"

namespace bcache2 {
namespace hash2 {

struct HashMetrics : public ModuleMetrics {
    explicit HashMetrics(MetricsManager* metrics_manager) : ModuleMetrics(metrics_manager) {
        mget_batch_field_num =
            metrics_manager->Get<MetricsEnv::Histogram>("module.hash.mget_batch_field_num", {});
        mset_batch_field_num =
            metrics_manager->Get<MetricsEnv::Histogram>("module.hash.mset_batch_field_num", {});
        getall_field_num =
            metrics_manager->Get<MetricsEnv::Histogram>("module.hash.getall_field_num", {});
    }
    std::unique_ptr<MetricsEnv::HistogramHolder> mget_batch_field_num;
    std::unique_ptr<MetricsEnv::HistogramHolder> mset_batch_field_num;
    std::unique_ptr<MetricsEnv::HistogramHolder> getall_field_num;
};
REGISTER_MODULE_METRICS(HASH, HashMetrics);

Status Set(ExecuteEnv* env, const SetRequest& request, SetResponse* response) {
    ObjectHandle<model::HashModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    object->OrSet().Set(nullptr, request.field(), request.value());
    return Status::OK();
}
REGISTER_FUNCTION(HASH, SET, Set, Write);

Status Get(ExecuteEnv* env, const GetRequest& request, GetResponse* response) {
    ObjectHandle<model::HashModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    status = object->OrSet().Get(request.field(), response->mutable_value());
    if (!status.ok() && !status.IsNotFound()) {
        return status;
    }

    if (status.IsNotFound()) {
        response->set_exist(false);
    } else {
        response->set_exist(true);
    }
    return Status::OK();
}
REGISTER_FUNCTION(HASH, GET, Get, Read);

Status Del(ExecuteEnv* env, const DelRequest& request, DelResponse* response) {
    ObjectHandle<model::HashModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    status = object->OrSet().Del(nullptr, request.field());
    if (!status.ok()) {
        return status;
    }

    if (object->OrSet().Size() == 0) {
        status = env->DeleteObject(request.key());
        BYTE_ASSERT(status.ok() || status.IsNotFound()) << status;
    }
    return Status::OK();
}
REGISTER_FUNCTION(HASH, DEL, Del, Write);

Status MGet(ExecuteEnv* env, const MGetRequest& request, MGetResponse* response) {
    ObjectHandle<model::HashModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    env->GetModuleMetrics<HashMetrics>()->mget_batch_field_num->get()->Set(request.fields_size());
    for (int i = 0; i < request.fields_size(); ++i) {
        bool* exist = response->mutable_exists()->Add();
        std::string* value = response->mutable_values()->Add();
        Status status = object->OrSet().Get(request.fields(i), value);
        if (status.IsNotFound()) {
            *exist = false;
            continue;
        }
        *exist = true;
        BYTE_ASSERT(status.ok()) << "invalid status " << status;
    }
    return Status::OK();
}
REGISTER_FUNCTION(HASH, MGET, MGet, Read);

Status MSet(ExecuteEnv* env, const MSetRequest& request, MSetResponse* response) {
    if (request.fields_size() != request.values_size()) {
        return Status::InvalidArgument("fields_size and values_size not match");
    }

    ObjectHandle<model::HashModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    env->GetModuleMetrics<HashMetrics>()->mset_batch_field_num->get()->Set(request.fields_size());
    for (int i = 0; i < request.fields_size(); ++i) {
        Status status = object->OrSet().Set(nullptr, request.fields(i), request.values(i));
        BYTE_ASSERT(status.ok());
    }
    return Status::OK();
}
REGISTER_FUNCTION(HASH, MSET, MSet, Write);

Status IncrBy(ExecuteEnv* env, const IncrByRequest& request, IncrByResponse* response) {
    ObjectHandle<model::HashModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    std::string value;
    int64_t incr = request.increment(), old_value = 0, new_value = 0;
    status = object->OrSet().Get(request.field(), &value);
    if (status.ok()) {
        char* eptr;
        errno = 0;
        old_value = strtoll(value.c_str(), &eptr, 10);
        if ((errno == ERANGE && (old_value == LLONG_MAX || old_value == LLONG_MIN)) ||
            (errno != 0 && old_value == 0) || *eptr != '\0') {
            return Status::Unmatched("hash value is not an integer or out of range");
        }
    }
    if ((incr < 0 && old_value < 0 && incr < (LLONG_MIN - old_value)) ||
        (incr > 0 && old_value > 0 && incr > (LLONG_MAX - old_value))) {
        return Status::OutOfRange("increment or decrement would overflow");
    }
    new_value = old_value + incr;
    status = object->OrSet().Set(nullptr, request.field(), std::to_string(new_value));
    BYTE_ASSERT(status.ok());
    response->set_value(new_value);
    return Status::OK();
}
REGISTER_FUNCTION(HASH, INCRBY, IncrBy, Write);

Status GetAll(ExecuteEnv* env, const GetAllRequest& request, GetAllResponse* response) {
    ObjectHandle<model::HashModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    object->OrSet().Scan([response](const std::string& key, const std::string& value) {
        *response->add_fields() = key;
        *response->add_values() = value;
        return true;
    });
    BYTE_ASSERT(response->fields_size() == response->values_size());

    env->GetModuleMetrics<HashMetrics>()->getall_field_num->get()->Set(response->fields_size());
    return Status::OK();
}
REGISTER_FUNCTION(HASH, GETALL, GetAll, Read);

Status Len(ExecuteEnv* env, const LenRequest& request, LenResponse* response) {
    ObjectHandle<model::HashModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }

    response->set_len(object->OrSet().Size());
    return Status::OK();
}
REGISTER_FUNCTION(HASH, LEN, Len, Read);

}  // namespace hash2
}  // namespace bcache2
