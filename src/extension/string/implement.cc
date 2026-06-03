// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/cmd_manager.h"
#include "common/macros.h"
#include "common/metrics.h"
#include "extension/string/interface.pb.h"
#include "model/raw_model.h"
#include "model/string_model.h"
#include "partition/compute/execute_env.h"
#include "partition/metrics.h"

namespace bcache2 {
namespace str2 {

struct StringMetrics : public ModuleMetrics {
    explicit StringMetrics(MetricsManager* metrics_manager) : ModuleMetrics(metrics_manager) {
        string_get_failure_counter =
            metrics_manager->Get<MetricsEnv::Counter>("module.string.get.failure.counter", {});
        string_set_size = metrics_manager->Get<MetricsEnv::Histogram>("module.string.set.size", {});
    }
    std::unique_ptr<MetricsEnv::CounterHolder> string_get_failure_counter;
    std::unique_ptr<MetricsEnv::HistogramHolder> string_set_size;
};
REGISTER_MODULE_METRICS(STRING, StringMetrics);

Status Set(ExecuteEnv* env, const SetRequest& request, SetResponse* response) {
    if (request.nx_flag() || request.xx_flag()) {
        if (request.nx_flag() && request.xx_flag()) {
            return Status::InvalidArgument("nx_flag and xx_flag both exist");
        }
        ObjectHandle<model::StringModel> object;
        Status status = env->GetObject(request.key(), &object);
        if (!status.ok() && !status.IsNotFound()) {
            return status;
        } else if (request.nx_flag() && status.ok()) {
            return Status::AlreadyExists("nx_flag requires object not exist");
        } else if (request.xx_flag() && status.IsNotFound()) {
            return Status::NotFound("xx_flag requires object exist");
        }
    }

    ObjectHandle<model::StringModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }
    uint64_t ttl = object.Ttl();
    object->SetValue(nullptr, request.value(), ttl);
    env->GetModuleMetrics<StringMetrics>()->string_set_size->get()->Set(request.value().size());
    return Status::OK();
}
REGISTER_FUNCTION(STRING, SET, Set, Write);

Status Setex(ExecuteEnv* env, const SetexRequest& request, SetexResponse* response) {
    ObjectHandle<model::StringModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }
    uint64_t ttl_ms = request.ttl_ms() + GetCurrentTimeInMs();
    object->SetValue(nullptr, request.value(), ttl_ms);
    object.SetTtl(ttl_ms);
    return Status::OK();
}
REGISTER_FUNCTION(STRING, SETEX, Setex, Write);

Status Get(ExecuteEnv* env, const GetRequest& request, GetResponse* response) {
    ObjectHandle<model::StringModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        env->GetModuleMetrics<StringMetrics>()->string_get_failure_counter->get()->Increment();
        return status;
    }

    std::string ret = object->GetValue();
    *response->mutable_value() = std::move(ret);
    return Status::OK();
}
REGISTER_FUNCTION(STRING, GET, Get, Read);

}  // namespace str2
}  // namespace bcache2
