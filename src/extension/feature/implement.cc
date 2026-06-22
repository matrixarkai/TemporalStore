// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/cmd_manager.h"
#include "common/macros.h"
#include "common/module_config.h"
#include "extension/feature/interface.pb.h"
#include "model/feature_model.h"
#include "partition/compute/execute_env.h"

DECLARE_uint64(feature_max_size);

namespace bcache2 {
namespace feature2 {

void ParseFilter(const QueryRequest* request,
                 std::map<std::string, std::function<bool(uint64_t)>>* filter_map) {
    auto filters = request->filters();
    for (auto& filter : filters) {
        std::string field_key, op_code, val;
        uint64_t op_val = 0, index = 0;
        size_t pos = 0;
        std::string space_delimiter = " ";
        while ((pos = filter.find(space_delimiter)) != std::string::npos) {
            switch (index) {
            case 0:
                field_key = filter.substr(0, pos);
                break;
            case 1:
                op_code = filter.substr(0, pos);
                break;
            default:
                break;
            }
            index = index + 1;
            filter.erase(0, pos + space_delimiter.length());
        }

        char* end;
        op_val = std::strtoull(filter.c_str(), &end, 10);

        static const std::unordered_map<std::string, int> string_to_case{
            {"=", 1}, {"!=", 2}, {">", 3}, {"<", 4}};
        switch (string_to_case.at(op_code)) {
        case 1:
            (*filter_map)[field_key] = [op_val](uint64_t val) -> bool { return val == op_val; };
            break;
        case 2:
            (*filter_map)[field_key] = [op_val](uint64_t val) -> bool { return val != op_val; };
            break;
        case 3:
            (*filter_map)[field_key] = [op_val](uint64_t val) -> bool { return val > op_val; };
            break;
        case 4:
            (*filter_map)[field_key] = [op_val](uint64_t val) -> bool { return val < op_val; };
            break;
        default:
            break;
        }
    }
}

bool Filter(ExecuteEnv* env, const std::string& entry,
            const std::map<std::string, std::function<bool(uint64_t)>>& filter_map) {
    auto message = env->GetModuleCustomConfig<Schema>()->Of();
    bool ret = true;
    uint64_t field_val;
    auto iter = filter_map.begin();
    while (iter != filter_map.end()) {
        auto field_key = iter->first;
        auto field_filter = iter->second;
        auto instance = InstanceReader(message).Decode(entry);
        if (!instance) {
            return false;
        }
        auto pd = instance->Instance()->GetDescriptor();
        auto field_descriptor = pd->FindFieldByName(field_key);
        if (!field_descriptor) {
            return false;
        }
        switch (field_descriptor->type()) {
        case google::protobuf::FieldDescriptor::TYPE_UINT32:
            field_val = (uint64_t)instance->GetUInt32(field_key);
            ret = ret && field_filter(field_val);
            break;

        case google::protobuf::FieldDescriptor::TYPE_UINT64:
            field_val = instance->GetUInt64(field_key);
            ret = ret && field_filter(field_val);
            break;
        default:
            break;
        }
        iter++;
    }
    return ret;
}

Status Add(ExecuteEnv* env, const AddRequest& request, AddResponse* response) {
    ObjectHandle<model::FeatureModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    if (!status.ok()) {
        return status;
    }
    uint64_t total = object->OrSet().Size() + request.point_list_size();
    if (total > 100000UL) {
        LOG_ERROR_SAMPLE("Feature add size bigger than 100000")
            .put("PartitionID", env->GetPartitionID())
            .put("Key", request.key())
            .put("Total", total);
        return Status::InvalidArgument(request.key() + " size bigger than 100000");
    }

    Point feature_item;
    auto write_policy = request.policy();
    switch (write_policy) {
    case UPSERT:
        for (int i = 0; i < request.point_list_size(); ++i) {
            feature_item = request.point_list(i);
            object->OrSet().Add(nullptr, feature_item.ts(), std::move(feature_item.value()));
        }
        break;
    case FIRST:
        for (int i = 0; i < request.point_list_size(); ++i) {
            feature_item = request.point_list(i);
            if (object->OrSet().Get(feature_item.ts())) {
                continue;
            } else {
                object->OrSet().Add(nullptr, feature_item.ts(), std::move(feature_item.value()));
            }
        }
        break;
    case UPDATE:
        for (int i = 0; i < request.point_list_size(); ++i) {
            feature_item = request.point_list(i);
            if (object->OrSet().Get(feature_item.ts())) {
                object->OrSet().Add(nullptr, feature_item.ts(), std::move(feature_item.value()));
            } else {
                continue;
            }
        }
        break;
    default:
        return Status::InvalidArgument("Invalid write policy");
    }

    object->OrSet().DelBegins(nullptr, FLAGS_feature_max_size);
    return Status::OK();
}
REGISTER_FUNCTION(FEATURE, ADD, Add, Write);

Status Query(ExecuteEnv* env, const QueryRequest& request, QueryResponse* response) {
    ObjectHandle<model::FeatureModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            response->set_key(request.key());
            return Status::OK();
        }
        return status;
    }

    auto format = request.format();
    if (format != "json" && format != "protobuf") {
        LOG_ERROR("Format should be json or protobuf").put("Format is", format);
        return Status::InvalidArgument("Format should be json or protobuf");
    }

    if (env->GetModuleCustomConfig<Schema>() == nullptr ||
        env->GetModuleCustomConfig<Schema>()->Of() == nullptr) {
        return Status::NotFound("Schema not defined yet");
    }

    std::map<std::string, std::function<bool(uint64_t)>> filter_map;
    ParseFilter(&request, &filter_map);

    auto start = request.start_ts();
    auto end = request.end_ts();
    auto max_num = request.count();

    auto filter_closure = [&env, filter_map, response](const uint64_t key,
                                                       const std::string& val) mutable {
        if (Filter(env, val, filter_map)) {
            auto pt = response->add_point_list();
            pt->set_ts(key);
            pt->set_value(std::move(val));
        }
    };
    object->OrSet().Query(start, end, max_num, filter_closure);
    response->set_key(request.key());
    return Status::OK();
}
REGISTER_FUNCTION(FEATURE, QUERY, Query, Read);
REGISTER_MODULE_CONFIG_INIT(FEATURE, Schema);

}  // namespace feature2
}  // namespace bcache2
