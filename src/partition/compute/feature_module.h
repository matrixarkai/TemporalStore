// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <absl/container/flat_hash_map.h>
#include <byte/base/closure.h>
#include <byte/include/macros.h>

#include <map>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/controller.h"
#include "common/logging.h"
#include "model/feature_model.h"
#include "model/schema.h"
#include "partition/cmd_context.h"
#include "partition/compute/cmd.h"
#include "partition/storage/object_manager.h"
#include "protocol/feature_module.pb.h"
#include "protocol/server.pb.h"

DECLARE_uint64(feature_max_size);

#define CMD_EXECUTOR_FEATURE_PREPARE_CTX_DEFAULT(cmd_type, cmd_name)                \
    CMD_EXECUTOR_PREPARE_CTX_DEFAULT(feature, Feature, feature, cmd_type, cmd_name, \
                                     model::ModelManager::GetModelId<FeatureModel>())
#define CMD_EXECUTOR_FEATURE_EXECUTE(cmd_type, cmd_name) \
    CMD_EXECUTOR_EXECUTE(feature, Feature, feature, cmd_type, cmd_name)

#define PREPARE_ARGUMENT_CHECK(format, schema, response)                             \
    do {                                                                             \
        if (format != "json" && format != "protobuf") {                              \
            LOG_ERROR("Format should be json or protobuf").put("Format is", format); \
            return Status::InvalidArgument("Format should be json or protobuf");     \
        }                                                                            \
        if (schema == nullptr || schema->Of() == nullptr) {                          \
            return Status::NotFound("Schema not defined yet");                       \
        }                                                                            \
    } while (false)

namespace bcache2 {
namespace partition {

using model::FeatureModel;

class ObjectManager;

class FeatureModule {
 public:
    static void ParseFilter(const ModuleManager::Options& options,
                            const feature::QueryRequest* request,
                            std::map<std::string, std::function<bool(uint64_t)>>* filter_map);
    static bool Filter(const ModuleManager::Options& options, const std::string& entry,
                       const std::map<std::string, std::function<bool(uint64_t)>>& filter_map);

 private:
    DISALLOW_INSTANTIATE(FeatureModule);
};

CMD_EXECUTOR_FEATURE_PREPARE_CTX_DEFAULT(Add, add)
CMD_EXECUTOR_FEATURE_EXECUTE(Add, add) {
    if (!ctx->object) {
        Status status = options.object_manager_->NewObject(
            ctx->slot_id, model::ModelManager::GetModelId<FeatureModel>(), request->key(),
            &ctx->object, false);
        BYTE_ASSERT(status.ok()) << status;
    }
    FeatureModel* model = ctx->object.Model<FeatureModel>();

    LOG_CALL_DEBUG();
    feature::Point feature_item;

    auto write_policy = request->policy();
    switch (write_policy) {
    case feature::UPSERT:
        for (int i = 0; i < request->point_list_size(); ++i) {
            feature_item = request->point_list(i);
            model->OrSet().Add(ctx, feature_item.ts(), std::move(feature_item.value()));
        }

        break;
    case feature::FIRST:
        for (int i = 0; i < request->point_list_size(); ++i) {
            feature_item = request->point_list(i);
            if (model->OrSet().Get(feature_item.ts())) {
                continue;
            } else {
                model->OrSet().Add(ctx, feature_item.ts(), std::move(feature_item.value()));
            }
        }
        break;
    case feature::UPDATE:
        for (int i = 0; i < request->point_list_size(); ++i) {
            feature_item = request->point_list(i);
            if (model->OrSet().Get(feature_item.ts())) {
                model->OrSet().Add(ctx, feature_item.ts(), std::move(feature_item.value()));
            } else {
                continue;
            }
        }
        break;
    default:
        return Status::InvalidArgument("Invalid write policy");
    }

    model->OrSet().DelBegins(ctx, FLAGS_feature_max_size);
    return Status::OK();
}

CMD_EXECUTOR_FEATURE_PREPARE_CTX_DEFAULT(Query, query)
CMD_EXECUTOR_FEATURE_EXECUTE(Query, query) {
    if (!ctx->object) {
        return Status::NotFound("Key not found");
    }
    FeatureModel* model = ctx->object.Model<FeatureModel>();

    auto format = request->format();
    PREPARE_ARGUMENT_CHECK(format, options.schema, response);

    std::map<std::string, std::function<bool(uint64_t)>> filter_map;
    FeatureModule::ParseFilter(options, request, &filter_map);

    auto start = request->start_ts();
    auto end = request->end_ts();
    auto max_num = request->count();

    auto filter_closure = [options, filter_map, response](const uint64_t key,
                                                          const std::string& val) mutable {
        if (FeatureModule::Filter(options, val, filter_map)) {
            auto pt = response->add_point_list();
            pt->set_ts(key);
            pt->set_value(std::move(val));
        }
    };
    model->OrSet().Query(start, end, max_num, filter_closure);
    response->set_key(request->key());
    return Status::OK();
}

void FeatureModule::ParseFilter(const ModuleManager::Options& options,
                                const feature::QueryRequest* request,
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

bool FeatureModule::Filter(const ModuleManager::Options& options, const std::string& entry,
                           const std::map<std::string, std::function<bool(uint64_t)>>& filter_map) {
    auto message = options.schema->Of();
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

}  // namespace partition
}  // namespace bcache2
