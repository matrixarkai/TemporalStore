// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#include "common/cmd_manager.h"
#include "common/logging.h"
#include "common/macros.h"
#include "common/module_config.h"
#include "extension/ips/interface.pb.h"
#include "model/ips/ips_interface.h"
#include "model/ips/ips_table_manager.h"
#include "model/ips/utils.h"
#include "model/ips_model.h"
#include "model/model_manager.h"
#include "partition/compute/execute_env.h"
#include "protocol/config.pb.h"

namespace bcache2 {
namespace ips {

struct IPSMetrics : public ModuleMetrics {
    explicit IPSMetrics(MetricsManager* metrics_manager) : ModuleMetrics(metrics_manager) {
        ips_add_instance_num =
            metrics_manager->Get<MetricsEnv::Histogram>("module.ips.add_instance_num", {});
        ips_query_req_num =
            metrics_manager->Get<MetricsEnv::Histogram>("module.ips.query_req_num", {});
    }
    std::unique_ptr<MetricsEnv::HistogramHolder> ips_add_instance_num;
    std::unique_ptr<MetricsEnv::HistogramHolder> ips_query_req_num;
};
REGISTER_MODULE_METRICS(IPS, IPSMetrics);

class IPSTableSchema : public ModuleCustomConfig {
 public:
    IPSTableSchema() {}
    Status LoadCustomConfig(const CustomConfig& config) {
        if (config.value_case() == CustomConfig::kIpsConfig)
            return ips_table_manager_.Init(config.ips_config().value());
        else
            return ips_table_manager_.Init("");
    }

    Status UpdateCustomConfig(const CustomConfig& config) {
        if (config.value_case() == CustomConfig::kIpsConfig)
            return ips_table_manager_.Init(config.ips_config().value());
        else
            return ips_table_manager_.Init("");
    }

    ips::IpsTableSchemaManager ips_table_manager_;
};
REGISTER_MODULE_CONFIG_INIT(IPS, IPSTableSchema)

class IpsModule {
 public:
    static void AggregateInstances(ExecuteEnv* env, const ips::AddRequest* req,
                                   std::unordered_map<std::string, std::vector<ips::Instance>>* m) {
        using KeyType = std::tuple<int64_t, int16_t, int16_t, int16_t, int64_t>;
        struct KeyHash {
            std::size_t operator()(const KeyType& key) const {
                size_t v = 0;
                ips::hash_combine(&v, std::get<0>(key));
                ips::hash_combine(&v, std::get<1>(key));
                ips::hash_combine(&v, std::get<2>(key));
                ips::hash_combine(&v, std::get<3>(key));
                ips::hash_combine(&v, std::get<4>(key));
                return v;
            }
        };

        // preallocate memory before aggregation
        std::string root_key;
        {
            std::unordered_set<KeyType, KeyHash> root_with_ts_set;
            std::unordered_map<std::string, size_t> capacity_map;
            env->GetModuleMetrics<IPSMetrics>()->ips_add_instance_num->get()->Set(
                req->instance_list_size());
            for (int instance_idx = 0; instance_idx < req->instance_list_size(); instance_idx++) {
                auto& ins = req->instance_list(instance_idx);
                for (int feature_idx = 0; feature_idx < ins.feature_stat32_list_size();
                     feature_idx++) {
                    // for (auto& feature : ins.feature_stat32_list) {
                    auto& feature = ins.feature_stat32_list(feature_idx);
                    auto root_with_ts =
                        std::make_tuple(ins.uid(), ips::IPSInterface::CalcSlot(feature),
                                        ins.table(), ins.action_type(), ins.ts());
                    auto it = root_with_ts_set.find(root_with_ts);
                    if (it == root_with_ts_set.end()) {
                        ips::PutIPSKeyToTreeKey(ins.uid(), ips::IPSInterface::CalcSlot(feature),
                                                ins.table(), ins.action_type(), &root_key);
                        ++capacity_map[root_key];
                        root_with_ts_set.emplace(root_with_ts);
                    }
                }
            }
            for (auto& root_cap : capacity_map) {
                (*m)[root_cap.first].reserve(root_cap.second);
            }
        }

        std::unordered_map<KeyType, ips::Instance*, KeyHash> rootWithTs;
        for (int i_idx = 0; i_idx < req->instance_list_size(); i_idx++) {
            auto& ins = req->instance_list(i_idx);
            for (int f_idx = 0; f_idx < ins.feature_stat32_list_size(); f_idx++) {
                auto& feature = ins.feature_stat32_list(f_idx);
                auto rootTs = std::make_tuple(ins.uid(), ips::IPSInterface::CalcSlot(feature),
                                              ins.table(), ins.action_type(), ins.ts());
                auto it = rootWithTs.find(rootTs);
                if (it == rootWithTs.end()) {
                    ips::PutIPSKeyToTreeKey(ins.uid(), ips::IPSInterface::CalcSlot(feature),
                                            ins.table(), ins.action_type(), &root_key);
                    auto& ins_vec = (*m)[root_key];
                    ins_vec.emplace_back();
                    ips::Instance* instance = &ins_vec.back();
                    instance->set_table(ins.table());
                    instance->set_action_type(ins.action_type());
                    instance->set_uid(ins.uid());
                    instance->set_ts(ins.ts());
                    auto ret = rootWithTs.emplace(rootTs, instance);
                    it = ret.first;
                }
                ips::FeatureStat32* f32 = it->second->add_feature_stat32_list();
                f32->CopyFrom(feature);
            }
        }
    }

    static void AggregateInstances(ExecuteEnv* env, const ips::LoadRequest* req,
                std::unordered_map<std::string, std::vector<ips::LoadInstance>>* m) {
        using KeyType = std::tuple<int64_t, int16_t, int16_t, int16_t, int64_t>;
        struct KeyHash {
            std::size_t operator()(const KeyType& key) const {
                size_t v = 0;
                ips::hash_combine(&v, std::get<0>(key));
                ips::hash_combine(&v, std::get<1>(key));
                ips::hash_combine(&v, std::get<2>(key));
                ips::hash_combine(&v, std::get<3>(key));
                ips::hash_combine(&v, std::get<4>(key));
                return v;
            }
        };

        // preallocate memory before aggregation
        std::string root_key;
        {
            std::unordered_set<KeyType, KeyHash> root_with_ts_set;
            std::unordered_map<std::string, size_t> capacity_map;
            env->GetModuleMetrics<IPSMetrics>()->ips_add_instance_num->get()->Set(
                req->instance_list_size());
            for (int instance_idx = 0; instance_idx < req->instance_list_size(); instance_idx++) {
                auto& ins = req->instance_list(instance_idx);
                for (int feature_idx = 0; feature_idx < ins.feature_stat32_list_size();
                     feature_idx++) {
                    // for (auto& feature : ins.feature_stat32_list) {
                    auto& feature = ins.feature_stat32_list(feature_idx);
                    auto root_with_ts =
                        std::make_tuple(ins.uid(), ips::IPSInterface::CalcSlot(feature),
                                        ins.table(), ins.action_type(), ins.max_ts());
                    auto it = root_with_ts_set.find(root_with_ts);
                    if (it == root_with_ts_set.end()) {
                        ips::PutIPSKeyToTreeKey(ins.uid(), ips::IPSInterface::CalcSlot(feature),
                                                ins.table(), ins.action_type(), &root_key);
                        ++capacity_map[root_key];
                        root_with_ts_set.emplace(root_with_ts);
                    }
                }
            }
            for (auto& root_cap : capacity_map) {
                (*m)[root_cap.first].reserve(root_cap.second);
            }
        }

        std::unordered_map<KeyType, ips::LoadInstance*, KeyHash> rootWithTs;
        for (int i_idx = 0; i_idx < req->instance_list_size(); i_idx++) {
            auto& ins = req->instance_list(i_idx);
            for (int f_idx = 0; f_idx < ins.feature_stat32_list_size(); f_idx++) {
                auto& feature = ins.feature_stat32_list(f_idx);
                auto rootTs = std::make_tuple(ins.uid(), ips::IPSInterface::CalcSlot(feature),
                                              ins.table(), ins.action_type(), ins.max_ts());
                auto it = rootWithTs.find(rootTs);
                if (it == rootWithTs.end()) {
                    ips::PutIPSKeyToTreeKey(ins.uid(), ips::IPSInterface::CalcSlot(feature),
                                            ins.table(), ins.action_type(), &root_key);
                    auto& ins_vec = (*m)[root_key];
                    ins_vec.emplace_back();
                    ips::LoadInstance* instance = &ins_vec.back();
                    instance->set_table(ins.table());
                    instance->set_action_type(ins.action_type());
                    instance->set_uid(ins.uid());
                    instance->set_max_ts(ins.max_ts());
                    auto ret = rootWithTs.emplace(rootTs, instance);
                    it = ret.first;
                }
                ips::FeatureStat32* f32 = it->second->add_feature_stat32_list();
                f32->CopyFrom(feature);
            }
        }
    }
};

template <class T>
std::string ConvertErrorCode(T* resp, const Status& s) {
    if (s.ok()) {
        resp->set_err_code(ips::ErrorCode::SUCCESS);
        return "Ok";
    } else if (s.IsInvalidArgument()) {
        resp->set_err_code(ips::ErrorCode::INVALID_ARG);
        return s.ToString();
    } else if (s.IsNotFound()) {
        resp->set_err_code(ips::ErrorCode::USER_NO_DATA);
        return "cur args does't match any data";
    } else if (s.IsUnavailable()) {
        resp->set_err_code(ips::ErrorCode::UPSTREAM_OVERRUN);
        return "your quota is run out, sleep and retry";
    } else if (s.IsInternal()) {
        resp->set_err_code(ips::ErrorCode::REDUCE_CLOSED);
        return s.ToString();
    } else {
        resp->set_err_code(ips::ErrorCode::ERROR);
        return s.ToString();
    }
}

Status Add(ExecuteEnv* env, const AddRequest& request, AddResponse* response) {
    Status ret;
    BYTE_DEFER({ response->set_error_desc(ConvertErrorCode<ips::AddResponse>(response, ret)); });

    std::string table_str = request.table();
    if (table_str.empty()) {
        ret = Status::InvalidArgument("table is empty");
        return ret;
    }
    if (request.instance_list_size() < 1) {
        ret = Status::InvalidArgument("empty instance list");
        return ret;
    }
    for (int i_idx = 0; i_idx < request.instance_list_size(); i_idx++) {
        auto& ins = request.instance_list(i_idx);
        if (ins.feature_stat32_list_size() < 1) {
            ret = Status::InvalidArgument("empty featurestat32 list");
            return ret;
        }
    }
    auto table_schema =
        env->GetModuleCustomConfig<IPSTableSchema>()->ips_table_manager_.GetTreeFactoryAndSchema(
            table_str);
    if (table_schema == nullptr) {
        ret = Status::NotFound("table not exist");
        LOG_ERROR("Table not exists").put("TableName", table_str);
        return ret;
    }

    std::unordered_map<std::string, std::vector<ips::Instance>> instances;
    IpsModule::AggregateInstances(env, &request, &instances);

    bool idempotent_add = false, is_replace = false;
    for (auto& ips_obj : instances) {
        std::string ips_obj_key = table_str + std::string("_") + ips_obj.first;
        ObjectHandle<model::IpsModel> object;
        ret = env->GetOrNewObject(ips_obj_key, &object);
        if (!ret.ok()) {
            return ret;
        }
        model::IpsModel* ips_model = object.operator->();
        ret = ips::IPSInterface::Add(nullptr, ips_obj.second, idempotent_add,
                                     ips_model, *table_schema, is_replace);
        if (!ret.IsOK()) {
            return ret;
        }
    }
    ret = Status::OK();
    return ret;
}
REGISTER_FUNCTION(IPS, ADD, Add, Write);

Status BatchQuery(ExecuteEnv* env,
            const BatchQueryRequest& request, BatchQueryResponse* response) {
    Status ret;
    BYTE_DEFER(
        { response->set_error_desc(ConvertErrorCode<ips::BatchQueryResponse>(response, ret)); });
    env->GetModuleMetrics<IPSMetrics>()->ips_query_req_num->get()->Set(request.reqs_size());
    if (request.reqs_size() < 1) {
        ret = Status::InvalidArgument("empty query request");
        return ret;
    }

    for (int req_idx = 0; req_idx < request.reqs_size(); req_idx++) {
        const ips::QueryRequest& req = request.reqs(req_idx);
        ips::QueryResponse* rsp = response->add_rsps();

        std::string table_str = req.table();
        uint64_t uid = req.uid();
        uint32_t table = req.filter().table();
        uint32_t slot = req.filter().slot();
        uint32_t action_type = req.filter().action_type();

        std::string ips_obj_key =
            table_str + std::string("_") + ips::IPSKeyToTreeKey(uid, slot, table, action_type);

        ObjectHandle<model::IpsModel> object;
        ret = env->GetOrNewObject(ips_obj_key, &object);
        if (!ret.ok()) {
            return ret;
        }
        auto table_schema = env->GetModuleCustomConfig<IPSTableSchema>()
                                ->ips_table_manager_.GetTreeFactoryAndSchema(table_str);
        if (table_schema == nullptr) {
            ret = Status::NotFound("table not exist");
            return ret;
        }
        uint8_t cur_tree_version;
        int64_t base_ts;
        model::IpsModel* ips_model = object.operator->();
        ips::IPSOperator::GetQueryTreeVersionAndBaseTs(ips_model, &cur_tree_version, &base_ts);
        ips::TableType tabletype = table_schema->GetTableType();
        ips::ReduceType reducetype = table_schema->GetReduceType();
        ret = ips::IPSInterface::SubQueryReqHandler(req, rsp, ips_model, tabletype, reducetype,
                                                    cur_tree_version, base_ts);
    }
    return ret.IsNotFound()?Status::OK():ret;
}
REGISTER_FUNCTION(IPS, BATCH_QUERY, BatchQuery, Read);

Status Load(ExecuteEnv* env, const LoadRequest& request, LoadResponse* response) {
    Status ret;
    BYTE_DEFER({ response->set_error_desc(ConvertErrorCode<ips::LoadResponse>(response, ret)); });
    std::string table_str = request.table();
    if (table_str.empty()) {
        ret = Status::InvalidArgument("table is empty");
        return ret;
    }
    if (request.instance_list_size() < 1) {
        ret = Status::InvalidArgument("empty instance list");
        return ret;
    }
    for (int i_idx = 0; i_idx < request.instance_list_size(); i_idx++) {
        auto& ins = request.instance_list(i_idx);
        if (ins.feature_stat32_list_size() < 1) {
            ret = Status::InvalidArgument("empty featurestat32 list");
            return ret;
        }
    }
    auto table_schema =
        env->GetModuleCustomConfig<IPSTableSchema>()->ips_table_manager_.GetTreeFactoryAndSchema(
            table_str);
    if (table_schema == nullptr) {
        ret = Status::NotFound("table not exist");
        LOG_ERROR("Table not exists").put("TableName", table_str);
        return ret;
    }

    std::unordered_map<std::string, std::vector<ips::LoadInstance>> instances;
    IpsModule::AggregateInstances(env, &request, &instances);

    bool idempotent_add = false, is_replace = false;
    for (auto& ips_obj : instances) {
        std::string ips_obj_key = table_str + std::string("_") + ips_obj.first;
        ObjectHandle<model::IpsModel> object;
        ret = env->GetOrNewObject(ips_obj_key, &object);
        if (!ret.ok()) {
            return ret;
        }
        model::IpsModel* ips_model = object.operator->();
        ret = ips::IPSInterface::Load(nullptr, ips_obj.second, idempotent_add,
                                     ips_model, *table_schema, is_replace);
        if (!ret.IsOK()) {
            return ret;
        }
    }
    ret = Status::OK();
    return ret;
}
REGISTER_FUNCTION(IPS, LOAD, Load, Write);

Status Del(ExecuteEnv* env,
            const DelRequest& req, DelResponse* response) {
    Status ret;
    BYTE_DEFER(
        { response->set_error_desc(ConvertErrorCode<ips::DelResponse>(response, ret)); });
    int64_t uid = req.uid();
    std::string table_str = req.table();
    uint32_t table_int = req.filter().table();
    uint32_t slot = req.filter().slot();
    uint32_t action_type = req.filter().action_type();
    std::string ips_obj_key =
        table_str + std::string("_") + ips::IPSKeyToTreeKey(uid, slot, table_int, action_type);
    ObjectHandle<model::IpsModel> object;
    auto table_schema = env->GetModuleCustomConfig<IPSTableSchema>()
                                ->ips_table_manager_.GetTreeFactoryAndSchema(table_str);
    if (table_schema == nullptr) {
        ret = Status::NotFound("table not exist");
        return ret;
    }
    ret = env->DeleteObject(ips_obj_key);
    if (!ret.ok()) {
        return ret;
    }
    return Status::OK();
}
REGISTER_FUNCTION(IPS, DEL, Del, Write);

}  // namespace ips
}  // namespace bcache2
