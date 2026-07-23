// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <byte/include/macros.h>

#include <algorithm>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/cmd_manager.h"
#include "common/logging.h"
#include "common/macros.h"
#include "extension/modules.pb.h"
#include "extension/risk/define.h"
#include "extension/risk/global.h"
#include "extension/risk/interface.pb.h"
#include "extension/risk/metrics_log.h"
#include "extension/risk/window.h"
#include "model/risk_cpc_model.h"
#include "model/risk_hash_model.h"
#include "model/string_model.h"
#include "partition/compute/execute_env.h"

const int FirstAndSetTimestampSize = 10;
const int64_t NeedLogLongCostNs = 1 * 1000 * 1000;  // 1ms
const double kRiskKeyTTLBase = 1;

namespace bcache2 {
namespace risk {

Status CountHset(ExecuteEnv* env, RiskTimerLogger *timer,
                 const risk::HsetRequest& request, risk::HsetResponse* response,
                 const model::RiskUpsertFunc& riskUpsertFunc) {
    ObjectHandle<model::RiskHashModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    timer->AddCheckPoint("load_data");
    if (!status.ok()) {
        return status;
    }

    // 设置过期时间, 过期时间点的毫秒级时间戳, 过期时间延长 ttl 的 kRiskKeyTTLBase 倍
    object.SetTtl(bcache2::GetCurrentTimeInMs() + int64_t(request.ttl() * kRiskKeyTTLBase * 1000));

    timer->AddCheckPoint("set_ttl");
    std::vector<std::string> fields;
    std::vector<int64_t> values;
    time_t timestamp =
        risk_tool::fixTimeWithPrecision(request.occur_time(), request.key(), request.precision());
    fields.emplace_back(std::to_string(request.precision()) + std::to_string(timestamp));
    values.emplace_back(std::strtoll(request.value().c_str(), nullptr, 10));

    timer->AddCheckPoint("pre_handle");
    status = object->OrSet().BatchUpsert(nullptr, timer,
        fields, values, request.ttl(), riskUpsertFunc, request.uuid());

    if (UNLIKELY(status.IsRiskAlreadyHandled())) {
        LOG(INFO) << "[risk]request is already handled, key="
            << request.key() << " uuid=" << request.uuid();
        status = Status::OK();
    }
    response->set_err_code(status.errorcode());
    if (!status.ok()) {
        response->set_err_msg(status.ToString());
    }
    return status;
}
Status ChangeHset(ExecuteEnv* env, RiskTimerLogger *timer,
                  const risk::HsetRequest& request, risk::HsetResponse* response) {
    ObjectHandle<model::RiskHashModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    timer->AddCheckPoint("load_data");
    if (!status.ok()) {
        return status;
    }
    // 设置过期时间, 过期时间点的毫秒级时间戳, 过期时间延长 ttl 的 kRiskKeyTTLBase 倍
    object.SetTtl(bcache2::GetCurrentTimeInMs() + int64_t(request.ttl() * kRiskKeyTTLBase * 1000));

    timer->AddCheckPoint("set_ttl");
    std::string field = "";
    time_t timestamp =
        risk_tool::fixTimeWithPrecision(request.occur_time(), request.key(), request.precision());
    timer->AddCheckPoint("pre_handle");
    status = object->OrSet().CompareAndIncrBy(nullptr, timer,
        request.value(), std::to_string(request.precision()) + std::to_string(timestamp),
        request.ttl(), request.uuid());

    if (UNLIKELY(status.IsRiskAlreadyHandled())) {
        LOG(INFO) << "[risk]request is already handled, key="
            << request.key() << " uuid=" << request.uuid();
        status = Status::OK();
    }
    if (!status.ok()) {
        response->set_err_msg(status.ToString());
    }
    return status;
}

Status CountQuery(ExecuteEnv* env, RiskTimerLogger *timer,
                  ObjectHandle<model::RiskHashModel>* object,
                  const risk::Window& window, const risk::RiskPrecision riskPrecision,
                  risk::HqueryResponse* response, const model::RiskComputeFunc& riskComputeFunc) {
    std::vector<risk_tool::RiskQueryRange> queryRanges;
    int ret = risk_tool::getWindows(window, riskPrecision, &queryRanges, false, 0);
    if (ret != 0) {
        return Status::InvalidArgument("parse window failed");
    }
    timer->AddCheckPoint("pre_handle");
    Status status;
    for (auto queryRange : queryRanges) {
        status =
            (*object)->OrSet().Scan(nullptr, queryRange.begin, queryRange.end, riskComputeFunc);
        if (!status.ok()) {
            response->set_err_code(status.errorcode());
            response->set_err_msg(status.ToString());
            return status;
        }
    }
    timer->AddCheckPoint("scan");
    response->set_err_code(status.errorcode());
    return Status::OK();
}

Status FirstOrLastSet(ObjectHandle<model::StringModel>* object, const risk::FolSetRequest& request,
                      risk::FolSetResponse* response) {
    std::string pre_val = (*object)->GetValue();

    int64_t occur_time = request.occur_time();
    if (occur_time == 0) {
        time(&occur_time);
    }
    std::string cur_val = std::to_string(occur_time) + request.value();

    // 设置过期时间, 过期时间点的毫秒级时间戳, 过期时间延长 ttl 的 kRiskKeyTTLBase 倍
    object->SetTtl(bcache2::GetCurrentTimeInMs() + int64_t(request.ttl() * kRiskKeyTTLBase * 1000));

    if (pre_val.empty()) {
        (*object)->SetValue(nullptr, cur_val, bcache2::GetCurrentTimeInMs() + request.ttl() * 1000);
    } else {
        uint64_t cur_occur_time = occur_time;
        uint64_t pre_occur_time =
            std::strtoull(pre_val.substr(0, FirstAndSetTimestampSize).c_str(), nullptr, 10);
        auto fol_type = request.fol_type();
        bool needOverride = (fol_type == bcache2::risk::FIRST && cur_occur_time < pre_occur_time) ||
                            (fol_type == bcache2::risk::LAST && cur_occur_time > pre_occur_time);
        if (needOverride) {
            (*object)->SetValue(nullptr, cur_val,
                bcache2::GetCurrentTimeInMs() + request.ttl() * 1000);
        }
    }

    return Status::OK();
}

Status FirstOrLastGet(ObjectHandle<model::StringModel>* object,
                      const risk::FolQueryRequest& request, risk::FolQueryResponse* response) {
    response->set_result(((*object)->GetValue()).substr(FirstAndSetTimestampSize));
    return Status::OK();
}

// 实现指标，包括COUNT，MAX，MIN，CHANGE算子的查询
Status Hset(ExecuteEnv* env, const HsetRequest& request, HsetResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), request.htype());
    switch (request.htype()) {
    case bcache2::risk::COUNT:
        return CountHset(env, &timer, request, response,
        [](int64_t cur_val, int64_t pre_val) -> int64_t {
            return cur_val + pre_val;
        });
    case bcache2::risk::MIN:
        return CountHset(env, &timer, request, response,
        [](int64_t cur_val, int64_t pre_val) -> int64_t {
            return std::min(cur_val, pre_val);
        });
    case bcache2::risk::MAX:
        return CountHset(env, &timer, request, response,
        [](int64_t cur_val, int64_t pre_val) -> int64_t {
            return std::max(cur_val, pre_val);
        });
    case bcache2::risk::CHANGE:
        return ChangeHset(env, &timer, request, response);
    default:
        return Status::InvalidArgument("Invalid htype");
    }
}

REGISTER_FUNCTION(RISK, HSET, Hset, Write);

// 用于DC场景的写入
Status CPCSet(ExecuteEnv* env, const CPCSetRequest& request, CPCSetResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), 0);
    ObjectHandle<model::CPCModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    timer.AddCheckPoint("load_data");
    if (!status.ok()) {
        return status;
    }
    // 设置过期时间, 过期时间点的毫秒级时间戳, 过期时间延长 ttl 的 kRiskKeyTTLBase 倍
    object.SetTtl(bcache2::GetCurrentTimeInMs() + int64_t(request.ttl() * kRiskKeyTTLBase * 1000));

    timer.AddCheckPoint("set_ttl");
    std::vector<std::string> fields;

    // 根据输入的精度，然后获取到需要写入的精度列表，进而拼接字段
    auto precison = request.precision();
    auto storeLevels = risk_tool::RiskGlobalData::getSingleton().getPrecisionLevelInfo(precison);
    if (storeLevels.size() <= 0) {
        LOG(WARNING) << "[risk]DcSet failed, get storeLevel failed, precision = " << precison
                     << std::endl;
        return Status::InvalidArgument("precision setting failed");
    }
    // CPC 不需要 field 信息
    for (const auto& p : storeLevels) {
        // 构建当前的filed值
        time_t timestamp =
            risk_tool::fixTimeWithPrecision(request.occur_time(), request.key(), p->window);
        fields.emplace_back(std::to_string(p->window) + std::to_string(timestamp));
    }
    int values_size = request.values_size();
    if (values_size == 0) {
        return Status::InvalidArgument("value set is empty");
    }
    timer.AddCheckPoint("pre_handle");
    for (int i = 0; i < values_size; i++) {
        status = object->Update(nullptr, &timer, fields, request.values(i), request.ttl(),
                                request.dont_upgrade_cpc(), request.uuid());

        if (UNLIKELY(status.IsRiskAlreadyHandled())) {
            LOG(INFO) << "[risk]request is already handled, key="
                << request.key() << " uuid=" << request.uuid();
            status = Status::OK();
        }
        response->set_err_code(status.errorcode());
        if (!status.ok()) {
            response->set_err_msg(status.ToString());
            return status;
        }
    }
    return status;
}

REGISTER_FUNCTION(RISK, CPCSET, CPCSet, Write);

// 用于dc的读
Status CPCQuery(ExecuteEnv* env, const risk::CPCQueryRequest& request,
                risk::CPCQueryResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), 0);
    ObjectHandle<model::CPCModel> object;
    Status status = env->GetObject(request.key(), &object);
    timer.AddCheckPoint("load_data");
    if (!status.ok()) {
        if (status.IsNotFound()) {
            int windows_size = request.windows_size();
            for (int i = 0; i < windows_size; i++) {
                response->add_count_list(0);
                response->add_detail_lists();
            }
            return Status::OK();
        } else {
            response->set_err_code(status.errorcode());
            response->set_err_msg(status.ToString());
            return status;
        }
    }
    int windows_size = request.windows_size();
    for (int i = 0; i < windows_size; i++) {
        std::vector<risk_tool::RiskQueryRange> queryRanges;
        int ret =
            risk_tool::getWindows(request.windows(i), request.precision(), &queryRanges, true, 0);
        if (ret != 0) {
            return Status::InvalidArgument("parse window failed");
        }
        if (request.with_detail()) {  // 查询dc详情列表
            absl::flat_hash_set<std::string> dResult;
            status = object->ScanForList(queryRanges, &dResult);
            if (!status.ok()) {
                response->set_err_code(status.errorcode());
                response->set_err_msg(status.ToString());
                return status;
            }
            response->set_err_code(status.errorcode());

            risk::ListDetail list_detail;
            for (const auto& filed : dResult) {
                list_detail.add_detail(filed);
            }
            auto* detail = response->add_detail_lists();
            *detail = list_detail;
            response->add_count_list(list_detail.detail_size());
            timer.AddCheckPoint("dc_list");
        } else {  // 查询count
            double dResult = 0.0;
            status = object->Scan(queryRanges, &dResult);
            if (!status.ok()) {
                response->set_err_code(status.errorcode());
                response->set_err_msg(status.ToString());
                return status;
            }
            response->set_err_code(status.errorcode());
            int64_t result = static_cast<int64_t>(dResult + 0.5);
            response->add_count_list(result);
            timer.AddCheckPoint("dc");
        }
    }
    return status;
}

REGISTER_FUNCTION(RISK, CPCQUERY, CPCQuery, Read);

// 实现指标包括，COUNT，MAX，MIN,CHANGE 算子的查询
Status Hquery(ExecuteEnv* env, const HqueryRequest& request, HqueryResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), request.htype());
    ObjectHandle<model::RiskHashModel> object;
    Status status = env->GetObject(request.key(), &object);
    timer.AddCheckPoint("load_data");
    int windows_size = request.windows_size();
    if (!status.ok()) {
        if (status.IsNotFound()) {
            for (int i = 0; i < windows_size; i++) {
                response->add_result_list();
            }
            return Status::OK();
        } else {
            response->set_err_code(status.errorcode());
            response->set_err_msg(status.ToString());
            return status;
        }
    }
    switch (request.htype()) {
    case bcache2::risk::CHANGE:
    case bcache2::risk::COUNT: {
        for (int i = 0; i < windows_size; i++) {
            int64_t count = 0;
            bool has_field = false;
            auto riskCountFunc = [&count, &has_field](const std::string& field,
                                          const std::string& value) -> void {
                has_field = true;
                count += std::strtoll(value.c_str(), nullptr, 10);
            };
            status = CountQuery(env, &timer, &object, request.windows(i), request.precision(),
                                response, riskCountFunc);
            if (!status.ok()) {
                return status;
            }
            ResultDetail* resultDetail = response->add_result_list();
            resultDetail->set_has_result(has_field);
            if (has_field) {
                resultDetail->set_result(count);
            }
        }
        return Status::OK();
    }
    case bcache2::risk::MIN: {
        for (int i = 0; i < windows_size; i++) {
            bool has_field = false;
            int64_t result = INT64_MAX;
            auto riskMinFunc = [&result, &has_field](const std::string& field,
                                                     const std::string& value) -> void {
                has_field = true;
                int64_t cur_value = std::strtoll(value.c_str(), nullptr, 10);
                if (cur_value < result) {
                    result = cur_value;
                }
            };
            status = CountQuery(env, &timer, &object, request.windows(i), request.precision(),
                                response, riskMinFunc);
            if (!status.ok()) {
                return status;
            }
            ResultDetail* resultDetail = response->add_result_list();
            resultDetail->set_has_result(has_field);
            if (has_field) {
                resultDetail->set_result(result);
            }
        }
        return Status::OK();
    }
    case bcache2::risk::MAX: {
        for (int i = 0; i < windows_size; i++) {
            bool has_field = false;
            int64_t result = INT64_MIN;
            auto riskMaxFunc = [&result, &has_field](const std::string& filed,
                                                     const std::string& value) -> void {
                has_field = true;
                int64_t cur_value = std::strtoll(value.c_str(), nullptr, 10);
                if (cur_value > result) {
                    result = cur_value;
                }
            };
            status = CountQuery(env, &timer, &object, request.windows(i), request.precision(),
                                response, riskMaxFunc);
            if (!status.ok()) {
                return status;
            }
            ResultDetail* resultDetail = response->add_result_list();
            resultDetail->set_has_result(has_field);
            if (has_field) {
                resultDetail->set_result(result);
            }
        }
        return Status::OK();
    }
    default:
        return Status::InvalidArgument("Invalid htype");
    }
}
REGISTER_FUNCTION(RISK, HQUERY, Hquery, Read);

// 实现同步指标，COUNT，MAX，MIN,CHANGE等算子
Status HsetAndGet(ExecuteEnv* env, const HsetAndGetRequest& request, HsetAndGetResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), request.htype());
    Status status;
    // 构建HsetRequest参数
    HsetRequest set_request;
    set_request.set_key(request.key());
    set_request.set_ttl(request.ttl());
    set_request.set_htype(request.htype());
    set_request.set_value(request.value());
    set_request.set_precision(request.precision());

    HsetResponse set_response;
    status = Hset(env, set_request, &set_response);
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
    // 执行查询操作，构建HqueryRequest
    HqueryRequest query_request;
    HqueryResponse query_response;
    query_request.set_key(request.key());
    query_request.set_precision(request.precision());
    query_request.set_htype(request.htype());
    query_request.mutable_windows()->CopyFrom(request.windows());
    status = Hquery(env, query_request, &query_response);
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
    response->mutable_result_list()->CopyFrom(query_response.result_list());

    return status;
}

REGISTER_FUNCTION(RISK, HSETANDGET, HsetAndGet, Write);

// 实现同步指标，DC，LIST算子
Status CPCSetAndGet(ExecuteEnv* env, const CPCSetAndGetRequest& request,
                    CPCSetAndGetResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), 0);
    Status status;
    CPCSetRequest set_request;
    set_request.set_key(request.key());
    set_request.set_ttl(request.ttl());
    set_request.mutable_values()->CopyFrom(request.values());
    set_request.set_precision(request.precision());

    CPCSetResponse set_response;
    status = CPCSet(env, set_request, &set_response);
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
    CPCQueryRequest query_request;
    CPCQueryResponse query_response;
    query_request.set_key(request.key());
    query_request.set_precision(request.precision());
    query_request.set_with_detail(request.with_detail());
    query_request.mutable_windows()->CopyFrom(request.windows());
    status = CPCQuery(env, query_request, &query_response);
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
    response->mutable_count_list()->CopyFrom(query_response.count_list());
    response->mutable_detail_lists()->CopyFrom(query_response.detail_lists());

    return status;
}

REGISTER_FUNCTION(RISK, CPCSETANDGET, CPCSetAndGet, Write);

// 实现first/last的指标查询算子
Status FolQuery(ExecuteEnv* env, const FolQueryRequest& request, FolQueryResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), -1);
    ObjectHandle<model::StringModel> object;
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            return Status::OK();
        }
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
    return FirstOrLastGet(&object, request, response);
}

REGISTER_FUNCTION(RISK, FOLQUERY, FolQuery, Read);

// 实现first/last的指标写入算子
Status FolSet(ExecuteEnv* env, const FolSetRequest& request, FolSetResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), -1);
    ObjectHandle<model::StringModel> object;
    Status status = env->GetOrNewObject(request.key(), &object);
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
    return FirstOrLastSet(&object, request, response);
}

REGISTER_FUNCTION(RISK, FOLSET, FolSet, Write);

// 实现first/last的同步指标算子
Status FolSetAndGet(ExecuteEnv* env, const FolSetAndGetRequest& request,
                    FolSetAndGetResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), -1);
    ObjectHandle<model::StringModel> object;
    auto key = request.key();
    Status status = env->GetOrNewObject(key, &object);
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
    FolSetResponse set_response;
    FolSetRequest set_request;
    set_request.set_key(request.key());
    set_request.set_value(request.value());
    set_request.set_occur_time(request.occur_time());
    set_request.set_fol_type(request.fol_type());
    set_request.set_ttl(request.ttl());

    status = FirstOrLastSet(&object, set_request, &set_response);
    if (status.ok()) {
        // 执行查询操作
        FolQueryRequest query_request;
        query_request.set_key(key);
        FolQueryResponse query_response;
        status = FirstOrLastGet(&object, query_request, &query_response);
        if (!status.ok()) {
            response->set_err_code(status.errorcode());
            response->set_err_msg(status.ToString());
        }
        response->set_result(query_response.result());
        return status;
    } else {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
}

REGISTER_FUNCTION(RISK, FOLSETANDGET, FolSetAndGet, Write);

Status CPCManager(ExecuteEnv* env, const ManagerRequest& request, ManagerResponse* response) {
    RiskTimerLogger timer(__func__, request.key(), request.op_type());
    ObjectHandle<model::CPCModel> object;
    auto key = request.key();
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
    std::vector<std::string> field_list;
    std::vector<std::string> value_list;
    size_t size = request.field_list_size();
    for (size_t i = 0; i < size; ++i) {
        auto kvpair = request.field_list(i);
        field_list.emplace_back(kvpair.field());
        value_list.emplace_back(kvpair.value());
    }
    status = Status::OK();
    bcache2::risk::KvPair* resKVPair = nullptr;
    absl::flat_hash_map<std::string, std::string> resMap;
    switch (request.op_type()) {
    case FULLGC:
        if (size == 0) {
            status = Status::InvalidArgument("field list is empty");
            break;
        }
        status = object->DoGC(nullptr, field_list, true, std::time(0));
        break;
    case SUBGC:
        if (size == 0) {
            status = Status::InvalidArgument("field list is empty");
            break;
        }
        status = object->DoGC(nullptr, field_list, false, std::time(0));
        break;
    case QUERY:
        if (size != 0) {
            status = object->Query(field_list, &resMap);
            if (status.ok()) {
                for (auto iter : resMap) {
                    resKVPair = response->add_result();
                    resKVPair->set_field(iter.first);
                    resKVPair->set_value(iter.second);
                }
            }
        } else {
            status = Status::InvalidArgument("field list is empty");
            break;
        }
        break;
    case DEL:
        if (size != 0) {
            status = object->BatchDel(nullptr, field_list);
            break;
        } else {
            status = Status::InvalidArgument("field list is empty");
            break;
        }
        break;
    case FIELD_LIST:
        resKVPair = response->add_result();
        resKVPair->set_field("key_list");
        resKVPair->set_value(object->FieldList(request.range_start(), request.range_end()));
        break;
    case FIELD_COUNT:
        resKVPair = response->add_result();
        resKVPair->set_field("size");
        resKVPair->set_value(std::to_string(object->Size()));
        break;
    case ALL_DATA_VALUE:
        status = object->ScanForManager("", "9999999999999", &resMap);
        if (status.ok()) {
            for (auto iter : resMap) {
                resKVPair = response->add_result();
                resKVPair->set_field(iter.first);
                resKVPair->set_value(iter.second);
            }
        }
        break;
    case FORCE_CHANGE_CPC:
        status = object->ForceChangeCPC(nullptr);
        break;
    case GET_DC_TYPE:
        resKVPair = response->add_result();
        resKVPair->set_field("dc_type");
        resKVPair->set_value(object->GetDcType());
        break;
    case MEM_SIZE:
        resKVPair = response->add_result();
        resKVPair->set_field("mem_size");
        resKVPair->set_value(std::to_string(object->MemSize()));
        break;
    case REPEAT_CHECK_DATA:
        resKVPair = response->add_result();
        resKVPair->set_field("repeat_data");
        resKVPair->set_value(object->RepeatData());
        break;
    default:
        status = Status::InvalidArgument("op_type is invalid");
        break;
    }
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
    }
    return status;
}

// 管理方法(不包含First Last)
Status Manager(ExecuteEnv* env, const ManagerRequest& request, ManagerResponse* response) {
    if (request.is_cpc()) {
        return CPCManager(env, request, response);
    }
    RiskTimerLogger timer(__func__, request.key(), request.op_type());
    ObjectHandle<model::RiskHashModel> object;
    auto key = request.key();
    Status status = env->GetObject(request.key(), &object);
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
        return status;
    }
    std::vector<std::string> field_list;
    std::vector<std::string> value_list;
    size_t size = request.field_list_size();
    for (size_t i = 0; i < size; i++) {
        auto kvpair = request.field_list(i);
        field_list.emplace_back(kvpair.field());
        value_list.emplace_back(kvpair.value());
    }
    status = Status::OK();
    bcache2::risk::KvPair* resKVPair = nullptr;
    switch (request.op_type()) {
    case FULLGC:
        if (size == 0) {
            status = Status::InvalidArgument("field list is empty");
            break;
        }
        object->OrSet().DoFullGC(nullptr, field_list, std::time(0));
        break;
    case SUBGC:
        if (size == 0) {
            status = Status::InvalidArgument("field list is empty");
            break;
        }
        object->OrSet().DoMinGC(nullptr, field_list, std::time(0));
        break;
    case UPDATE:
        // 更新操作直接覆盖
        if (size != 0) {
            object->OrSet().BatchOverWrite(nullptr, &timer, field_list, value_list, 0);
        } else {
            status = Status::InvalidArgument("field list is empty");
            break;
        }
        break;
    case QUERY:
        if (size != 0) {
            // #include <absl/container/flat_hash_map.h>
            absl::flat_hash_map<std::string, std::string> res;
            object->OrSet().Query(field_list, &res);
            for (auto iter : res) {
                resKVPair = response->add_result();
                resKVPair->set_field(iter.first);
                resKVPair->set_value(iter.second);
            }
        } else {
            status = Status::InvalidArgument("field list is empty");
            break;
        }
        break;
    case DEL:
        if (size != 0) {
            for (size_t i = 0; i < size; ++i) {
                object->OrSet().Del(nullptr, field_list[i]);
            }
            break;
        } else {
            status = Status::InvalidArgument("field list is empty");
            break;
        }
        break;
    case FIELD_LIST:
        resKVPair = response->add_result();
        resKVPair->set_field("key_list");
        resKVPair->set_value(object->OrSet().FieldList(request.range_start(), request.range_end()));
        break;
    case FIELD_COUNT:
        resKVPair = response->add_result();
        resKVPair->set_field("size");
        resKVPair->set_value(std::to_string(object->OrSet().Size()));
        break;
    case ALL_DATA_VALUE:
        status = object->OrSet().Scan(
            nullptr, "", "9999999999999",
            [&response](const std::string& field, const std::string& value) -> void {
                auto kvPair = response->add_result();
                kvPair->set_field(field);
                kvPair->set_value(value);
            });
        break;
    case REPEAT_CHECK_DATA:
        resKVPair = response->add_result();
        resKVPair->set_field("repeat_data");
        resKVPair->set_value(object->OrSet().RepeatData());
        break;
    default:
        status = Status::InvalidArgument("op_type is invalid");
        break;
    }
    if (!status.ok()) {
        response->set_err_code(status.errorcode());
        response->set_err_msg(status.ToString());
    }
    return status;
}

REGISTER_FUNCTION(RISK, MANAGER, Manager, Write);

}  // namespace risk
}  // namespace bcache2
