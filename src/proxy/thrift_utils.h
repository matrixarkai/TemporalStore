// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>
#include <vector>

#include "client/client_impl.h"
#include "common/cmd_manager.h"
#include "common/status.h"
#include "extension/feature/interface.pb.h"
#include "extension/hash/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/risk/interface.pb.h"
#include "extension/string/interface.pb.h"
#include "thrift/TApplicationException.h"
#include "thrift/TProcessor.h"
#include "thrift/Thrift.h"
#include "thrift/protocol/TBinaryProtocol.h"
#include "thrift/server_types.h"
#include "thrift/transport/TBufferTransports.h"

namespace bcache2 {
namespace proxy {

inline thrift::Status ToThriftStatus(const Status& status) {
    thrift::Status thrift_status;
    thrift_status.__set_code(status.errorcode());
    thrift_status.__set_message(status.ToString());
    return thrift_status;
}

// transform thrift to client::Request

inline Status TransformRequest(const thrift::FeatureAddRequest& request,
                               client::TableCore::Request* client_request) {
    feature2::AddRequest pb_request;
    pb_request.set_key(request.key);
    pb_request.set_format(request.format);
    for (const auto& point : request.point_list) {
        auto pb_point = pb_request.add_point_list();
        pb_point->set_ts(point.ts);
        pb_point->set_value(point.value);
    }
    pb_request.set_policy(feature2::WritePolicy(request.policy));
    client_request->cmd_id = MakeCmdId(FEATURE, feature2::ADD);
    client_request->key = request.key;
    client_request->input.set_module_id(FEATURE);
    client_request->input.set_function_id(feature2::ADD);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::FeatureQueryRequest& request,
                               client::TableCore::Request* client_request) {
    feature2::QueryRequest pb_request;
    pb_request.set_key(request.key);
    pb_request.set_start_ts(request.start_ts);
    pb_request.set_end_ts(request.end_ts);
    pb_request.set_count(request.count);
    pb_request.set_format(request.format);
    *pb_request.mutable_filters() = {request.filters.begin(), request.filters.end()};
    pb_request.set_fields(request.fields);
    client_request->cmd_id = MakeCmdId(FEATURE, feature2::QUERY);
    client_request->key = request.key;
    client_request->input.set_module_id(FEATURE);
    client_request->input.set_function_id(feature2::QUERY);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::SetRequest& request,
                               client::TableCore::Request* client_request) {
    str2::SetRequest pb_request;
    pb_request.set_key(request.key);
    pb_request.set_value(request.value);
    client_request->cmd_id = MakeCmdId(STRING, str2::SET);
    client_request->key = request.key;
    client_request->input.set_module_id(STRING);
    client_request->input.set_function_id(str2::SET);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::GetRequest& request,
                               client::TableCore::Request* client_request) {
    str2::GetRequest pb_request;
    pb_request.set_key(request.key);
    client_request->cmd_id = MakeCmdId(STRING, str2::GET);
    client_request->key = request.key;
    client_request->input.set_module_id(STRING);
    client_request->input.set_function_id(str2::GET);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::RiskHsetRequest& request,
                               client::TableCore::Request* client_request) {
    risk::HsetRequest pb_request;
    pb_request.set_key(request.key);
    pb_request.set_htype(risk::HType(request.htype));
    pb_request.set_ttl(request.ttl);
    pb_request.set_value(request.value);
    pb_request.set_occur_time(request.occur_time);
    pb_request.set_precision(risk::RiskPrecision(request.precision));
    client_request->cmd_id = MakeCmdId(RISK, risk::HSET);
    client_request->key = request.key;
    client_request->input.set_module_id(RISK);
    client_request->input.set_function_id(risk::HSET);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::RiskHqueryRequest& request,
                               client::TableCore::Request* client_request) {
    risk::HqueryRequest pb_request;
    pb_request.set_key(request.key);
    for (size_t i = 0; i < request.windows.size(); i++) {
        auto window = pb_request.add_windows();
        window->set_start(request.windows[i].start_offset);
        window->set_end(request.windows[i].end_offset);
        window->set_unit(risk::WindowUnit(request.windows[i].unit));
    }
    pb_request.set_precision(risk::RiskPrecision(request.precision));
    pb_request.set_htype(risk::HType(request.htype));
    client_request->cmd_id = MakeCmdId(RISK, risk::HQUERY);
    client_request->key = request.key;
    client_request->input.set_module_id(RISK);
    client_request->input.set_function_id(risk::HQUERY);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::RiskCPCSetRequest& request,
                               client::TableCore::Request* client_request) {
    risk::CPCSetRequest pb_request;
    pb_request.set_key(request.key);
    pb_request.set_ttl(request.ttl);
    pb_request.set_occur_time(request.occur_time);
    pb_request.set_precision(risk::RiskPrecision(request.precision));
    pb_request.set_dont_upgrade_cpc(request.dont_upgrade_cpc);
    for (size_t i = 0; i < request.values.size(); i++) {
        pb_request.add_values(request.values[i]);
    }
    client_request->cmd_id = MakeCmdId(RISK, risk::CPCSET);
    client_request->key = request.key;
    client_request->input.set_module_id(RISK);
    client_request->input.set_function_id(risk::CPCSET);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::RiskCPCQueryRequest& request,
                               client::TableCore::Request* client_request) {
    risk::CPCQueryRequest pb_request;
    pb_request.set_key(request.key);
    for (size_t i = 0; i < request.windows.size(); i++) {
        auto window = pb_request.add_windows();
        window->set_start(request.windows[i].start_offset);
        window->set_end(request.windows[i].end_offset);
        window->set_unit(risk::WindowUnit(request.windows[i].unit));
    }
    pb_request.set_precision(risk::RiskPrecision(request.precision));
    pb_request.set_with_detail(request.with_detail);
    client_request->cmd_id = MakeCmdId(RISK, risk::CPCQUERY);
    client_request->key = request.key;
    client_request->input.set_module_id(RISK);
    client_request->input.set_function_id(risk::CPCQUERY);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::RiskFolSetRequest& request,
                               client::TableCore::Request* client_request) {
    risk::FolSetRequest pb_request;
    pb_request.set_key(request.key);
    pb_request.set_value(request.value);
    pb_request.set_ttl(request.ttl);
    pb_request.set_occur_time(request.occur_time);
    pb_request.set_fol_type(bcache2::risk::FolType(request.fol_type));
    client_request->cmd_id = MakeCmdId(RISK, risk::FOLSET);
    client_request->key = request.key;
    client_request->input.set_module_id(RISK);
    client_request->input.set_function_id(risk::FOLSET);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::RiskFolQueryRequest& request,
                               client::TableCore::Request* client_request) {
    risk::FolQueryRequest pb_request;
    pb_request.set_key(request.key);
    client_request->cmd_id = MakeCmdId(RISK, risk::FOLQUERY);
    client_request->key = request.key;
    client_request->input.set_module_id(RISK);
    client_request->input.set_function_id(risk::FOLQUERY);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::RiskManagerRequest& request,
                               client::TableCore::Request* client_request) {
    risk::ManagerRequest pb_request;
    for (size_t i = 0; i < request.field_list.size(); i++) {
        auto kv_pair = pb_request.add_field_list();
        kv_pair->set_field(request.field_list[i].key);
        kv_pair->set_value(request.field_list[i].value);
    }
    pb_request.set_key(request.key);
    pb_request.set_is_cpc(request.is_cpc);
    pb_request.set_op_type(bcache2::risk::ManagerType(request.op_type));
    pb_request.set_range_start(request.start_offset);
    pb_request.set_range_end(request.end_offset);
    client_request->cmd_id = MakeCmdId(RISK, risk::MANAGER);
    client_request->key = request.key;
    client_request->input.set_module_id(RISK);
    client_request->input.set_function_id(risk::MANAGER);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::HMGetRequest& request,
                               client::TableCore::Request* client_request) {
    hash2::MGetRequest pb_request;
    pb_request.set_key(request.key);
    *pb_request.mutable_fields() = {request.fields.begin(), request.fields.end()};
    client_request->cmd_id = MakeCmdId(HASH, hash2::MGET);
    client_request->key = request.key;
    client_request->input.set_module_id(HASH);
    client_request->input.set_function_id(hash2::MGET);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::HMSetRequest& request,
                               client::TableCore::Request* client_request) {
    hash2::MSetRequest pb_request;
    pb_request.set_key(request.key);
    *pb_request.mutable_fields() = {request.fields.begin(), request.fields.end()};
    *pb_request.mutable_values() = {request.values.begin(), request.values.end()};
    client_request->cmd_id = MakeCmdId(HASH, hash2::MSET);
    client_request->key = request.key;
    client_request->input.set_module_id(HASH);
    client_request->input.set_function_id(hash2::MSET);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::HGetAllRequest& request,
                               client::TableCore::Request* client_request) {
    hash2::GetAllRequest pb_request;
    pb_request.set_key(request.key);
    client_request->cmd_id = MakeCmdId(HASH, hash2::GETALL);
    client_request->key = request.key;
    client_request->input.set_module_id(HASH);
    client_request->input.set_function_id(hash2::GETALL);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

inline Status TransformRequest(const thrift::HLenRequest& request,
                               client::TableCore::Request* client_request) {
    hash2::LenRequest pb_request;
    pb_request.set_key(request.key);
    client_request->cmd_id = MakeCmdId(HASH, hash2::LEN);
    client_request->key = request.key;
    client_request->input.set_module_id(HASH);
    client_request->input.set_function_id(hash2::LEN);
    client_request->input.set_request_bytes(pb_request.SerializeAsString());
    return Status::OK();
}

// transform client::Response to thrift

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::FeatureAddResponse* response) {
    // do nothing
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::FeatureQueryResponse* response) {
    feature2::QueryResponse pb_resp;
    if (!pb_resp.ParseFromString(client_response.output->response_bytes())) {
        return Status::InvalidArgument("invalid response format");
    }
    std::vector<thrift::Point> point_list;
    for (const auto& pb_point : pb_resp.point_list()) {
        thrift::Point point;
        point.__set_ts(pb_point.ts());
        point.__set_value(pb_point.value());
        point_list.emplace_back(point);
    }
    response->__set_point_list(point_list);
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::SetResponse* response) {
    // do nothing
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::GetResponse* response) {
    str2::GetResponse pb_resp;
    if (!pb_resp.ParseFromString(client_response.output->response_bytes())) {
        return Status::InvalidArgument("invalid response format");
    }
    response->__set_value(pb_resp.value());
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::RiskCommonResponse* response) {
    // do nothing
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::RiskHqueryResponse* response) {
    risk::HqueryResponse pb_resp;
    if (!pb_resp.ParseFromString(client_response.output->response_bytes())) {
        return Status::InvalidArgument("invalid response format");
    }
    std::vector<thrift::RiskResultDetail> result_list;
    for (int i = 0; i < pb_resp.result_list_size(); i++) {
        thrift::RiskResultDetail resultDetail;
        resultDetail.has_result = pb_resp.result_list(i).has_result();
        resultDetail.result = pb_resp.result_list(i).result();
        result_list.emplace_back(resultDetail);
    }
    response->__set_result_list(result_list);
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::RiskCPCQueryResponse* response) {
    risk::CPCQueryResponse pb_resp;
    if (!pb_resp.ParseFromString(client_response.output->response_bytes())) {
        return Status::InvalidArgument("invalid response format");
    }
    std::vector<std::int64_t> count_list;
    for (int i = 0; i < pb_resp.count_list_size(); i++) {
        count_list.emplace_back(pb_resp.count_list(i));
    }
    response->__set_count_list(count_list);

    std::vector<thrift::RiskListDetail> result_list;
    for (int i = 0; i < pb_resp.detail_lists_size(); i++) {
        thrift::RiskListDetail resultDetail;
        for (int j = 0; j < pb_resp.detail_lists(i).detail_size(); j++) {
            resultDetail.detail.emplace_back(pb_resp.detail_lists(i).detail(j));
        }
        result_list.emplace_back(resultDetail);
    }
    response->__set_detail_lists(result_list);
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::RiskFolQueryResponse* response) {
    risk::FolQueryResponse pb_resp;
    if (!pb_resp.ParseFromString(client_response.output->response_bytes())) {
        return Status::InvalidArgument("invalid response format");
    }
    response->__set_result(pb_resp.result());
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::RiskManagerResponse* response) {
    risk::ManagerResponse pb_resp;
    if (!pb_resp.ParseFromString(client_response.output->response_bytes())) {
        return Status::InvalidArgument("invalid response format");
    }
    std::vector<thrift::RiskKvPair> result_list;
    for (int i = 0; i < pb_resp.result_size(); i++) {
        thrift::RiskKvPair kvPair;
        kvPair.key = pb_resp.result(i).field();
        kvPair.value = pb_resp.result(i).value();
        result_list.emplace_back(kvPair);
    }
    response->__set_result(result_list);
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::HMGetResponse* response) {
    hash2::MGetResponse pb_resp;
    if (!pb_resp.ParseFromString(client_response.output->response_bytes())) {
        return Status::InvalidArgument("invalid response format");
    }
    std::vector<bool> exists(pb_resp.exists().begin(), pb_resp.exists().end());
    std::vector<std::string> values(pb_resp.values().begin(), pb_resp.values().end());
    response->__set_values(values);
    response->__set_exists(exists);
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::HMSetResponse* response) {
    // do nothing
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::HGetAllResponse* response) {
    hash2::GetAllResponse pb_resp;
    if (!pb_resp.ParseFromString(client_response.output->response_bytes())) {
        return Status::InvalidArgument("invalid response format");
    }
    std::vector<std::string> fields(pb_resp.fields().begin(), pb_resp.fields().end());
    std::vector<std::string> values(pb_resp.values().begin(), pb_resp.values().end());
    response->__set_fields(fields);
    response->__set_values(values);
    return Status::OK();
}

inline Status TransformResponse(const client::TableCore::Response& client_response,
                                thrift::HLenResponse* response) {
    hash2::LenResponse pb_resp;
    if (!pb_resp.ParseFromString(client_response.output->response_bytes())) {
        return Status::InvalidArgument("invalid response format");
    }
    response->__set_len(pb_resp.len());
    return Status::OK();
}

}  // namespace proxy
}  // namespace bcache2
