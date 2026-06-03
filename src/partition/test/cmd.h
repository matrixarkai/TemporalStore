// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.

#pragma once

#include <string>
#include <utility>

#include "common/status.h"
#include "extension/feature/interface.pb.h"
#include "extension/hash/interface.pb.h"
#include "extension/ips/interface.pb.h"
#include "extension/modules.pb.h"
#include "extension/string/interface.pb.h"
#include "partition/partition.h"

namespace bcache2 {
namespace partition {

inline Status PartitionHSet(Partition* partition, std::string key, std::string field,
                            std::string value) {
    Controller ctrl;
    hash2::SetRequest set_request;
    hash2::SetResponse set_response;
    set_request.set_key(std::move(key));
    set_request.set_field(std::move(field));
    set_request.set_value(std::move(value));
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, HASH, hash2::SET, &set_request, &set_response, &ret);
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionHDel(Partition* partition, std::string key, std::string field) {
    Controller ctrl;
    CmdRequest request;
    hash::DelRequest* del_request = request.mutable_hash_request()->mutable_del_request();
    del_request->set_key(std::move(key));
    del_request->set_field(std::move(field));

    CmdResponse response;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, &request, &response);
    if (ctrl.status().ok()) {
        ctrl.set_status(Status::FromRpcStatus(response.response_status()));
    }
    return ctrl.status();
}

inline Status PartitionHGet(Partition* partition, std::string key, std::string field,
                            std::string* value) {
    Controller ctrl;
    hash2::GetRequest get_request;
    hash2::GetResponse get_response;
    get_request.set_key(std::move(key));
    get_request.set_field(std::move(field));
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, HASH, hash2::GET, &get_request, &get_response, &ret);
    *value = get_response.value();
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionHlen(Partition* partition, std::string key, uint64_t* len) {
    Controller ctrl;
    hash2::LenRequest request;
    hash2::LenResponse response;
    request.set_key(std::move(key));
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, HASH, hash2::LEN, &request, &response, &ret);
    *len = response.len();
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionHGetWithExist(Partition* partition, std::string key, std::string field,
                                     std::string* value, bool* exist) {
    Controller ctrl;
    hash2::GetRequest get_request;
    hash2::GetResponse get_response;
    get_request.set_key(std::move(key));
    get_request.set_field(std::move(field));
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, HASH, hash2::GET, &get_request, &get_response, &ret);
    *value = get_response.value();
    *exist = get_response.exist();
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionSet(Partition* partition, std::string key, std::string value) {
    Controller ctrl;
    str2::SetRequest set_request;
    str2::SetResponse set_response;
    set_request.set_key(std::move(key));
    set_request.set_value(std::move(value));
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, STRING, str2::SET, &set_request, &set_response, &ret);
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionGet(Partition* partition, std::string key, std::string* value) {
    Controller ctrl;
    str2::GetRequest get_request;
    str2::GetResponse get_response;
    get_request.set_key(std::move(key));
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, STRING, str2::GET, &get_request, &get_response, &ret);
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    *value = get_response.value();
    return ctrl.status();
}

inline Status PartitionDel(Partition* partition, std::string key) {
    Controller ctrl;
    CmdRequest request;
    request.mutable_common_request()->mutable_del_object_request()->set_key(std::move(key));
    CmdResponse response;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, &request, &response);
    if (ctrl.status().ok()) {
        ctrl.set_status(Status::FromRpcStatus(response.response_status()));
    }
    return ctrl.status();
}

inline Status PartitionSetEx(Partition* partition, std::string key, std::string value,
                             uint64_t ttl_ms) {
    Controller ctrl;
    str2::SetexRequest request;
    str2::SetexResponse response;
    request.set_key(std::move(key));
    request.set_value(std::move(value));
    request.set_ttl_ms(ttl_ms);
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, STRING, str2::SETEX, &request, &response, &ret);
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionExpire(Partition* partition, std::string key, uint64_t ttl_ms) {
    Controller ctrl;
    CmdRequest request;
    auto common_request = request.mutable_common_request();
    auto expire_request = common_request->mutable_expire_request();
    expire_request->set_key(std::move(key));
    expire_request->set_ttl_ms(ttl_ms);
    CmdResponse response;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, &request, &response);
    if (ctrl.status().ok()) {
        ctrl.set_status(Status::FromRpcStatus(response.response_status()));
    }
    return ctrl.status();
}

inline Status PartitionTtl(Partition* partition, std::string key, uint64_t* ttl_ms) {
    Controller ctrl;
    CmdRequest request;
    auto common_request = request.mutable_common_request();
    auto ttl_request = common_request->mutable_ttl_request();
    ttl_request->set_key(std::move(key));
    CmdResponse response;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, &request, &response);
    if (ctrl.status().ok()) {
        ctrl.set_status(Status::FromRpcStatus(response.response_status()));
    }
    *ttl_ms = response.common_response().ttl_response().ttl_ms();
    return ctrl.status();
}

inline Status PartitionSetConfig(Partition* partition, Config config) {
    config.set_version(partition->GetConfig().version() + 1);
    return partition->SetConfig(config);
}

inline Status PartitionIPSAdd(Partition* partition, ips::AddRequest add_request,
                              ips::AddResponse add_response) {
    Controller ctrl;
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, IPS, ips::ADD, &add_request, &add_response, &ret);
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionIPSBatchQuery(Partition* partition,
                                     ips::BatchQueryRequest ips_batch_query_request,
                                     ips::BatchQueryResponse ips_batch_query_response) {
    Controller ctrl;
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, IPS, ips::BATCH_QUERY, &ips_batch_query_request,
              &ips_batch_query_response, &ret);
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionFeatureAdd(Partition* partition, feature2::AddRequest feature_add_request,
                                  feature2::AddResponse feature_add_response) {
    Controller ctrl;
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, FEATURE, feature2::ADD, &feature_add_request,
              &feature_add_response, &ret);
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionQueryRequest(Partition* partition,
                                    feature2::QueryRequest feature_query_request,
                                    feature2::QueryResponse feature_query_response) {
    Controller ctrl;
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, FEATURE, feature2::QUERY, &feature_query_request,
              &feature_query_response, &ret);
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

inline Status PartitionDelRequest(Partition* partition, feature2::QueryRequest feature_del_request,
                                  feature2::QueryResponse feature_del_response) {
    Controller ctrl;
    Status ret;
    SYNC_CALL(partition->ExecuteCmd, &ctrl, FEATURE, feature2::DEL, &feature_del_request,
              &feature_del_response, &ret);
    if (ctrl.status().ok()) {
        ctrl.set_status(ret);
    }
    return ctrl.status();
}

}  // namespace partition
}  // namespace bcache2
