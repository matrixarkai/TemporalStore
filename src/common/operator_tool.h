// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#pragma once

#include <string>

#include "common/status.h"
#include "protocol/host_spec.pb.h"

namespace bcache2 {
namespace operator_tool {

class CMDBClient {
 public:
    CMDBClient(std::string cmdb_host, std::string jwt_uri, std::string key);
    ~CMDBClient() = default;

    Status QueryHostLocation(const std::string& host, Location* vloc);

 private:
    void MaybeAcquireJwt();

 private:
    const std::string cmdb_host_;
    const std::string jwt_uri_;
    const std::string key_;

    std::string jwt_;
    int64_t last_jwt_timestamp_{0};  // wall clock for human readable
};

}  // namespace operator_tool
}  // namespace bcache2

