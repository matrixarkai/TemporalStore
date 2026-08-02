// Copyright (c) 2022-present, ByteDance Inc. All rights reserved.
#include "common/operator_tool.h"

#include <string>
#include <utility>

#include "brpc/channel.h"
#include "butil/time.h"
#include "json2pb/rapidjson.h"
#include "spdlog/fmt/fmt.h"

#include "common/logging.h"

namespace bcache2 {
namespace operator_tool {

#define JSON_CHECK(condition)                                                \
    if (!(condition)) {                                                      \
        butil::rapidjson::StringBuffer sb;                                   \
        butil::rapidjson::Writer<butil::rapidjson::StringBuffer> writer(sb); \
        body.Accept(writer);                                                 \
        LOG_WARNING("invalid json body").put("v", sb.GetString());           \
        return Status::Internal("invalid json");                             \
    }

CMDBClient::CMDBClient(std::string cmdb_host, std::string jwt_uri, std::string key)
    : cmdb_host_(cmdb_host), jwt_uri_(std::move(jwt_uri)), key_(std::move(key)) {}

Status CMDBClient::QueryHostLocation(const std::string& host, Location* vloc) {
    MaybeAcquireJwt();
    if (jwt_.empty()) {
        return Status::Internal("invalid jwt");
    }

    const std::string uri = fmt::format("{}/cmdb/api/v1/hosts/{}/brief", cmdb_host_, host);

    brpc::ChannelOptions opts;
    opts.protocol = brpc::PROTOCOL_HTTP;
    brpc::Channel channel;
    opts.timeout_ms = 1000;
    opts.connect_timeout_ms = 1000;
    channel.Init(uri.c_str(), &opts);

    brpc::Controller cntl;
    cntl.http_request().uri() = uri;
    cntl.http_request().set_method(brpc::HTTP_METHOD_GET);
    cntl.http_request().SetHeader("x-jwt-token", jwt_);
    channel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
    if (cntl.Failed()) {
        LOG_WARNING("query host rpc failed").put("uri", uri).put("err", cntl.ErrorText());
        return Status::Internal("rpc failed");
    }

    butil::rapidjson::Document body;
    if (body.Parse<0>(cntl.response_attachment().to_string().c_str()).HasParseError()) {
        return Status::Internal("json parse failed");
    }
    JSON_CHECK(body.IsObject() && body.HasMember("error_code"));
    auto& code = body["error_code"];
    JSON_CHECK(code.IsInt() && code.GetInt() == 0);
    JSON_CHECK(body.HasMember("data"));
    auto& data = body["data"];
    vloc->set_vregion(data["vregion"].GetString());
    vloc->set_vdc(data["vdc"].GetString());
    vloc->set_vau(data["vau"].GetString());
    return Status::OK();
}

void CMDBClient::MaybeAcquireJwt() {
    if (!jwt_.empty() && butil::gettimeofday_s() < last_jwt_timestamp_ + 30 * 60) {
        return;
    }

    brpc::ChannelOptions opts;
    opts.protocol = brpc::PROTOCOL_HTTP;
    brpc::Channel channel;
    opts.timeout_ms = 1000;
    opts.connect_timeout_ms = 1000;
    channel.Init(jwt_uri_.c_str(), &opts);

    brpc::Controller cntl;
    cntl.http_request().uri() = jwt_uri_;
    cntl.http_request().set_method(brpc::HTTP_METHOD_GET);
    cntl.http_request().SetHeader("Authorization", "Bearer " + key_);
    channel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
    if (cntl.Failed()) {
        LOG_WARNING("jwt rpc failed")
            .put("uri", jwt_uri_)
            .put("key_len", key_.size())
            .put("err", cntl.ErrorText());
        return;
    }
    jwt_ = *cntl.http_response().GetHeader("X-Jwt-Token");
    if (!jwt_.empty()) {
        last_jwt_timestamp_ = butil::gettimeofday_s();
    }
}

#undef JSON_CHECK

}  // namespace operator_tool
}  // namespace bcache2

